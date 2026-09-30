#include "valencedb/optimizer/optimizer.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/common/error.hpp"
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <sstream>

namespace valencedb {
namespace {
using Plans = std::vector<PhysicalPlan>;
ModeledCost Add(ModeledCost a, ModeledCost b) { return {a.pages + b.pages, a.cpu + b.cpu}; }
ModeledCost Times(ModeledCost a, long double n) { return {a.pages * n, a.cpu * n}; }
std::string Number(long double n) {
    std::ostringstream out;
    out << std::setprecision(5) << n;
    return out.str();
}
long double PackedPages(const Schema& s, long double rows) {
    const size_t per_page = (kPageSize - 32) / (s.max_encoded_bytes() + 4);
    return std::ceil(rows / per_page);
}
void Rejected(std::vector<std::string>& reasons, const std::string& reason) {
    if (std::find(reasons.begin(), reasons.end(), reason) == reasons.end()) reasons.push_back(reason);
}
const char* Name(PlanKind k) {
    switch (k) {
        case PlanKind::SeqScan: return "SeqScan";
        case PlanKind::IndexScan: return "IndexScan";
        case PlanKind::Filter: return "Filter";
        case PlanKind::Projection: return "Projection";
        case PlanKind::NLJ: return "NLJ";
        case PlanKind::BNLJ: return "BNLJ";
        case PlanKind::GHJ: return "GHJ";
        case PlanKind::GlobalAggregate: return "GlobalAggregate";
        case PlanKind::HashAggregate: return "HashAggregate";
        case PlanKind::SortAggregate: return "SortAggregate";
        case PlanKind::ExternalSort: return "ExternalSort";
    }
    throw DbError("invalid physical operator");
}
std::string PredicateText(const Int64Predicate& p) {
    const char* op = "=";
    switch (p.op) {
        case CompareOp::Equal: break;
        case CompareOp::Less: op = "<"; break;
        case CompareOp::LessEqual: op = "<="; break;
        case CompareOp::Greater: op = ">"; break;
        case CompareOp::GreaterEqual: op = ">="; break;
    }
    return "column " + std::to_string(p.column) + op + std::to_string(p.constant);
}
PhysicalPlan Unary(PlanKind kind, const PhysicalPlan& child, const Schema& schema) {
    PhysicalPlan n(kind, schema);
    n.children.push_back(child);
    n.rows = child.rows;
    n.exact_rows = child.exact_rows;
    n.row_upper_bound = child.row_upper_bound;
    n.input_rows = child.rows;
    n.ordering = child.ordering;
    n.ndv = child.ndv;
    n.confidence = child.confidence;
    n.temp_pages = child.temp_pages;
    n.subtree_cost = child.subtree_cost;
    n.pages = PackedPages(schema, n.rows);
    return n;
}
PhysicalPlan Project(const PhysicalPlan& child, const ProjectionPlan& projection) {
    auto n = Unary(PlanKind::Projection, child, ProjectedSchema(child.schema, projection));
    n.projection = projection;
    n.ordering = ProjectedOrdering(child.ordering, projection);
    n.ndv.clear();
    for (size_t c : projection.columns) {
        n.ndv.push_back(child.ndv[c]);
        n.details += std::to_string(c) + " ";
    }
    return n;
}
// Exact counts bound fan-in needs. Sampled cardinality never certifies a private-run-only sort.
PhysicalPlan Sort(const PhysicalPlan& child, size_t key, const QueryResources& resources, size_t bytes) {
    ValidateInt64Key(child.schema, key);
    auto n = Unary(PlanKind::ExternalSort, child, child.schema);
    n.column = key;
    n.ordering = {true, key};
    const auto m = ComputeSortMemory(resources, child.schema, bytes);
    n.sort_memory = m;
    if (child.row_upper_bound > m.run_capacity && m.merge_fan_in < 2)
        throw DbError("ExternalSort requires k >= 2 for possible multiple runs");
    const long double runs = std::ceil(child.rows / m.run_capacity);
    if (child.exact_rows && runs > m.descriptor_capacity)
        throw DbError("ExternalSort exact input exceeds initial run descriptor cap");
    long double remaining = runs;
    size_t levels = 0;
    while (remaining > 1 && remaining > m.merge_fan_in) {
        remaining = std::ceil(remaining / m.merge_fan_in);
        ++levels;
    }
    const long double pages = PackedPages(child.schema, child.rows);
    n.local_cost = {runs <= 1 ? 0 : (2 * levels + 2) * pages,
                    child.rows * std::log2(std::max(2.0L, child.rows))};
    n.subtree_cost = Add(child.subtree_cost, n.local_cost);
    n.temp_pages += runs <= 1 ? 0 : (levels + 1) * pages;
    n.details = "key=" + std::to_string(key) + " initial_runs=" + Number(runs) +
        " merge_levels=" + std::to_string(levels) +
        (runs <= 1 ? " predicted private run (no temp)" : " final merge streamed");
    if (std::ceil(child.row_upper_bound / m.run_capacity) > m.descriptor_capacity)
        n.details += "; finite descriptor cap may reject at runtime";
    return n;
}
Plans Scans(QueryContext& context, const BaseAccess& access, ScanChoice choice, ScanRingChoice ring,
            std::optional<size_t> useful_key, std::vector<std::string>& rejected) {
    const auto table = context.Db().Table(access.table);
    const auto* stats = context.Db().Statistics(access.table);
    const auto column_stats = [&](size_t c) -> const ColumnStatistics* {
        return stats ? &stats->columns[c] : nullptr;
    };
    for (const auto& p : access.predicates) p.Validate(table.schema);
    if (access.projection) (void)ProjectedSchema(table.schema, *access.projection);
    if (useful_key && access.projection) {
        if (*useful_key >= access.projection->columns.size()) throw DbError("invalid projected key column");
        useful_key = access.projection->columns[*useful_key];
    }
    bool relevant = choice == ScanChoice::IndexScan || useful_key == table.index_col;
    std::optional<int64_t> low, high;
    long double access_selectivity = 1;
    for (auto p : access.predicates) {
        if (p.column != table.index_col) continue;
        relevant = true;
        // Inclusive superset bounds; retain strict/equality predicates as real Filters.
        if (p.op == CompareOp::Equal || p.op == CompareOp::Greater || p.op == CompareOp::GreaterEqual)
            low = low ? std::max(*low, p.constant) : p.constant;
        if (p.op == CompareOp::Equal || p.op == CompareOp::Less || p.op == CompareOp::LessEqual)
            high = high ? std::min(*high, p.constant) : p.constant;
        if (p.op == CompareOp::Less) p.op = CompareOp::LessEqual;
        if (p.op == CompareOp::Greater) p.op = CompareOp::GreaterEqual;
        access_selectivity = std::min(access_selectivity,
            EstimateSelectivity(column_stats(p.column), table.live_rows, p));
    }
    if (low && high && *low > *high) access_selectivity = 0;
    Plans result;
    for (auto kind : {PlanKind::SeqScan, PlanKind::IndexScan}) {
        if ((choice == ScanChoice::SeqScan && kind != PlanKind::SeqScan) ||
            (choice == ScanChoice::IndexScan && kind != PlanKind::IndexScan)) continue;
        if (kind == PlanKind::IndexScan && (table.index_meta == kInvalidPageId || !relevant)) {
            Rejected(rejected, "IndexScan rejected: no matching index access/order requirement on " + access.table);
            continue;
        }
        PhysicalPlan n(kind, table.schema);
        n.rows = table.live_rows;
        n.exact_rows = true;
        n.row_upper_bound = table.live_rows;
        n.confidence = !stats ? "heuristic" : stats->stale ? "stale-sampled" : "fresh sampled";
        for (size_t c = 0; c < table.schema.size(); ++c)
            n.ndv.push_back(EstimateNDV(column_stats(c), n.rows));
        n.details = "table=" + access.table + " table_rows=" + Number(table.live_rows) +
            " heap_pages=" + Number(table.heap_pages) + " [exact metadata]";
        if (choice != ScanChoice::Auto) n.details += " [forced scan]";
        if (kind == PlanKind::SeqScan) {
            n.seq = {access.table, ring == ScanRingChoice::On ||
                (ring == ScanRingChoice::Auto && table.heap_pages > context.Db().Buffer().FrameCount() / 4)};
            n.local_cost.pages = table.heap_pages;
            n.details += " scan_ring=" + std::string(ring == ScanRingChoice::Auto ? "auto/" : "forced/") +
                (n.seq.use_scan_ring ? "on" : "off");
        } else {
            n.index = {access.table, table.index_col, low, high};
            n.rows *= access_selectivity;
            n.exact_rows = !low && !high;
            n.ordering = {true, table.index_col};
            const auto& index = context.Db().Index(access.table);
            const long double leaves = std::max(1.0L, std::ceil(n.rows / index.Capacity().leaf_max));
            n.local_cost.pages = index.Height() - 1 + leaves + n.rows;
            n.details += " index_column=" + std::to_string(table.index_col) +
                " range=[" + (low ? std::to_string(*low) : "MIN") + "," +
                (high ? std::to_string(*high) : "MAX") + "] height=" + std::to_string(index.Height()) +
                " leaf_pages=" + Number(leaves) + " heap_fetches=" + Number(n.rows);
        }
        n.pages = PackedPages(n.schema, n.rows);
        n.subtree_cost = n.local_cost;
        // Estimate each prefix once from the base cardinality, without double-applying index bounds.
        long double selectivity = 1;
        for (const auto& p : access.predicates) {
            auto f = Unary(PlanKind::Filter, n, n.schema);
            f.predicate = p;
            f.exact_rows = false;
            const auto fraction = EstimateSelectivity(column_stats(p.column), table.live_rows, p);
            selectivity *= fraction;
            f.rows = std::min(n.rows, table.live_rows * selectivity);
            f.pages = PackedPages(f.schema, f.rows);
            f.local_cost.cpu = n.rows;
            f.subtree_cost = Add(n.subtree_cost, f.local_cost);
            f.details = PredicateText(p) + " selectivity=" + Number(fraction);
            for (auto& d : f.ndv) d = std::min(d, f.rows);
            if (p.op == CompareOp::Equal) f.ndv[p.column] = std::min(1.0L, f.rows);
            n = std::move(f);
        }
        if (access.projection) n = Project(n, *access.projection);
        result.push_back(std::move(n));
    }
    return result;
}
Plans Joins(const Plans& left, const Plans& right, const EquiJoinSpec& keys,
            const QueryResources& resources, const PlanOptions& options, std::vector<std::string>& rejected) {
    Plans result;
    for (const auto& l : left) for (const auto& r : right) {
        ValidateInt64Key(l.schema, keys.left_key);
        ValidateInt64Key(r.schema, keys.right_key);
        std::vector<Column> columns;
        for (size_t c = 0; c < l.schema.size(); ++c) columns.push_back(l.schema.column(c));
        for (size_t c = 0; c < r.schema.size(); ++c) columns.push_back(r.schema.column(c));
        const auto schema = IntermediateSchema(std::move(columns));
        for (auto kind : {PlanKind::NLJ, PlanKind::BNLJ, PlanKind::GHJ}) {
            if ((options.join == JoinChoice::NLJ && kind != PlanKind::NLJ) ||
                (options.join == JoinChoice::BNLJ && kind != PlanKind::BNLJ) ||
                (options.join == JoinChoice::GHJ && kind != PlanKind::GHJ)) continue;
            for (auto side : {PhysicalOuter::Left, PhysicalOuter::Right}) {
                if (kind == PlanKind::GHJ && (side == PhysicalOuter::Right || options.outer)) continue;
                if (options.outer && side != *options.outer) continue;
                try {
                    PhysicalPlan n(kind, schema);
                    n.children = {l, r};
                    n.join = {keys.left_key, keys.right_key, side};
                    n.input_rows = l.rows + r.rows;
                    n.row_upper_bound = l.row_upper_bound * r.row_upper_bound;
                    n.rows = l.rows * r.rows / std::max({1.0L, l.ndv[keys.left_key], r.ndv[keys.right_key]});
                    n.pages = PackedPages(schema, n.rows);
                    n.ndv = l.ndv;
                    n.ndv.insert(n.ndv.end(), r.ndv.begin(), r.ndv.end());
                    for (auto& d : n.ndv) d = std::min(d, n.rows);
                    n.confidence = l.confidence + "/" + r.confidence + "; uniform join estimate";
                    const auto& outer = side == PhysicalOuter::Left ? l : r;
                    const auto& inner = side == PhysicalOuter::Left ? r : l;
                    if (kind == PlanKind::GHJ) {
                        n.grace_memory = ComputeGraceJoinMemory(resources, l.schema, r.schema,
                                                               schema, options.join_memory);
                        const auto pages = PackedPages(l.schema, l.rows) + PackedPages(r.schema, r.rows);
                        n.local_cost = {2 * pages, 3 * (l.rows + r.rows)};
                        n.subtree_cost = Add(Add(l.subtree_cost, r.subtree_cost), n.local_cost);
                        n.temp_pages = l.temp_pages + r.temp_pages + pages;
                        n.details = "skew uncertainty; runtime BNLJ overflow fallback enabled";
                    } else {
                        // QuerySpec only admits scan/filter/projection children, all of which support replay.
                        n.inner_executions = outer.rows;
                        if (kind == PlanKind::BNLJ) {
                            n.block_memory = ComputeBlockJoinMemory(resources, outer.schema, inner.schema,
                                                                   schema, options.join_memory);
                            n.inner_executions = std::ceil(outer.rows / n.block_memory->block_capacity);
                        }
                        n.local_cost.cpu = l.rows * r.rows;
                        n.subtree_cost = Add(Add(outer.subtree_cost, Times(inner.subtree_cost, n.inner_executions)),
                                             n.local_cost);
                        n.temp_pages = outer.temp_pages + n.inner_executions * inner.temp_pages;
                        n.details = std::string("physical_outer=") + (side == PhysicalOuter::Left ? "Left" : "Right") +
                            (options.outer ? " [forced outer]" : "") + " inner_executions=" + Number(n.inner_executions) +
                            " inner_replay_cost=" + Number(inner.subtree_cost.Score() * n.inner_executions);
                    }
                    n.details += " keys=" + std::to_string(keys.left_key) + "," + std::to_string(keys.right_key);
                    if (options.join != JoinChoice::Auto) n.details += " [forced join]";
                    result.push_back(std::move(n));
                } catch (const DbError& e) { Rejected(rejected, std::string(Name(kind)) + " rejected: " + e.what()); }
            }
        }
    }
    return result;
}
Plans Aggregates(const Plans& children, const AggregateSpec& spec, const QueryResources& resources,
                 const PlanOptions& options, std::vector<std::string>& rejected) {
    Plans result;
    for (const auto& child : children) {
        const auto schema = AggregateOutputSchema(child.schema, spec);
        for (auto kind : {PlanKind::HashAggregate, PlanKind::SortAggregate}) {
            if (!spec.group_column && kind == PlanKind::SortAggregate) continue;
            if ((options.aggregate == AggregateChoice::HashAggregate && kind != PlanKind::HashAggregate) ||
                (options.aggregate == AggregateChoice::SortAggregate && kind != PlanKind::SortAggregate)) continue;
            try {
                auto n = Unary(spec.group_column ? kind : PlanKind::GlobalAggregate, child, schema);
                n.aggregate = spec;
                n.exact_rows = !spec.group_column;
                n.rows = spec.group_column ? std::min(child.rows, child.ndv[*spec.group_column]) : 1;
                n.row_upper_bound = spec.group_column ? child.row_upper_bound : 1;
                n.pages = PackedPages(schema, n.rows);
                n.ndv.assign(schema.size(), n.rows);
                n.ordering = {};
                n.local_cost.cpu = child.rows;
                if (!spec.group_column || kind == PlanKind::HashAggregate) {
                    n.aggregate_memory = ComputeHashAggregateMemory(resources, child.schema, schema,
                        spec.group_column.has_value(), options.aggregate_memory);
                    if (spec.group_column) {
                        const auto m = *n.aggregate_memory;
                        const bool spill = n.rows > m.hash_group_capacity;
                        n.details = "group=" + std::to_string(*spec.group_column) + " estimated_groups=" + Number(n.rows) +
                            " predicted spill=" + (spill ? "yes" : "no") + " [uncertain]; output unordered";
                        n.local_cost.cpu = 2 * child.rows;
                        if (child.row_upper_bound > m.hash_group_capacity) {
                            // Worst-case one partial per raw row; checked even if sampled NDV predicts fit.
                            PhysicalPlan partials(PlanKind::SeqScan, AggregatePartialSchema());
                            partials.rows = spill ? child.rows : 0;
                            partials.row_upper_bound = child.row_upper_bound;
                            auto sort = Sort(partials, 0, resources, m.sort_memory_bytes);
                            n.details += "; spill path uses reduced internal sort budget=" + Number(m.sort_memory_bytes);
                            if (spill) {
                                const auto pages = PackedPages(partials.schema, partials.rows);
                                n.local_cost = Add(n.local_cost, {2 * pages, partials.rows});
                                n.local_cost = Add(n.local_cost, sort.local_cost);
                                n.temp_pages += pages + sort.temp_pages;
                                n.sort_memory = sort.sort_memory;
                                n.details += "; estimated_partial_records=" + Number(partials.rows) + " " + sort.details;
                            }
                        }
                    }
                } else {
                    const bool ordered = child.ordering == Ordering{true, *spec.group_column};
                    n.aggregate_memory = ComputeSortAggregateMemory(resources, child.schema, schema,
                        !ordered, options.aggregate_memory);
                    n.ordering = {true, 0};
                    n.details = "group=" + std::to_string(*spec.group_column) + " input ordered=" +
                        (ordered ? "yes; sort elided=yes" : "no; sort elided=no");
                    if (!ordered) {
                        auto sort = Sort(child, *spec.group_column, resources, n.aggregate_memory->sort_memory_bytes);
                        sort.details += "; internal reduced aggregate budget";
                        n.children = {std::move(sort)};
                        n.temp_pages = n.children[0].temp_pages;
                    } else n.details += "; Sort eliminated: input already ordered by column " +
                        std::to_string(*spec.group_column);
                }
                if (options.aggregate != AggregateChoice::Auto) n.details += " [forced aggregate]";
                n.subtree_cost = Add(n.children[0].subtree_cost, n.local_cost);
                result.push_back(std::move(n));
            } catch (const DbError& e) { Rejected(rejected, std::string(Name(kind)) + " rejected: " + e.what()); }
        }
    }
    return result;
}
std::unique_ptr<Executor> Build(QueryContext& q, const PhysicalPlan& n) {
    std::unique_ptr<Executor> left, right;
    if (n.kind == PlanKind::SortAggregate && n.aggregate_memory->sort_memory_bytes) {
        // The displayed child is the aggregate operator's internal sort; let the aggregate own and construct it.
        left = Build(q, n.children[0].children[0]);
    } else if (!n.children.empty()) left = Build(q, n.children[0]);
    if (n.children.size() == 2) right = Build(q, n.children[1]);
    switch (n.kind) {
        case PlanKind::SeqScan: return std::make_unique<SeqScan>(q, n.seq);
        case PlanKind::IndexScan: return std::make_unique<IndexScan>(q, n.index);
        case PlanKind::Filter: return std::make_unique<Filter>(q, FilterPlan{*n.predicate}, std::move(left));
        case PlanKind::Projection: return std::make_unique<Projection>(q, n.projection, std::move(left));
        case PlanKind::NLJ: return std::make_unique<NestedLoopJoin>(q, n.join, std::move(left), std::move(right));
        case PlanKind::BNLJ: return std::make_unique<BlockNestedLoopJoin>(q, n.join, std::move(left), std::move(right), n.block_memory->max_bytes);
        case PlanKind::GHJ: return std::make_unique<GraceHashJoin>(q,
            GraceHashJoinPlan{n.join.left_key, n.join.right_key, n.grace_memory->max_bytes}, std::move(left), std::move(right));
        case PlanKind::GlobalAggregate: case PlanKind::HashAggregate:
            return std::make_unique<HashAggregate>(q, n.aggregate, std::move(left), n.aggregate_memory->max_bytes);
        case PlanKind::SortAggregate:
            return std::make_unique<SortAggregate>(q, n.aggregate, std::move(left), n.aggregate_memory->max_bytes);
        case PlanKind::ExternalSort:
            return std::make_unique<ExternalSort>(q, ExternalSortPlan{n.column, n.sort_memory->max_bytes}, std::move(left));
    }
    throw DbError("unsupported plan node");
}
void ValidateOptions(const QuerySpec& spec, const PlanOptions& o) {
    if (spec.right.has_value() != spec.join.has_value()) throw DbError("right table and one equijoin must appear together");
    if (!spec.join && (o.join != JoinChoice::Auto || o.outer || o.right_scan != ScanChoice::Auto || o.join_memory))
        throw DbError("join options require a join");
    if (o.join == JoinChoice::GHJ && o.outer) throw DbError("GHJ has no physical outer option; build side is runtime");
    if ((!spec.aggregate || !spec.aggregate->group_column) && o.aggregate != AggregateChoice::Auto)
        throw DbError("forced aggregate choice requires grouping; global aggregation is separate");
    if (!spec.aggregate && o.aggregate_memory) throw DbError("aggregate memory requires an aggregate");
    if (o.outer && *o.outer != PhysicalOuter::Left && *o.outer != PhysicalOuter::Right)
        throw DbError("invalid physical outer");
    if (o.left_scan < ScanChoice::Auto || o.left_scan > ScanChoice::IndexScan ||
        o.right_scan < ScanChoice::Auto || o.right_scan > ScanChoice::IndexScan ||
        o.join < JoinChoice::Auto || o.join > JoinChoice::GHJ ||
        o.aggregate < AggregateChoice::Auto || o.aggregate > AggregateChoice::SortAggregate ||
        o.scan_ring < ScanRingChoice::Auto || o.scan_ring > ScanRingChoice::Off)
        throw DbError("unsupported plan option");
    for (auto bytes : {o.sort_memory, o.join_memory, o.aggregate_memory})
        if (bytes && !*bytes) throw DbError("operator memory must be positive");
}
void ExplainNode(std::ostringstream& out, const PhysicalPlan& n, size_t depth) {
    const std::string indent(depth * 2, ' ');
    out << indent << Name(n.kind) << " " << n.details << '\n' << indent
        << "  estimated_rows=" << n.rows << " input_rows=" << n.input_rows << " pages=" << n.pages
        << " local_cost=" << n.local_cost.Score() << " subtree_cost=" << n.subtree_cost.Score()
        << " (local pages=" << n.local_cost.pages << ", cpu=" << n.local_cost.cpu << ") [" << n.confidence << "]\n"
        << indent << "  order=" << (n.ordering.ordered ? "ascending column " + std::to_string(n.ordering.column) : "unordered")
        << " schema=[";
    for (size_t c = 0; c < n.schema.size(); ++c) {
        const auto& col = n.schema.column(c);
        out << (c ? "," : "") << col.name << ':' << (col.type == TypeId::Int64 ? "INT64" : col.type == TypeId::Double ? "DOUBLE" : "VARCHAR")
            << (col.nullable ? "?" : "");
    }
    out << "]\n";
    if (n.block_memory) {
        const auto m = *n.block_memory;
        out << indent << "  M=" << m.max_bytes << " Touter=" << m.outer_row_bytes << " S=" << m.scratch_bytes
            << " C_block=" << m.block_capacity << " estimated outer blocks=" << n.inner_executions << '\n';
    }
    if (n.grace_memory) {
        const auto m = *n.grace_memory;
        out << indent << "  M=" << m.max_bytes << " S=" << m.scratch_bytes << " p=" << m.partition_count
            << " Dghj=" << m.descriptor_bytes << " build_capacities=" << m.left_build_capacity << ',' << m.right_build_capacity
            << " fallback_M=" << m.fallback_memory << '\n';
    }
    if (n.aggregate_memory) {
        const auto m = *n.aggregate_memory;
        out << indent << "  M=" << m.max_bytes << " S=" << m.scratch_bytes << " group_capacity=" << m.hash_group_capacity
            << " internal_sort_budget=" << m.sort_memory_bytes << '\n';
    }
    if (n.sort_memory) {
        const auto m = *n.sort_memory;
        out << indent << "  " << (n.kind == PlanKind::HashAggregate ? "partial sort " : "")
            << "M=" << m.max_bytes << " Tinput=" << m.row_bytes << " S=" << m.scratch_bytes
            << " D=" << m.descriptor_capacity << " R=" << m.descriptor_bytes << " C_sort=" << m.run_capacity
            << " k=" << m.merge_fan_in << '\n';
    }
    for (const auto& child : n.children) ExplainNode(out, child, depth + 1);
}
}  // namespace

OptimizedPlan ChoosePlan(QueryContext& context, const QuerySpec& spec, const PlanOptions& options) {
    context.CheckActive();
    ValidateOptions(spec, options);
    std::vector<std::string> rejected;
    const auto useful = spec.join ? std::optional<size_t>(spec.join->left_key) :
        spec.aggregate ? spec.aggregate->group_column : spec.order_column;
    auto candidates = Scans(context, spec.left, options.left_scan, options.scan_ring, useful, rejected);
    if (spec.join) {
        auto right = Scans(context, *spec.right, options.right_scan, options.scan_ring, spec.join->right_key, rejected);
        candidates = Joins(candidates, right, *spec.join, context.Resources(), options, rejected);
    }
    if (spec.aggregate) candidates = Aggregates(candidates, *spec.aggregate, context.Resources(), options, rejected);
    std::optional<PhysicalPlan> best;
    for (auto candidate : candidates) {
        try {
            if (spec.order_column) {
                ValidateInt64Key(candidate.schema, *spec.order_column);
                if (candidate.ordering == Ordering{true, *spec.order_column})
                    candidate.details += "; Sort eliminated: input already ordered by column " + std::to_string(*spec.order_column);
                else candidate = Sort(candidate, *spec.order_column, context.Resources(),
                                       options.sort_memory.value_or(context.Resources().work_memory_bytes));
            }
            if (spec.projection) candidate = Project(candidate, *spec.projection);
            const auto score = candidate.subtree_cost.Score();
            // Stable enumeration wins relative ties; no dependency on container/hash iteration.
            if (!best || score < best->subtree_cost.Score() - 1e-9L * std::max(1.0L, best->subtree_cost.Score()))
                best = std::move(candidate);
        } catch (const DbError& e) { Rejected(rejected, std::string("final pipeline rejected: ") + e.what()); }
    }
    if (!best) {
        std::string message = "no feasible physical plan";
        for (const auto& reason : rejected) message += "; " + reason;
        throw DbError(message);
    }
    auto executor = Build(context, *best);
    return {std::move(*best), std::move(executor), context.Resources(), context.Db().Buffer().FrameCount(), std::move(rejected)};
}
std::string Explain(const OptimizedPlan& plan) {
    std::ostringstream out;
    out << std::setprecision(5) << "ValenceDB estimated algorithmic logical page transfers + 0.0001 * tuple CPU operations\n"
        << "F=" << plan.buffer_frames << " (shared BufferPool frames), max_temp_pages=" << plan.resources.max_temp_pages
        << ", estimated cumulative append-only scratch pages=" << plan.plan.temp_pages << " [uncertain]\n"
        << "Work memory is per heavyweight operator; simultaneously live operators each have their own M.\n"
        << "Algorithmic accounting, not a whole-query RSS guarantee. Estimates are not runtime measurements;\n"
        << "allocation initialization/cache effects are omitted; runtime TempStore enforces the cumulative cap.\n";
    ExplainNode(out, plan.plan, 0);
    for (const auto& reason : plan.rejected) out << reason << '\n';
    return out.str();
}

}  // namespace valencedb
