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
    n.ordering = child.ordering;
    n.ndv = child.ndv;
    n.subtree_cost = child.subtree_cost;
    return n;
}
PhysicalPlan Project(const PhysicalPlan& child, const ProjectionPlan& projection) {
    auto n = Unary(PlanKind::Projection, child, ProjectedSchema(child.schema, projection));
    n.projection = projection;
    n.ordering = ProjectedOrdering(child.ordering, projection);
    n.ndv.clear();
    for (size_t c : projection.columns) n.ndv.push_back(child.ndv[c]);
    return n;
}
struct SortEstimate {
    ModeledCost cost;
    long double initial_runs;
    size_t merge_levels;
};
// Sort work only: the caller pays for its input path. Exact bounds govern feasibility.
SortEstimate EstimateSort(const Schema& schema, long double rows, long double row_upper_bound,
                          bool exact_rows, const QueryResources& resources, size_t bytes) {
    const auto m = ComputeSortMemory(resources, schema, bytes);
    if (row_upper_bound > m.run_capacity && m.merge_fan_in < 2)
        throw DbError("ExternalSort requires k >= 2 for possible multiple runs");
    const long double runs = std::ceil(rows / m.run_capacity);
    if (exact_rows && runs > m.descriptor_capacity)
        throw DbError("ExternalSort exact input exceeds initial run descriptor cap");
    long double remaining = runs;
    size_t levels = 0;
    while (remaining > 1 && remaining > m.merge_fan_in) {
        remaining = std::ceil(remaining / m.merge_fan_in);
        ++levels;
    }
    const long double pages = PackedPages(schema, rows);
    return {{runs <= 1 ? 0 : (2 * levels + 2) * pages,
             rows * std::log2(std::max(2.0L, rows))}, runs, levels};
}
PhysicalPlan Sort(const PhysicalPlan& child, size_t key, const QueryResources& resources, size_t bytes) {
    ValidateInt64Key(child.schema, key);
    const auto estimate = EstimateSort(child.schema, child.rows, child.row_upper_bound,
                                       child.exact_rows, resources, bytes);
    auto n = Unary(PlanKind::ExternalSort, child, child.schema);
    n.column = key;
    n.ordering = {true, key};
    n.memory_bytes = bytes;
    n.local_cost = estimate.cost;
    n.subtree_cost = Add(child.subtree_cost, n.local_cost);
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
        n.statistics = !stats ? StatisticsSource::Heuristic :
            stats->stale ? StatisticsSource::Stale : StatisticsSource::Sampled;
        for (size_t c = 0; c < table.schema.size(); ++c)
            n.ndv.push_back(EstimateNDV(column_stats(c), n.rows));
        if (kind == PlanKind::SeqScan) {
            n.seq = {access.table, ring == ScanRingChoice::On ||
                (ring == ScanRingChoice::Auto && table.heap_pages > context.Db().Buffer().FrameCount() / 4)};
            n.local_cost.pages = table.heap_pages;
        } else {
            n.index = {access.table, table.index_col, low, high};
            n.rows *= access_selectivity;
            n.exact_rows = !low && !high;
            n.ordering = {true, table.index_col};
            const auto& index = context.Db().Index(access.table);
            const long double leaves = std::max(1.0L, std::ceil(n.rows / index.Capacity().leaf_max));
            n.local_cost.pages = index.Height() - 1 + leaves + n.rows;
        }
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
            f.local_cost.cpu = n.rows;
            f.subtree_cost = Add(n.subtree_cost, f.local_cost);
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
                    n.row_upper_bound = l.row_upper_bound * r.row_upper_bound;
                    n.rows = l.rows * r.rows / std::max({1.0L, l.ndv[keys.left_key], r.ndv[keys.right_key]});
                    n.ndv = l.ndv;
                    n.ndv.insert(n.ndv.end(), r.ndv.begin(), r.ndv.end());
                    for (auto& d : n.ndv) d = std::min(d, n.rows);
                    const auto& outer = side == PhysicalOuter::Left ? l : r;
                    const auto& inner = side == PhysicalOuter::Left ? r : l;
                    if (kind == PlanKind::GHJ) {
                        const auto memory = ComputeGraceJoinMemory(resources, l.schema, r.schema,
                                                                   schema, options.join_memory);
                        n.memory_bytes = memory.max_bytes;
                        const auto pages = PackedPages(l.schema, l.rows) + PackedPages(r.schema, r.rows);
                        n.local_cost = {2 * pages, 3 * (l.rows + r.rows)};
                        n.subtree_cost = Add(Add(l.subtree_cost, r.subtree_cost), n.local_cost);
                    } else {
                        // Scan/filter/projection inputs support replay of the whole inner path.
                        long double inner_executions = outer.rows;
                        if (kind == PlanKind::BNLJ) {
                            const auto memory = ComputeBlockJoinMemory(resources, outer.schema, inner.schema,
                                                                       schema, options.join_memory);
                            n.memory_bytes = memory.max_bytes;
                            inner_executions = std::ceil(outer.rows / memory.block_capacity);
                        }
                        n.local_cost.cpu = l.rows * r.rows;
                        n.subtree_cost = Add(Add(outer.subtree_cost, Times(inner.subtree_cost, inner_executions)),
                                             n.local_cost);
                    }
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
                n.ndv.assign(schema.size(), n.rows);
                n.ordering = {};
                n.local_cost.cpu = child.rows;
                if (!spec.group_column || kind == PlanKind::HashAggregate) {
                    const auto memory = ComputeHashAggregateMemory(resources, child.schema, schema,
                        spec.group_column.has_value(), options.aggregate_memory);
                    n.memory_bytes = memory.max_bytes;
                    if (spec.group_column) {
                        const bool spill = n.rows > memory.hash_group_capacity;
                        n.local_cost.cpu = 2 * child.rows;
                        if (child.row_upper_bound > memory.hash_group_capacity) {
                            // Check the spill path even when sampled NDV predicts no spill.
                            // At most one partial per raw row; partial count is not exact.
                            const auto partial_schema = AggregatePartialSchema();
                            const auto sort = EstimateSort(partial_schema, spill ? child.rows : 0,
                                child.row_upper_bound, false, resources, memory.sort_memory_bytes);
                            if (spill) {
                                const auto pages = PackedPages(partial_schema, child.rows);
                                // Partial write + TempScan read, combination CPU, then sort work.
                                n.local_cost = Add(n.local_cost, {2 * pages, child.rows});
                                n.local_cost = Add(n.local_cost, sort.cost);
                            }
                        }
                    }
                } else {
                    const auto memory = ComputeSortAggregateMemory(resources, child.schema, schema,
                        n.NeedsInternalSort(), options.aggregate_memory);
                    n.memory_bytes = memory.max_bytes;
                    n.ordering = {true, 0};
                    if (n.NeedsInternalSort()) {
                        const auto sort = EstimateSort(child.schema, child.rows, child.row_upper_bound,
                            child.exact_rows, resources, memory.sort_memory_bytes);
                        n.local_cost = Add(n.local_cost, sort.cost);
                    }
                }
                n.subtree_cost = Add(n.children[0].subtree_cost, n.local_cost);
                result.push_back(std::move(n));
            } catch (const DbError& e) { Rejected(rejected, std::string(Name(kind)) + " rejected: " + e.what()); }
        }
    }
    return result;
}
std::unique_ptr<Executor> Build(QueryContext& q, const PhysicalPlan& n) {
    std::unique_ptr<Executor> left, right;
    if (!n.children.empty()) left = Build(q, n.children[0]);
    if (n.children.size() == 2) right = Build(q, n.children[1]);
    switch (n.kind) {
        case PlanKind::SeqScan: return std::make_unique<SeqScan>(q, n.seq);
        case PlanKind::IndexScan: return std::make_unique<IndexScan>(q, n.index);
        case PlanKind::Filter: return std::make_unique<Filter>(q, FilterPlan{*n.predicate}, std::move(left));
        case PlanKind::Projection: return std::make_unique<Projection>(q, n.projection, std::move(left));
        case PlanKind::NLJ: return std::make_unique<NestedLoopJoin>(q, n.join, std::move(left), std::move(right));
        case PlanKind::BNLJ: return std::make_unique<BlockNestedLoopJoin>(q, n.join, std::move(left), std::move(right), n.memory_bytes);
        case PlanKind::GHJ: return std::make_unique<GraceHashJoin>(q,
            GraceHashJoinPlan{n.join.left_key, n.join.right_key, n.memory_bytes}, std::move(left), std::move(right));
        case PlanKind::GlobalAggregate: case PlanKind::HashAggregate:
            return std::make_unique<HashAggregate>(q, n.aggregate, std::move(left), n.memory_bytes);
        case PlanKind::SortAggregate:
            return std::make_unique<SortAggregate>(q, n.aggregate, std::move(left), n.memory_bytes);
        case PlanKind::ExternalSort:
            return std::make_unique<ExternalSort>(q, ExternalSortPlan{n.column, n.memory_bytes}, std::move(left));
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
void ExplainSort(std::ostringstream& out, const std::string& indent, const Schema& schema,
                 long double rows, long double row_upper_bound, bool exact_rows,
                 const QueryResources& resources, size_t bytes) {
    const auto m = ComputeSortMemory(resources, schema, bytes);
    const auto estimate = EstimateSort(schema, rows, row_upper_bound, exact_rows, resources, bytes);
    out << indent << "M=" << m.max_bytes << " Tinput=" << m.row_bytes << " S=" << m.scratch_bytes
        << " D=" << m.descriptor_capacity << " R=" << m.descriptor_bytes << " C_sort=" << m.run_capacity
        << " k=" << m.merge_fan_in << " initial_runs=" << estimate.initial_runs
        << " merge_levels=" << estimate.merge_levels
        << (estimate.initial_runs <= 1 ? " predicted private run (no temp)" : " final merge streamed");
    if (std::ceil(row_upper_bound / m.run_capacity) > m.descriptor_capacity)
        out << "; finite descriptor cap may reject at runtime";
    out << '\n';
}
void ExplainNode(std::ostringstream& out, const PhysicalPlan& n, const QueryResources& resources, size_t depth) {
    const std::string indent(depth * 2, ' ');
    out << indent << Name(n.kind);
    switch (n.kind) {
        case PlanKind::SeqScan:
            out << " table=" << n.seq.table << " scan_ring=" << (n.seq.use_scan_ring ? "on" : "off");
            break;
        case PlanKind::IndexScan:
            out << " table=" << n.index.table << " index_column=" << n.index.column
                << " range=[" << (n.index.low ? std::to_string(*n.index.low) : "MIN") << ','
                << (n.index.high ? std::to_string(*n.index.high) : "MAX") << "] heap_fetches=" << n.rows;
            break;
        case PlanKind::Filter: out << ' ' << PredicateText(*n.predicate); break;
        case PlanKind::Projection:
            out << " columns=";
            for (size_t column : n.projection.columns) out << column << ' ';
            break;
        case PlanKind::NLJ: case PlanKind::BNLJ: case PlanKind::GHJ:
            out << " keys=" << n.join.left_key << ',' << n.join.right_key;
            if (n.kind == PlanKind::GHJ)
                out << " skew uncertain; runtime BNLJ overflow fallback enabled";
            else out << " physical_outer=" << (n.join.outer == PhysicalOuter::Left ? "Left" : "Right");
            break;
        case PlanKind::GlobalAggregate: break;
        case PlanKind::HashAggregate: case PlanKind::SortAggregate:
            out << " group=" << *n.aggregate.group_column;
            if (n.kind == PlanKind::SortAggregate)
                out << " internal_sort=" << (n.NeedsInternalSort() ? "yes" : "no")
                    << " sort elided=" << (n.NeedsInternalSort() ? "no" : "yes");
            break;
        case PlanKind::ExternalSort: out << " key=" << n.column; break;
    }
    if (n.kind == PlanKind::SeqScan || n.kind == PlanKind::IndexScan) {
        out << " [" << (n.statistics == StatisticsSource::Heuristic ? "heuristic" :
            n.statistics == StatisticsSource::Stale ? "stale-sampled" : "fresh sampled") << ']';
    }
    long double input_rows = 0;
    for (const auto& child : n.children) input_rows += child.rows;
    out << '\n' << indent << "  estimated_rows=" << n.rows << " input_rows=" << input_rows
        << " row_upper_bound=" << n.row_upper_bound << " exact_rows=" << (n.exact_rows ? "yes" : "no")
        << " packed_pages=" << PackedPages(n.schema, n.rows)
        << " local_cost=" << n.local_cost.Score() << " subtree_cost=" << n.subtree_cost.Score()
        << " (local pages=" << n.local_cost.pages << ", cpu=" << n.local_cost.cpu << ")\n"
        << indent << "  order=" << (n.ordering.ordered ? "ascending column " + std::to_string(n.ordering.column) : "unordered")
        << " schema=[";
    for (size_t c = 0; c < n.schema.size(); ++c) {
        const auto& col = n.schema.column(c);
        out << (c ? "," : "") << col.name << ':' << (col.type == TypeId::Int64 ? "INT64" : col.type == TypeId::Double ? "DOUBLE" : "VARCHAR")
            << (col.nullable ? "?" : "");
    }
    out << "]\n";
    if (n.kind == PlanKind::NLJ || n.kind == PlanKind::BNLJ) {
        const auto& outer = n.children[n.join.outer == PhysicalOuter::Left ? 0 : 1];
        const auto& inner = n.children[n.join.outer == PhysicalOuter::Left ? 1 : 0];
        long double replays = outer.rows;
        if (n.kind == PlanKind::BNLJ) {
            const auto m = ComputeBlockJoinMemory(resources, outer.schema, inner.schema, n.schema, n.memory_bytes);
            replays = std::ceil(outer.rows / m.block_capacity);
            out << indent << "  M=" << m.max_bytes << " Touter=" << m.outer_row_bytes << " S=" << m.scratch_bytes
                << " C_block=" << m.block_capacity << '\n';
        }
        out << indent << "  inner_executions=" << replays
            << " inner_replay_cost=" << inner.subtree_cost.Score() * replays << '\n';
    }
    if (n.kind == PlanKind::GHJ) {
        const auto m = ComputeGraceJoinMemory(resources, n.children[0].schema, n.children[1].schema, n.schema, n.memory_bytes);
        out << indent << "  M=" << m.max_bytes << " S=" << m.scratch_bytes << " p=" << m.partition_count
            << " Dghj=" << m.descriptor_bytes << " build_capacities=" << m.left_build_capacity << ',' << m.right_build_capacity
            << " fallback_M=" << m.fallback_memory << '\n';
    }
    if (n.kind == PlanKind::GlobalAggregate || n.kind == PlanKind::HashAggregate || n.kind == PlanKind::SortAggregate) {
        const auto& child = n.children[0];
        const auto m = n.kind == PlanKind::SortAggregate
            ? ComputeSortAggregateMemory(resources, child.schema, n.schema, n.NeedsInternalSort(), n.memory_bytes)
            : ComputeHashAggregateMemory(resources, child.schema, n.schema, n.aggregate.group_column.has_value(), n.memory_bytes);
        out << indent << "  M=" << m.max_bytes << " S=" << m.scratch_bytes << " group_capacity=" << m.hash_group_capacity
            << " internal_sort_budget=" << m.sort_memory_bytes << '\n';
        if (n.NeedsInternalSort()) {
            ExplainSort(out, indent + "  internal sort: ", child.schema, child.rows, child.row_upper_bound,
                        child.exact_rows, resources, m.sort_memory_bytes);
        }
        if (n.kind == PlanKind::HashAggregate) {
            const bool spill = n.rows > m.hash_group_capacity;
            out << indent << "  predicted spill=" << (spill ? "yes" : "no") << " [uncertain]; output unordered\n";
            if (child.row_upper_bound > m.hash_group_capacity) {
                ExplainSort(out, indent + "  partial sort (if spill): ", AggregatePartialSchema(),
                            spill ? child.rows : 0, child.row_upper_bound, false, resources, m.sort_memory_bytes);
            }
        }
    }
    if (n.kind == PlanKind::ExternalSort) {
        const auto& child = n.children[0];
        ExplainSort(out, indent + "  ", child.schema, child.rows, child.row_upper_bound,
                    child.exact_rows, resources, *n.memory_bytes);
    }
    for (const auto& child : n.children) ExplainNode(out, child, resources, depth + 1);
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
    std::optional<size_t> elided_order_column;
    for (auto candidate : candidates) {
        try {
            std::optional<size_t> elided_order;
            if (spec.order_column) {
                ValidateInt64Key(candidate.schema, *spec.order_column);
                if (candidate.ordering == Ordering{true, *spec.order_column})
                    elided_order = spec.order_column;
                else candidate = Sort(candidate, *spec.order_column, context.Resources(),
                                       options.sort_memory.value_or(context.Resources().work_memory_bytes));
            }
            if (spec.projection) candidate = Project(candidate, *spec.projection);
            const auto score = candidate.subtree_cost.Score();
            // Stable enumeration wins relative ties; no dependency on container/hash iteration.
            if (!best || score < best->subtree_cost.Score() - 1e-9L * std::max(1.0L, best->subtree_cost.Score())) {
                best = std::move(candidate);
                elided_order_column = elided_order;
            }
        } catch (const DbError& e) { Rejected(rejected, std::string("final pipeline rejected: ") + e.what()); }
    }
    if (!best) {
        std::string message = "no feasible physical plan";
        for (const auto& reason : rejected) message += "; " + reason;
        throw DbError(message);
    }
    auto executor = Build(context, *best);
    return {std::move(*best), std::move(executor), context.Resources(), context.Db().Buffer().FrameCount(),
            std::move(rejected), options, elided_order_column};
}
std::string Explain(const OptimizedPlan& plan) {
    std::ostringstream out;
    out << std::setprecision(5) << "ValenceDB estimated algorithmic logical page transfers + 0.0001 * tuple CPU operations\n"
        << "F=" << plan.buffer_frames << " (shared BufferPool frames), max_temp_pages=" << plan.resources.max_temp_pages
        << '\n'
        << "Work memory is per heavyweight operator; simultaneously live operators each have their own M.\n"
        << "Algorithmic accounting, not a whole-query RSS guarantee. Estimates are not runtime measurements;\n"
        << "allocation initialization/cache effects are omitted; runtime TempStore enforces the cumulative cap.\n";
    const auto& options = plan.options;
    if (options.left_scan != ScanChoice::Auto)
        out << "[forced scan] left=" << (options.left_scan == ScanChoice::SeqScan ? "SeqScan" : "IndexScan") << '\n';
    if (options.right_scan != ScanChoice::Auto)
        out << "[forced scan] right=" << (options.right_scan == ScanChoice::SeqScan ? "SeqScan" : "IndexScan") << '\n';
    if (options.join != JoinChoice::Auto) out << "[forced join] " <<
        (options.join == JoinChoice::NLJ ? "NLJ" : options.join == JoinChoice::BNLJ ? "BNLJ" : "GHJ") << '\n';
    if (options.outer) out << "[forced outer] " << (*options.outer == PhysicalOuter::Left ? "Left" : "Right") << '\n';
    if (options.aggregate != AggregateChoice::Auto) out << "[forced aggregate] " <<
        (options.aggregate == AggregateChoice::HashAggregate ? "HashAggregate" : "SortAggregate") << '\n';
    if (options.scan_ring != ScanRingChoice::Auto)
        out << "[forced scan ring] " << (options.scan_ring == ScanRingChoice::On ? "on" : "off") << '\n';
    if (plan.elided_order_column)
        out << "Final Sort eliminated: input already ordered by column " << *plan.elided_order_column
            << " before final projection\n";
    ExplainNode(out, plan.plan, plan.resources, 0);
    for (const auto& reason : plan.rejected) out << reason << '\n';
    return out.str();
}

}  // namespace valencedb
