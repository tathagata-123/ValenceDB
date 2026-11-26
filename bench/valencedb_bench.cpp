#include "datasets.hpp"
#include "valencedb/optimizer/optimizer.hpp"
#include "valencedb/execution/basic_executors.hpp"

#include <bit>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>

using namespace valencedb;
using namespace valencedb::bench;
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

namespace {
void Require(bool condition, const std::string& reason) {
    if (!condition) throw std::runtime_error(reason);
}
std::string Quote(const std::string& s) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') out << '\\' << c;
        else if (c < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c);
        else out << c;
    }
    out << '"';
    return out.str();
}
// Small output-only JSON object. All strings are escaped; no JSON dependency.
struct Json {
    std::map<std::string, std::string> fields;
    void String(const std::string& key, const std::string& value) { fields[key] = Quote(value); }
    void Number(const std::string& key, uint64_t value) { fields[key] = std::to_string(value); }
    void Real(const std::string& key, double value) {
        std::ostringstream s; s << std::setprecision(17) << value; fields[key] = s.str();
    }
    void Bool(const std::string& key, bool value) { fields[key] = value ? "true" : "false"; }
    void Object(const std::string& key, const Json& value) { fields[key] = value.Dump(); }
    std::string Dump() const {
        std::string out = "{";
        for (const auto& [key, value] : fields) {
            if (out.size() > 1) out += ',';
            out += Quote(key) + ':' + value;
        }
        return out + '}';
    }
};
struct Config {
    std::string family = "sort", experiment = "manual", scan = "seq", right_scan = "seq";
    std::string join = "bnlj", outer = "auto", aggregate = "hash", ring = "off";
    std::string distribution = "uniform", measures = "mixed", range = "short", work_dir;
    uint64_t seed = 130013;
    size_t trial = 1, rows = 240, right_rows = 120, domain = 60, groups = 60, payload = 64;
    size_t frames = 16, memory = 32768, temp_limit = 16384;
    size_t sort_memory = 0, join_memory = 0, aggregate_memory = 0;
    size_t hot_pages = 4, scan_batch = 64, lookups = 8;
    bool reference = false, count_star = false, control = false;
};
size_t Integer(const std::string& s) {
    size_t n = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), n);
    Require(r.ec == std::errc{} && r.ptr == s.data() + s.size(), "invalid unsigned integer: " + s);
    return n;
}
void Parse(Config& c, int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (key == "--help") {
            std::cerr << "valencedb_bench --family clock|btree_point|btree_range|btree_insert|sort|join|aggregate\n"
                         "  --experiment NAME --seed N --trial N --rows N --right-rows N\n"
                         "  --domain N --groups N --distribution unique|sequential|uniform|duplicate|all_equal|skewed|uneven\n"
                         "  --measures mixed|positive|constant --payload N --frames F --memory M\n"
                         "  --sort-memory M --join-memory M --aggregate-memory M --max-temp-pages N\n"
                         "  --scan auto|seq|index --right-scan auto|seq|index --join auto|nlj|bnlj|ghj\n"
                         "  --outer auto|left|right --aggregate auto|hash|sort --ring auto|on|off\n"
                         "  --range short|medium|long --lookups N --hot-pages N --scan-batch N\n"
                         "  --reference --count-star --control --work-dir NEW_DIRECTORY\n"
                         "One schema-version-1 JSON record on stdout; help on stderr.\n";
            std::exit(0);
        }
        if (key == "--reference") { c.reference = true; continue; }
        if (key == "--count-star") { c.count_star = true; continue; }
        if (key == "--control") { c.control = true; continue; }
        Require(i + 1 < argc, "missing value for " + key);
        std::string value = argv[++i];
        if (key == "--family") c.family = value;
        else if (key == "--experiment") c.experiment = value;
        else if (key == "--seed") c.seed = Integer(value);
        else if (key == "--trial") c.trial = Integer(value);
        else if (key == "--rows") c.rows = Integer(value);
        else if (key == "--right-rows") c.right_rows = Integer(value);
        else if (key == "--domain") c.domain = Integer(value);
        else if (key == "--groups") c.groups = Integer(value);
        else if (key == "--payload") c.payload = Integer(value);
        else if (key == "--frames") c.frames = Integer(value);
        else if (key == "--memory") c.memory = Integer(value);
        else if (key == "--max-temp-pages") c.temp_limit = Integer(value);
        else if (key == "--sort-memory") { c.sort_memory = Integer(value); Require(c.sort_memory > 0, "sort override must be positive"); }
        else if (key == "--join-memory") { c.join_memory = Integer(value); Require(c.join_memory > 0, "join override must be positive"); }
        else if (key == "--aggregate-memory") { c.aggregate_memory = Integer(value); Require(c.aggregate_memory > 0, "aggregate override must be positive"); }
        else if (key == "--hot-pages") c.hot_pages = Integer(value);
        else if (key == "--scan-batch") c.scan_batch = Integer(value);
        else if (key == "--lookups") c.lookups = Integer(value);
        else if (key == "--distribution") c.distribution = value;
        else if (key == "--measures") c.measures = value;
        else if (key == "--range") c.range = value;
        else if (key == "--work-dir") c.work_dir = value;
        else if (key == "--scan") c.scan = value;
        else if (key == "--right-scan") c.right_scan = value;
        else if (key == "--join") c.join = value;
        else if (key == "--outer") c.outer = value;
        else if (key == "--aggregate") c.aggregate = value;
        else if (key == "--ring") c.ring = value;
        else throw std::runtime_error("unknown option: " + key);
    }
    Require(c.frames >= 8 && c.memory && c.temp_limit && c.domain && c.groups && c.payload <= 256,
            "require F>=8, positive M/temp/domain/groups, payload<=256");
    Require(c.distribution != "uneven" || c.groups > 1, "uneven needs at least two groups");
    Require(c.family == "clock" || c.family == "btree_point" || c.family == "btree_range" ||
            c.family == "btree_insert" || c.family == "sort" || c.family == "join" || c.family == "aggregate",
            "unknown experiment family");
}
Json Configuration(const Config& c) {
    Json j;
#define CN(field) j.Number(#field, c.field)
#define CS(field) j.String(#field, c.field)
    CN(seed); CN(trial); CN(rows); CN(right_rows); CN(domain); CN(groups); CN(payload);
    CN(frames); CN(memory); CN(temp_limit); CN(sort_memory); CN(join_memory); CN(aggregate_memory);
    CN(hot_pages); CN(scan_batch); CN(lookups);
    CS(family); CS(experiment); CS(scan); CS(right_scan); CS(join); CS(outer); CS(aggregate);
    CS(ring); CS(distribution); CS(measures); CS(range);
#undef CN
#undef CS
    j.Bool("reference", c.reference); j.Bool("count_star", c.count_star); j.Bool("control", c.control);
    return j;
}

// Fixed byte encoding for diagnostic hashes AND exact reference bag keys.
// Type tags: 1=INT64, 2=VARCHAR, 3=DOUBLE; each has a NULL marker.
void Word(std::string& bytes, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) bytes.push_back(static_cast<char>((value >> (8 * i)) & 255));
}
std::string Canonical(const Tuple& row) {
    std::string bytes;
    Word(bytes, row.size());
    for (size_t i = 0; i < row.size(); ++i) {
        const auto& v = row.value(i);
        bytes.push_back(v.type() == TypeId::Int64 ? 1 : v.type() == TypeId::Varchar ? 2 : 3);
        bytes.push_back(v.is_null() ? 1 : 0);
        if (v.is_null()) continue;
        if (v.type() == TypeId::Int64) Word(bytes, static_cast<uint64_t>(v.as_int64()));
        else if (v.type() == TypeId::Double) Word(bytes, std::bit_cast<uint64_t>(v.as_double()));
        else { Word(bytes, v.as_string().size()); bytes += v.as_string(); }
    }
    return bytes;
}
uint64_t Mix(uint64_t x) {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
std::string Hex(uint64_t n) {
    std::ostringstream s; s << std::hex << std::setw(16) << std::setfill('0') << n; return s.str();
}
struct Digest {
    uint64_t count = 0, x = 0, sum = 0, mixed = 0, ordered = 14695981039346656037ULL;
    void Add(const Tuple& row) {
        uint64_t h = 14695981039346656037ULL;
        for (unsigned char b : Canonical(row)) { h ^= b; h *= 1099511628211ULL; }
        ++count; x ^= h; sum += h; mixed += Mix(h); ordered = Mix(ordered ^ h);
    }
    Json Record() const {
        Json j; j.Number("count", count); j.String("xor", Hex(x)); j.String("sum", Hex(sum));
        j.String("mixed_sum", Hex(mixed)); return j;
    }
    bool Same(const Digest& b) const { return Record().Dump() == b.Record().Dump(); }
};
using Bag = std::map<std::string, size_t>;
void AddReference(Bag& bag, const Tuple& row, size_t& count) {
    Require(++count <= 1000000, "reference output exceeds one million rows; omit --reference at scale");
    ++bag[Canonical(row)];
}
Tuple Joined(const Tuple& a, const Tuple& b) {
    std::vector<Value> values;
    for (size_t i = 0; i < a.size(); ++i) values.push_back(a.value(i));
    for (size_t i = 0; i < b.size(); ++i) values.push_back(b.value(i));
    return Tuple(std::move(values));
}

ScanChoice Scan(const std::string& s) {
    if (s == "seq") return ScanChoice::SeqScan;
    if (s == "index") return ScanChoice::IndexScan;
    Require(s == "auto", "invalid scan choice"); return ScanChoice::Auto;
}
PlanOptions Options(const Config& c) {
    PlanOptions o;
    o.left_scan = Scan(c.scan);
    if (c.family == "join") o.right_scan = Scan(c.right_scan);
    if (c.family == "join") {
        if (c.join == "nlj") o.join = JoinChoice::NLJ;
        else if (c.join == "bnlj") o.join = JoinChoice::BNLJ;
        else if (c.join == "ghj") o.join = JoinChoice::GHJ;
        else Require(c.join == "auto", "invalid join");
        if (c.outer == "left") o.outer = PhysicalOuter::Left;
        else if (c.outer == "right") o.outer = PhysicalOuter::Right;
        else Require(c.outer == "auto", "invalid outer");
    }
    if (c.family == "aggregate") {
        if (c.aggregate == "hash") o.aggregate = AggregateChoice::HashAggregate;
        else if (c.aggregate == "sort") o.aggregate = AggregateChoice::SortAggregate;
        else Require(c.aggregate == "auto", "invalid aggregate");
    }
    if (c.ring == "on") o.scan_ring = ScanRingChoice::On;
    else if (c.ring == "off") o.scan_ring = ScanRingChoice::Off;
    else Require(c.ring == "auto", "invalid ring");
    if (c.sort_memory) o.sort_memory = c.sort_memory;
    if (c.join_memory) o.join_memory = c.join_memory;
    if (c.aggregate_memory) o.aggregate_memory = c.aggregate_memory;
    return o;
}
std::string Kind(PlanKind k) {
    switch (k) {
        case PlanKind::SeqScan: return "SeqScan";
        case PlanKind::IndexScan: return "IndexScan";
        case PlanKind::Filter: return "Filter";
        case PlanKind::Projection: return "Projection";
        case PlanKind::NLJ: return "NLJ";
        case PlanKind::BNLJ: return "BNLJ";
        case PlanKind::GHJ: return "GHJ";
        case PlanKind::HashAggregate: return "HashAggregate";
        case PlanKind::SortAggregate: return "SortAggregate";
        case PlanKind::GlobalAggregate: return "GlobalAggregate";
        case PlanKind::ExternalSort: return "ExternalSort";
    }
    throw std::runtime_error("unknown plan kind");
}
Json PlanRecord(const PhysicalPlan& p, const Config& c) {
    Json j; j.String("operator", Kind(p.kind));
    j.Bool("ordered", p.ordering.ordered);
    if (p.ordering.ordered) j.Number("order_column", p.ordering.column);
    j.Real("estimated_rows", static_cast<double>(p.rows));
    j.Real("estimated_local_pages", static_cast<double>(p.local_cost.pages));
    j.Real("estimated_subtree_pages", static_cast<double>(p.subtree_cost.pages));
    j.Real("estimated_local_cpu", static_cast<double>(p.local_cost.cpu));
    j.Real("estimated_subtree_cpu", static_cast<double>(p.subtree_cost.cpu));
    j.Real("estimated_score", static_cast<double>(p.subtree_cost.Score()));
    if (p.memory_bytes) j.Number("memory_bytes", *p.memory_bytes);
    if (p.kind == PlanKind::SortAggregate) j.Bool("internal_sort", p.NeedsInternalSort());
    if (p.kind == PlanKind::SeqScan || p.kind == PlanKind::IndexScan) {
        const auto& table = p.kind == PlanKind::SeqScan ? p.seq.table : p.index.table;
        const auto requested = table == "right" ? c.right_scan : c.scan;
        Require(requested == "auto" || (requested == "seq" && p.kind == PlanKind::SeqScan) ||
                (requested == "index" && p.kind == PlanKind::IndexScan), "forced scan mismatch");
        j.String("table", table);
        if (p.kind == PlanKind::SeqScan) {
            j.Bool("scan_ring", p.seq.use_scan_ring);
            Require(c.ring == "auto" || p.seq.use_scan_ring == (c.ring == "on"), "forced ring mismatch");
        }
    }
    if (p.kind == PlanKind::NLJ || p.kind == PlanKind::BNLJ) {
        j.String("physical_outer", p.join.outer == PhysicalOuter::Left ? "left" : "right");
        Require(c.outer == "auto" || (c.outer == "left") == (p.join.outer == PhysicalOuter::Left),
                "forced outer mismatch");
    }
    std::string children = "[";
    for (const auto& child : p.children) {
        if (children.size() > 1) children += ',';
        children += PlanRecord(child, c).Dump();
    }
    j.fields["children"] = children + ']';
    return j;
}
void CheckRoot(const Config& c, Executor& root) {
    if (c.family == "join") {
        Require(c.join == "auto" || (c.join == "nlj" && dynamic_cast<NestedLoopJoin*>(&root)) ||
                (c.join == "bnlj" && dynamic_cast<BlockNestedLoopJoin*>(&root)) ||
                (c.join == "ghj" && dynamic_cast<GraceHashJoin*>(&root)), "forced join executor mismatch");
    }
    if (c.family == "aggregate") {
        Require(c.aggregate == "auto" || (c.aggregate == "hash" && dynamic_cast<HashAggregate*>(&root)) ||
                (c.aggregate == "sort" && dynamic_cast<SortAggregate*>(&root)), "forced aggregate executor mismatch");
    }
    if (c.family == "sort") Require(dynamic_cast<ExternalSort*>(&root), "sort was elided; use --scan seq");
}

void SortMemory(Json& j, const ExternalSortMemory& m) {
    j.Number("M", m.max_bytes); j.Number("Tinput", m.row_bytes); j.Number("S", m.scratch_bytes);
    j.Number("D", m.descriptor_capacity); j.Number("R", m.descriptor_bytes);
    j.Number("C_sort", m.run_capacity); j.Number("k", m.merge_fan_in);
}
void SortMetrics(Json& j, const ExternalSortMetrics& m) {
#define SM(field) j.Number(#field, m.field)
    SM(initial_runs); SM(materialized_runs); SM(intermediate_merge_passes); SM(final_merge_fan_in);
    SM(peak_retained_rows); SM(peak_run_descriptors); SM(peak_merge_heads); SM(modeled_peak_bytes);
    SM(pending_boundary_tuples);
#undef SM
    j.Bool("final_merge_streamed", m.final_merge_streamed);
}
Json Algorithm(Executor& root, const Schema& input, const QueryResources& resources, uint64_t count) {
    Json j;
    if (auto* e = dynamic_cast<ExternalSort*>(&root)) {
        SortMemory(j, e->Memory()); SortMetrics(j, e->Metrics());
    } else if (auto* e = dynamic_cast<NestedLoopJoin*>(&root)) {
        const auto& m = e->Metrics();
        j.Number("outer_rows", m.outer_rows); j.Number("inner_scans", m.inner_scans);
        j.Number("candidate_comparisons", m.candidate_comparisons); j.Number("output_rows", m.output_rows);
        Require(m.output_rows == count && m.inner_scans == m.outer_rows, "NLJ counter mismatch");
    } else if (auto* e = dynamic_cast<BlockNestedLoopJoin*>(&root)) {
        const auto& m = e->Metrics(); const auto& b = e->Memory();
        j.Number("M", b.max_bytes); j.Number("Touter", b.outer_row_bytes);
        j.Number("S", b.scratch_bytes); j.Number("C_block", b.block_capacity);
#define BM(field) j.Number(#field, m.field)
        BM(outer_rows); BM(outer_blocks); BM(inner_scans); BM(candidate_comparisons); BM(output_rows);
        BM(peak_block_rows); BM(modeled_peak_bytes); BM(pending_boundary_tuples);
#undef BM
        Require(m.inner_scans == m.outer_blocks && m.output_rows == count, "BNLJ counter mismatch");
    } else if (auto* e = dynamic_cast<GraceHashJoin*>(&root)) {
        const auto& m = e->Metrics(); const auto& b = e->Memory();
        j.Number("M", b.max_bytes); j.Number("Tleft", b.left_row_bytes); j.Number("Tright", b.right_row_bytes);
        j.Number("S", b.scratch_bytes); j.Number("p", b.partition_count); j.Number("Dghj", b.descriptor_bytes);
        j.Number("left_build_capacity", b.left_build_capacity); j.Number("right_build_capacity", b.right_build_capacity);
        j.Number("fallback_M", b.fallback_memory); j.Number("left_block_capacity", b.left_block_capacity);
        j.Number("right_block_capacity", b.right_block_capacity);
#define GM(field) j.Number(#field, m.field)
        GM(hash_pairs); GM(fallback_pairs); GM(empty_pairs); GM(left_build_pairs); GM(right_build_pairs);
        GM(peak_build_rows); GM(modeled_peak_build_bytes); GM(output_rows); GM(peak_partition_writers);
        GM(left_fallback_pairs); GM(right_fallback_pairs); GM(completed_fallback_inner_scans);
#undef GM
        Require(m.output_rows == count, "GHJ counter mismatch");
    } else {
        const AggregateMemory* b = nullptr; const AggregateMetrics* m = nullptr;
        bool hash = false;
        if (auto* a = dynamic_cast<HashAggregate*>(&root)) { b = &a->Memory(); m = &a->Metrics(); hash = true; }
        if (auto* a = dynamic_cast<SortAggregate*>(&root)) { b = &a->Memory(); m = &a->Metrics(); }
        if (b) {
            j.Number("M", b->max_bytes); j.Number("Tinput", RowMemoryCharge(input));
            j.Number("Tmax", b->maximum_row_bytes); j.Number("S", b->scratch_bytes);
            j.Number("combine_bytes", b->combine_bytes); j.Number("group_capacity", b->hash_group_capacity);
            j.Number("internal_sort_M", b->sort_memory_bytes);
#define AM(field) j.Number(#field, m->field)
            AM(input_rows); AM(peak_hash_groups); AM(modeled_peak_hash_bytes); AM(overflow_flushes);
            AM(spill_batches); AM(spill_records); AM(output_groups);
#undef AM
            j.Bool("used_internal_sort", m->used_internal_sort); j.Bool("input_order_reused", m->input_order_reused);
            Json sort; SortMetrics(sort, m->sort);
            if (m->used_internal_sort)
                SortMemory(sort, ComputeSortMemory(resources, hash ? AggregatePartialSchema() : input, b->sort_memory_bytes));
            j.Object("sort", sort);
            Require(m->output_groups == count, "aggregate counter mismatch");
            Require(!m->spill_batches || m->spill_batches == m->overflow_flushes + 1, "final spill flush missing");
        }
    }
    return j;
}

void ColdPool(Database& db) {
    // Reopen populates metadata pages. Invalidate these clean pages as well.
    for (const auto& frame : db.Buffer().InspectFrames())
        if (frame.page) db.Buffer().Invalidate(*frame.page);
}
void Counters(Json& record, Database& db, const BufferMetrics& before, const IoCounters& io_before) {
    const auto after = db.Metrics(); const auto io = db.Disk().counters();
    Json b, f;
#define BD(field) b.Number(#field, after.field - before.field)
    BD(buffer_fetches); BD(buffer_hits); BD(buffer_misses); BD(new_pages); BD(evictions);
    BD(dirty_evictions); BD(explicit_flush_writes); BD(dirty_eviction_writes);
    BD(sweep_inspections); BD(sweep_decrements); BD(ring_reuses); BD(ring_fallbacks);
#undef BD
    b.Number("active_pins", after.active_pins); b.Number("peak_pins", after.peak_pins);
    b.Number("peak_pins_at_start", before.peak_pins);
    b.String("peak_pins_scope", "reopened BufferPool lifetime, including metadata preload");
#define ID(field) f.Number(#field, io.field - io_before.field)
    ID(database_reads); ID(database_writes); ID(database_allocations);
    ID(temporary_reads); ID(temporary_writes); ID(temporary_allocations);
#undef ID
    f.Number("database_write_operations_including_initialization",
        io.database_writes - io_before.database_writes + io.database_allocations - io_before.database_allocations);
    f.Number("temporary_write_operations_including_initialization",
        io.temporary_writes - io_before.temporary_writes + io.temporary_allocations - io_before.temporary_allocations);
    Require(after.buffer_hits + after.buffer_misses == after.buffer_fetches, "buffer counter mismatch");
    Require(after.active_pins == 0, "pins remain at benchmark boundary");
    record.Object("buffer", b); record.Object("file_io", f);
}
void CheckReset(Database& db, QueryContext& context) {
    Require(context.Succeeded() && db.Disk().PageCount(FileKind::Temporary) == 0, "scratch reset failed");
    for (const auto& frame : db.Buffer().InspectFrames()) {
        Require(frame.pin_count == 0, "pin remains after FinishQuery");
        Require(!frame.page || frame.page->file != FileKind::Temporary, "stale temporary cache mapping");
    }
}
struct Workspace {
    fs::path path;
    explicit Workspace(const Config& c) {
        path = c.work_dir.empty() ? fs::temp_directory_path() /
            ("valencedb_bench_" + std::to_string(Clock::now().time_since_epoch().count())) : fs::path(c.work_dir);
        Require(fs::create_directory(path), "work directory must be new: " + path.string());
    }
    ~Workspace() { std::error_code error; fs::remove_all(path, error); }
};

std::vector<QuerySpec> Queries(const Config& c, const std::vector<Tuple>& left) {
    QuerySpec q; q.left.table = "left";
    if (c.family == "join") { q.right = BaseAccess{"right", {}, {}}; q.join = EquiJoinSpec{0, 0}; }
    if (c.family == "sort") q.order_column = 0;
    if (c.family == "aggregate") {
        q.aggregate = c.count_star ? AggregateSpec{0, std::nullopt, true} :
            AggregateSpec{0, 1, true, true, true, true, true};
    }
    if (c.family == "btree_point") {
        Require(!left.empty() && c.lookups > 0, "point lookup needs rows and lookups");
        std::vector<QuerySpec> queries;
        for (size_t i = 0; i < c.lookups; ++i) {
            auto one = q;
            // Dataset keys are nonnegative. Alternate present and absent keys.
            int64_t key = i % 2 ? -1 - static_cast<int64_t>(i) :
                left[(c.seed + i * 17) % left.size()].value(0).as_int64();
            one.left.predicates = {{0, CompareOp::Equal, key}};
            queries.push_back(std::move(one));
        }
        return queries;
    }
    if (c.family == "btree_range") {
        Require(!left.empty(), "range lookup needs rows");
        int64_t max_key = 0;
        for (const auto& row : left) max_key = std::max(max_key, row.value(0).as_int64());
        int64_t high = max_key;
        if (c.range == "short") high = max_key / 100;
        else if (c.range == "medium") high = max_key / 4;
        else Require(c.range == "long", "invalid range");
        q.left.predicates = {{0, CompareOp::GreaterEqual, 0}, {0, CompareOp::LessEqual, high}};
    }
    return {q};
}
Bag Reference(const Config& c, const QuerySpec& q, const std::vector<Tuple>& left,
              const std::vector<Tuple>& right) {
    Bag bag; size_t count = 0;
    if (c.family == "join") {
        for (const auto& a : left) for (const auto& b : right)
            if (a.value(0).as_int64() == b.value(0).as_int64()) AddReference(bag, Joined(a, b), count);
    } else if (c.family == "aggregate") {
        // Independent arithmetic: deliberately do not use AggregateState helpers.
        struct State { int64_t count = 0, sum = 0, min = INT64_MAX, max = INT64_MIN; };
        std::map<int64_t, State> groups;
        for (const auto& row : left) {
            auto& s = groups[row.value(0).as_int64()]; ++s.count;
            if (!c.count_star) {
                const auto n = row.value(1).as_int64(); s.sum += n;
                s.min = std::min(s.min, n); s.max = std::max(s.max, n);
            }
        }
        for (const auto& [key, s] : groups) {
            std::vector<Value> values{Value(key), Value(s.count)};
            if (!c.count_star) {
                values.emplace_back(s.sum); values.emplace_back(s.min); values.emplace_back(s.max);
                values.emplace_back(static_cast<double>(s.sum) / static_cast<double>(s.count));
            }
            AddReference(bag, Tuple(std::move(values)), count);
        }
    } else {
        for (const auto& row : left) {
            bool match = true;
            for (const auto& p : q.left.predicates) {
                const auto n = row.value(p.column).as_int64();
                // Independent predicate evaluation, not Int64Predicate::Matches.
                if (p.op == CompareOp::Equal) match &= n == p.constant;
                else if (p.op == CompareOp::GreaterEqual) match &= n >= p.constant;
                else if (p.op == CompareOp::LessEqual) match &= n <= p.constant;
                else throw std::runtime_error("unexpected reference predicate");
            }
            if (match) AddReference(bag, row, count);
        }
    }
    return bag;
}
void Consume(Executor& root, Digest& sink, Bag* expected = nullptr) {
    const auto ordering = root.OutputOrdering();
    std::optional<int64_t> previous;
    Tuple row({});
    while (root.Next(row)) {
        if (ordering.ordered) {
            auto key = row.value(ordering.column).as_int64();
            Require(!previous || key >= *previous, "result order decreased"); previous = key;
        }
        sink.Add(row);
        if (expected) {
            auto found = expected->find(Canonical(row));
            Require(found != expected->end() && found->second > 0, "row absent from canonical reference");
            if (--found->second == 0) expected->erase(found);
        }
    }
    if (expected) Require(expected->empty(), "rows missing from canonical reference");
}

void RunQueries(const Config& c, Database& db, const std::vector<QuerySpec>& queries,
                const Schema& schema, Json& record, Digest& sink, std::string& stage) {
    const QueryResources resources{c.memory, c.temp_limit}; const auto options = Options(c);
    const auto before = db.Metrics(); const auto io_before = db.Disk().counters();
    double elapsed = 0;
    size_t allocated = 0, relations = 0;
    std::string plans = "[", explains;
    for (size_t i = 0; i < queries.size(); ++i) {
        // Includes context startup, physical planning/construction, Init, all Next,
        // bounded digest/order checks, and checked FinishQuery. JSON stays outside.
        auto begin = Clock::now(); stage = "query_context";
        QueryContext context(db, resources);
        stage = "planning"; auto plan = ChoosePlan(context, queries[i], options);
        stage = "init"; plan.root->Init();
        const auto previous_count = sink.count;
        stage = "next"; Consume(*plan.root, sink);
        const auto temp = context.Temps().Metrics(); // Must copy before reset.
        stage = "finish_query"; context.FinishQuery(*plan.root);
        elapsed += std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
        stage = "metrics_validation"; CheckReset(db, context); CheckRoot(c, *plan.root);
        auto physical = PlanRecord(plan.plan, c);
        if (plans.size() > 1) plans += ',';
        plans += physical.Dump();
        explains += "query " + std::to_string(i) + "\n" + Explain(plan) + "\n";
        record.Object("algorithm", Algorithm(*plan.root, schema, resources, sink.count - previous_count));
        allocated += temp.allocated_temp_pages; relations += temp.temp_relations_created;
    }
    record.Real("elapsed_ms", elapsed); record.fields["physical_plans"] = plans + ']';
    record.String("explain", explains); record.Number("query_count", queries.size());
    record.String("timing_boundary", "sum(context + planning/construction + Init + Next through EOF + digest/order + FinishQuery)");
    record.Bool("finish_query_succeeded", true); record.Bool("order_verified", true);
    Json temp;
    temp.Number("allocated_temp_pages", allocated); temp.Number("extent_after_reset", 0);
    temp.Number("temp_relations_created", relations); temp.Number("temporary_pins_after", 0);
    temp.Number("temporary_cached_pages_after", 0); record.Object("scratch", temp);
    Counters(record, db, before, io_before);
}
void ValidateReference(const Config& c, const Workspace& workspace, const std::vector<QuerySpec>& queries,
                       const std::vector<Tuple>& left, const std::vector<Tuple>& right, const Digest& measured) {
    Database db(workspace.path / "database", OpenMode::OpenExisting, workspace.path / "scratch", c.frames);
    Digest validation;
    for (const auto& q : queries) {
        auto expected = Reference(c, q, left, right);
        QueryContext context(db, {c.memory, c.temp_limit});
        auto plan = ChoosePlan(context, q, Options(c));
        plan.root->Init(); Consume(*plan.root, validation, &expected);
        context.FinishQuery(*plan.root); CheckReset(db, context);
    }
    Require(validation.Same(measured), "measured digest differs from reference-validated execution");
    db.Close();
}

void Metadata(Database& db, Json& record) {
    Json dataset;
    for (const auto& name : db.TableNames()) {
        auto table = db.Table(name); Json info;
        info.Number("rows", table.live_rows); info.Number("heap_pages", table.heap_pages);
        info.Number("schema_columns", table.schema.size());
        info.Number("max_encoded_row_bytes", table.schema.max_encoded_bytes());
        info.Number("T", RowMemoryCharge(table.schema));
        info.Bool("indexed", table.index_meta != kInvalidPageId);
        if (table.index_meta != kInvalidPageId) info.Number("tree_height", db.Index(name).Height());
        dataset.Object(name, info);
    }
    dataset.Number("database_pages", db.Disk().PageCount(FileKind::Database));
    record.Object("dataset", dataset);
}
void NoScratch(Json& record) {
    Json s;
    for (const auto& name : {"allocated_temp_pages",
             "extent_after_reset", "temporary_pins_after", "temporary_cached_pages_after"}) s.Number(name, 0);
    record.Object("scratch", s);
    record.fields["finish_query_succeeded"] = "null";
    record.fields["physical_plans"] = "[]";
}
void RunClock(const Config& c, Database& db, const std::vector<PageId>& pages,
              Json& record, Digest& sink) {
    Require(c.hot_pages > 0 && c.hot_pages < c.frames && c.scan_batch > 0, "invalid clock hot set/batch");
    Require(pages.size() > c.hot_pages + 8 * c.frames, "scan must exceed eight BufferPools");
    bool use_ring = c.ring == "on" || (c.ring == "auto" && pages.size() > c.frames / 4);
    Require(c.ring == "on" || c.ring == "off" || c.ring == "auto", "invalid ring");
    ScanRing ring(db.Buffer());
    // Deliberately prewarm only the hot set, with identical count-five usage.
    for (size_t repeat = 0; repeat < 5; ++repeat)
        for (size_t i = 0; i < c.hot_pages; ++i) { auto guard = db.Buffer().Fetch({FileKind::Database, pages[i]}); }
    const auto before = db.Metrics(); const auto io_before = db.Disk().counters();
    uint64_t hot_fetches = 0, hot_hits = 0, hot_misses = 0, scan_fetches = 0;
    auto access = [&](PageId id, bool hot) {
        const auto prior = db.Metrics();
        auto guard = db.Buffer().Fetch({FileKind::Database, id}, hot ? AccessMode::Normal : AccessMode::Scan,
                                      hot || !use_ring ? nullptr : &ring);
        const auto stored_id = ReadU32(guard.Data(), 8);
        Require(stored_id == id, "clock schedule fetched wrong page contents");
        if (hot) {
            ++hot_fetches; hot_hits += db.Metrics().buffer_hits - prior.buffer_hits;
            hot_misses += db.Metrics().buffer_misses - prior.buffer_misses;
        } else ++scan_fetches;
        sink.Add(Tuple({Value(static_cast<int64_t>(id)), Value(static_cast<int64_t>(stored_id))}));
    };
    auto begin = Clock::now();
    for (size_t start = c.hot_pages; start < pages.size(); start += c.scan_batch) {
        for (size_t i = 0; i < c.hot_pages; ++i) access(pages[i], true);
        if (!c.control)
            for (size_t i = start; i < std::min(pages.size(), start + c.scan_batch); ++i) access(pages[i], false);
    }
    for (size_t i = 0; i < c.hot_pages; ++i) access(pages[i], true);
    record.Real("elapsed_ms", std::chrono::duration<double, std::milli>(Clock::now() - begin).count());
    Json a;
    a.Number("hot_fetches", hot_fetches); a.Number("hot_hits", hot_hits); a.Number("hot_misses", hot_misses);
    a.Number("scan_fetches", scan_fetches); a.Number("ring_size", ring.size()); a.Bool("ring_enabled", use_ring);
    record.Object("algorithm", a); NoScratch(record); Counters(record, db, before, io_before);
    record.String("buffer_state", "reopened/emptied pool, then hot pages warmed five times; scan pages cold BufferPool");
    record.String("timing_boundary", "hot/scan page access schedule plus bounded digest; hot preload excluded");
    record.String("explain", "BufferPool page schedule: hot set, scan batch, repeat, final hot set; real default ScanRing");
    record.String("reference_validation", "every fetched page header id checked against exact schedule");
}
void RunInsert(const Config& c, Database& db, const std::vector<Tuple>& rows,
               Json& record, Digest& sink) {
    const auto before = db.Metrics(); const auto io_before = db.Disk().counters();
    const auto begin = Clock::now();
    for (const auto& row : rows) { db.Insert("left", row); sink.Add(row); }
    db.Flush(); // Explicitly timed; no setup dirtiness survives initial close/reopen.
    record.Real("elapsed_ms", std::chrono::duration<double, std::milli>(Clock::now() - begin).count());
    Counters(record, db, before, io_before); NoScratch(record);
    Json a; a.Number("inserted_rows", rows.size()); a.Number("tree_height", db.Index("left").Height());
    a.String("split_counter", "not instrumented"); record.Object("algorithm", a);
    record.String("timing_boundary", "Database::Insert including index maintenance + digest + persistent Flush");
    record.String("explain", "Random key order Database::Insert into empty indexed heap; index creation excluded");
    Metadata(db, record);
    // Full real IndexScan validation, outside the insertion interval.
    QueryContext context(db, {c.memory, c.temp_limit});
    IndexScan scan(context, IndexScanPlan{"left", 0});
    scan.Init(); Digest actual; Consume(scan, actual); context.FinishQuery(scan); CheckReset(db, context);
    Require(actual.Same(sink), "inserted index contents differ from input");
    record.Bool("validation_finish_query_succeeded", true);
}

void Run(const Config& c, Json& record, std::string& stage) {
    if (c.family == "join" && c.distribution == "all_equal") {
        auto expected = CheckedMultiply(c.rows, c.right_rows);
        Require(expected <= 10000000, "all-equal output exceeds harness safety limit of ten million rows");
        record.Number("expected_cartesian_rows", expected);
    }
    Workspace workspace(c);
    const bool aggregate = c.family == "aggregate";
    const auto schema = DatasetSchema(c.payload, c.count_star);
    DatasetConfig generation{c.rows, aggregate ? c.groups : c.domain, c.payload, c.seed, c.distribution, c.measures};
    stage = "dataset_generation";
    const auto left = Dataset(generation, c.count_star);
    generation.rows = c.right_rows; generation.seed = c.seed ^ 0x9e3779b97f4a7c15ULL;
    const auto right = c.family == "join" ? Dataset(generation, c.count_star) : std::vector<Tuple>{};
    std::vector<PageId> pages;
    stage = "setup";
    {
        Database db(workspace.path / "database", OpenMode::CreateNew, workspace.path / "scratch", c.frames);
        db.CreateTable("left", schema);
        if (c.family != "btree_insert")
            for (const auto& row : left) {
                const auto rid = db.Insert("left", row);
                if (pages.empty() || pages.back() != rid.page_id) pages.push_back(rid.page_id);
            }
        // Equal index state across scan/aggregate alternatives, even SeqScan.
        if (c.family.starts_with("btree") || aggregate || c.scan == "index") db.CreateIndex("left", 0);
        if (c.family == "join") {
            db.CreateTable("right", schema);
            for (const auto& row : right) db.Insert("right", row);
            if (c.right_scan == "index") db.CreateIndex("right", 0);
        }
        db.Flush(); db.Close();
    }
    Digest sink;
    auto queries = c.family == "clock" ? std::vector<QuerySpec>{} : Queries(c, left);
    {
        Database db(workspace.path / "database", OpenMode::OpenExisting, workspace.path / "scratch", c.frames);
        Metadata(db, record); ColdPool(db);
        record.String("buffer_state", "cold BufferPool after reopen and invalidation of clean metadata; preserved within point batch");
        if (c.family == "clock") { stage = "clock_schedule"; RunClock(c, db, pages, record, sink); }
        else if (c.family == "btree_insert") { stage = "insert_and_flush"; RunInsert(c, db, left, record, sink); }
        else RunQueries(c, db, queries, schema, record, sink, stage);
        stage = "database_close"; db.Close();
    }
    if (c.family == "sort" || c.family.starts_with("btree")) {
        Digest expected;
        for (const auto& q : queries) for (const auto& row : left) {
            bool match = true;
            for (const auto& predicate : q.left.predicates) {
                const auto key = row.value(0).as_int64();
                if (predicate.op == CompareOp::Equal) match &= key == predicate.constant;
                else if (predicate.op == CompareOp::GreaterEqual) match &= key >= predicate.constant;
                else if (predicate.op == CompareOp::LessEqual) match &= key <= predicate.constant;
                else throw std::runtime_error("unsupported diagnostic reference predicate");
            }
            if (match) expected.Add(row);
        }
        stage = "input_digest_validation";
        Require(expected.Same(sink), "output digest differs from independently filtered generated input");
        record.Object("input_reference_digest", expected.Record());
    }
    record.Object("diagnostic_digest", sink.Record()); record.Number("result_rows", sink.count);
    record.String("ordered_digest", Hex(sink.ordered));
    record.String("digest_kind", "diagnostic result digest, not cryptographic proof; FNV-1a + xor/sum/mixed sum");
    if (c.reference && c.family != "clock") {
        stage = "reference_validation";
        ValidateReference(c, workspace, queries, left, right, sink);
        record.String("reference_validation", "exact canonical reference bag in separate untimed query, linked by measured digest");
    } else if (c.family != "clock") record.String("reference_validation", "not requested; diagnostic equivalence only");
    if (c.family == "join" && c.distribution == "all_equal") {
        const auto expected = CheckedMultiply(c.rows, c.right_rows);
        Require(sink.count == expected, "all-equal Cartesian cardinality mismatch");
        record.Number("expected_cartesian_rows", expected);
    }
    record.Bool("success", true); record.String("failure_stage", ""); record.String("failure_reason", "");
}
} // namespace

int main(int argc, char** argv) {
    Config c; Json record; std::string stage = "cli";
    record.Number("schema_version", 5); record.Bool("success", false);
    record.String("build_type", VALENCEDB_BENCH_BUILD_TYPE);
    record.String("compiler", VALENCEDB_BENCH_COMPILER);
    record.String("platform", VALENCEDB_BENCH_PLATFORM);
    try {
        Parse(c, argc, argv); record.Object("config", Configuration(c));
        Run(c, record, stage);
        std::cout << record.Dump() << '\n'; return 0;
    } catch (const std::exception& error) {
        record.Object("config", Configuration(c)); record.Bool("success", false);
        record.String("failure_stage", stage); record.String("failure_reason", error.what());
        std::cout << record.Dump() << '\n'; return 1;
    }
}
