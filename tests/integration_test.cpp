#include "test_harness.h"
#include "bplus_validator.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/optimizer/optimizer.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <numeric>
#include <random>

using namespace valencedb;
namespace {
struct Files {
    std::filesystem::path dir, db, temp;
    Files() {
        static size_t serial = 0;
        dir = std::filesystem::temp_directory_path() / ("valencedb_integration_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "_" + std::to_string(++serial));
        std::filesystem::create_directory(dir);
        db = dir / "pages.db";
        temp = dir / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(dir); }
};
void Throws(const std::function<void()>& run, const std::string& message) {
    bool caught = false;
    try { run(); } catch (const DbError& e) {
        caught = true;
        VDB_CHECK(std::string(e.what()).find(message) != std::string::npos);
    }
    VDB_CHECK(caught);
}
void Clean(QueryContext& q) {
    VDB_CHECK(q.Db().Metrics().active_pins == 0);
    VDB_CHECK(q.Db().Disk().PageCount(FileKind::Temporary) == 0);
    VDB_CHECK(q.Db().Disk().PageCount(FileKind::Temporary) == 0);
    for (const auto& f : q.Db().Buffer().InspectFrames())
        VDB_CHECK(!f.page || f.page->file != FileKind::Temporary);
}
using Encoded = std::vector<std::vector<std::byte>>;
Encoded Drain(Executor& root) {
    Encoded rows;
    Tuple out({});
    std::optional<int64_t> previous;
    const auto order = root.OutputOrdering();
    while (root.Next(out)) {
        VDB_CHECK(root.Context().Db().Metrics().active_pins == 0);
        if (order.ordered) {
            const auto key = out.value(order.column).as_int64();
            VDB_CHECK(!previous || *previous <= key);
            previous = key;
        }
        rows.push_back(EncodeTuple(root.OutputSchema(), out));
    }
    return rows;
}
Encoded Bag(Encoded rows) { std::sort(rows.begin(), rows.end()); return rows; }
Schema LeftSchema() {
    return Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"measure", TypeId::Int64},
        {"payload", TypeId::Varchar, 256}});
}
Schema RightSchema() {
    return Schema(SchemaKind::Base, {{"label", TypeId::Varchar, 48}, {"key", TypeId::Int64}});
}
struct SavedRow { RID rid; Tuple row; bool live = true; };
using SavedRows = std::vector<SavedRow>;
void CheckStored(Database& db, const std::string& name, const SavedRows& rows, size_t key) {
    const auto schema = db.Table(name).schema;
    bplus_test::Oracle expected;
    Encoded heap_expected;
    for (const auto& r : rows) {
        auto fetched = db.Fetch(name, r.rid);
        VDB_CHECK(bool(fetched) == r.live);
        if (!r.live) continue;
        VDB_CHECK(EncodeTuple(schema, *fetched) == EncodeTuple(schema, r.row));
        expected.insert({r.row.value(key).as_int64(), r.rid});
        heap_expected.push_back(EncodeTuple(schema, r.row));
    }
    auto shape = bplus_test::Validator(db.Buffer(), db.Disk().PageCount(FileKind::Database), db.Index(name)).Run();
    VDB_CHECK(shape.entries == std::vector<BPlusKey>(expected.begin(), expected.end()));
    VDB_CHECK(db.Table(name).live_rows == expected.size());
    Encoded actual;
    auto scan = db.Scan(name);
    while (auto row = scan.Next()) actual.push_back(EncodeTuple(schema, row->second));
    VDB_CHECK(Bag(actual) == Bag(heap_expected));
}
QuerySpec Pipeline() {
    QuerySpec s;
    s.left = {"l", {{1, CompareOp::GreaterEqual, 0}}, ProjectionPlan{{1, 0, 2}}};
    s.right = BaseAccess{"r", {}, ProjectionPlan{{1}}};
    s.join = EquiJoinSpec{1, 0};
    s.aggregate = AggregateSpec{1, 0, true, true, false, false, true};
    s.order_column = 0;
    s.projection = ProjectionPlan{{2, 0, 3, 1}}; // sum, group, avg, count
    return s;
}
Encoded Reference(const SavedRows& left, const SavedRows& right, const Schema& output) {
    std::map<int64_t, std::pair<int64_t, int64_t>> groups;
    for (const auto& l : left) if (l.live && l.row.value(1).as_int64() >= 0) {
        for (const auto& r : right) if (r.live && l.row.value(0).as_int64() == r.row.value(1).as_int64()) {
            auto& [sum, count] = groups[l.row.value(0).as_int64()];
            sum += l.row.value(1).as_int64();
            ++count;
        }
    }
    Encoded expected;
    for (auto [key, state] : groups) {
        auto [sum, count] = state;
        expected.push_back(EncodeTuple(output, Tuple({Value(sum), Value(key),
            Value(static_cast<double>(sum) / static_cast<double>(count)), Value(count)})));
    }
    return expected;
}
void PersistentPipeline() {
    Files f;
    SavedRows left, right;
    std::optional<TableInfo> left_meta, right_meta;
    PageId root = kInvalidPageId;
    uint16_t height = 0;
    constexpr unsigned seed = 120012;
    std::cout << "lifecycle seed=" << seed << '\n';
    {
        Database db(f.db, OpenMode::CreateNew, f.temp, 8);
        db.CreateTable("l", LeftSchema());
        db.CreateTable("r", RightSchema());
        std::vector<int64_t> ids(660);
        std::iota(ids.begin(), ids.end(), 0);
        std::mt19937 rng(seed);
        std::shuffle(ids.begin(), ids.end(), rng);
        for (int64_t i : ids) {
            std::string payload(i % 3 == 0 ? 0 : i % 3 == 1 ? 37 : 256, char('a' + i % 26));
            if (payload.size() > 10) payload[10] = '\0';
            Tuple row({Value(i % 300 - 150), Value(i % 91 - 7), Value(payload)});
            left.push_back({db.Insert("l", row), row});
        }
        for (int64_t i = 0; i < 450; ++i) {
            Tuple row({Value(std::string(i % 2 ? "right" : "")), Value(i % 300 - 150)});
            right.push_back({db.Insert("r", row), row});
        }
        // V1 permits heap deletion before the table is indexed.
        for (size_t i = 0; i < left.size(); i += 17) {
            VDB_CHECK(db.Delete("l", left[i].rid));
            VDB_CHECK(!db.Delete("l", left[i].rid));
            left[i].live = false;
        }
        for (size_t i = 0; i < right.size(); i += 29) {
            VDB_CHECK(db.Delete("r", right[i].rid));
            right[i].live = false;
        }
        db.CreateIndex("l", 0);
        db.CreateIndex("r", 1);
        // Index maintenance also runs after bulk index construction, with signed endpoints.
        for (int64_t key : {INT64_MIN, INT64_MAX}) {
            Tuple l({Value(key), Value(int64_t{5}), Value(std::string(256, 'z'))});
            Tuple r({Value(std::string("extreme")), Value(key)});
            left.push_back({db.Insert("l", l), l});
            right.push_back({db.Insert("r", r), r});
        }
        CheckStored(db, "l", left, 0);
        CheckStored(db, "r", right, 1);
        left_meta = db.Table("l"); right_meta = db.Table("r");
        root = db.Index("l").RootPage(); height = db.Index("l").Height();
        VDB_CHECK(height > 1);
        db.Flush(); db.Close();
    }
    Database db(f.db, OpenMode::OpenExisting, f.temp, 8);
    VDB_CHECK(db.TableNames() == std::vector<std::string>({"l", "r"}));
    VDB_CHECK(db.Table("l").meta_page == left_meta->meta_page);
    VDB_CHECK(db.Table("l").first_page == left_meta->first_page);
    VDB_CHECK(db.Table("l").last_page == left_meta->last_page);
    VDB_CHECK(db.Table("l").heap_pages == left_meta->heap_pages);
    VDB_CHECK(db.Table("r").index_meta == right_meta->index_meta);
    VDB_CHECK(db.Table("l").schema.column(2).varchar_limit == 256);
    VDB_CHECK(db.Index("l").RootPage() == root && db.Index("l").Height() == height);
    VDB_CHECK(!db.Statistics("l"));
    CheckStored(db, "l", left, 0); CheckStored(db, "r", right, 1);
    // Retain actual owned VARCHAR output while enough other pages evict the original frame.
    auto owned = db.Fetch("l", left.back().rid);
    const auto saved = EncodeTuple(LeftSchema(), *owned);
    const auto evictions = db.Metrics().evictions;
    for (PageId id = 0; id < db.Disk().PageCount(FileKind::Database); ++id)
        db.Buffer().Fetch({FileKind::Database, id});
    VDB_CHECK(db.Metrics().evictions > evictions);
    VDB_CHECK(EncodeTuple(LeftSchema(), *owned) == saved);
    QuerySpec scan;
    scan.left = {"l", {{0, CompareOp::GreaterEqual, -30}, {0, CompareOp::Less, 40}}, {}};
    scan.projection = ProjectionPlan{{2, 0, 2}};
    Encoded scan_reference;
    for (auto choice : {ScanChoice::SeqScan, ScanChoice::IndexScan}) {
        QueryContext q(db);
        PlanOptions o; o.left_scan = choice;
        auto p = ChoosePlan(q, scan, o);
        p.root->Init();
        auto result = Bag(Drain(*p.root));
        if (choice == ScanChoice::SeqScan) scan_reference = result;
        else VDB_CHECK(result == scan_reference);
        q.FinishQuery(*p.root); Clean(q);
    }
    {
        QueryContext q(db);
        IndexScan index(q, IndexScanPlan{"l", 0});
        auto fetches = db.Metrics().buffer_fetches;
        index.Init();
        const auto init_fetches = db.Metrics().buffer_fetches - fetches;
        index.Close();
        SortAggregate a(q, AggregateSpec{0, {}, true},
            std::make_unique<IndexScan>(q, IndexScanPlan{"l", 0}), 4096);
        fetches = db.Metrics().buffer_fetches;
        a.Init();
        // Only the index's initial descent; aggregation does not consume rows.
        VDB_CHECK(db.Metrics().buffer_fetches - fetches == init_fetches);
        std::map<int64_t, int64_t> counts;
        for (const auto& r : left) if (r.live) ++counts[r.row.value(0).as_int64()];
        Encoded expected;
        for (auto [key, count] : counts)
            expected.push_back(EncodeTuple(a.OutputSchema(), Tuple({Value(key), Value(count)})));
        VDB_CHECK(Drain(a) == expected);
        VDB_CHECK(a.Metrics().input_order_reused && !a.Metrics().used_internal_sort);
        VDB_CHECK(a.Metrics().sort.initial_runs == 0 && q.Temps().Metrics().allocated_temp_pages == 0);
        q.FinishQuery(a); Clean(q);
    }
    auto spec = Pipeline();
    const auto execute = [&](PlanOptions options) {
        QueryContext q(db, {65536, 4096});
        auto p = ChoosePlan(q, spec, options);
        VDB_CHECK(p.root->OutputSchema().size() == 4);
        VDB_CHECK(p.root->OutputSchema().column(0).name == "sum");
        VDB_CHECK(p.root->OutputSchema().column(2).type == TypeId::Double);
        VDB_CHECK((p.root->OutputOrdering() == Ordering{true, 1}));
        p.root->Init();
        VDB_CHECK(Drain(*p.root) == Reference(left, right, p.root->OutputSchema()));
        q.FinishQuery(*p.root); VDB_CHECK(q.Succeeded()); Clean(q);
    };
    execute({}); // Optimized query immediately after clean reopen, without analyzed stats.
    db.Analyze("l"); db.Analyze("r");
    // Deliberately stale NDV/cardinality: insert a previously absent key after Analyze.
    Tuple extra({Value(int64_t{900000}), Value(int64_t{42}), Value(std::string("later"))});
    Tuple partner({Value(std::string("later")), Value(int64_t{900000})});
    left.push_back({db.Insert("l", extra), extra});
    right.push_back({db.Insert("r", partner), partner});
    VDB_CHECK(db.Statistics("l")->stale && db.Statistics("r")->stale);
    execute({});
    db.Analyze("l"); db.Analyze("r");
    execute({});
    for (auto choice : {JoinChoice::NLJ, JoinChoice::BNLJ, JoinChoice::GHJ}) {
        PlanOptions o;
        o.join = choice; o.join_memory = 32768;
        o.aggregate = AggregateChoice::HashAggregate; o.aggregate_memory = 32768;
        o.sort_memory = 24576;
        o.left_scan = ScanChoice::IndexScan; o.right_scan = ScanChoice::SeqScan;
        if (choice != JoinChoice::GHJ) o.outer = PhysicalOuter::Right;
        execute(o);
        o.aggregate = AggregateChoice::SortAggregate; o.aggregate_memory = 65536;
        o.left_scan = ScanChoice::SeqScan; o.right_scan = ScanChoice::IndexScan;
        execute(o);
    }
    std::cout << "reopen pipeline: Auto/stale/fresh + NLJ/BNLJ/GHJ x Hash/Sort results and order match\n";
    db.Close();
}

struct Consumption { size_t inits = 0, rows = 0, calls = 0; };
// Observe actual scan children and reject replay; no replacement query algorithm in this wrapper.
class OnceScan final : public Executor {
public:
    OnceScan(QueryContext& q, const std::string& table, Consumption& counts)
        : Executor(q), scan_(q, SeqScanPlan{table, true}), counts_(counts) {}
    ~OnceScan() noexcept override { Close(); }
    void DoInit() override {
        VDB_CHECK(counts_.inits++ == 0);
        scan_.Init();
    }
    bool DoNext(Tuple& row) override {
        ++counts_.calls;
        if (!scan_.Next(row)) return false;
        ++counts_.rows;
        return true;
    }
    void DoClose() noexcept override { scan_.Close(); }
    const Schema& OutputSchema() const override { return scan_.OutputSchema(); }
    Ordering OutputOrdering() const override { return {}; }
private:
    SeqScan scan_;
    Consumption& counts_;
};
void PartitionedGroupPipeline() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    const auto left = Schema(SchemaKind::Base, {{"group", TypeId::Int64}, {"join", TypeId::Int64}, {"value", TypeId::Int64}});
    const auto right = Schema(SchemaKind::Base, {{"join", TypeId::Int64}, {"copy", TypeId::Int64}});
    db.CreateTable("l", left); db.CreateTable("r", right);
    // Separate join partitions deliberately repeat every grouping key with uneven contributions.
    const int64_t copies[] = {1, 2, 5}, measures[] = {10, 100, -5};
    const auto partition_of = [](int64_t key) {
        uint64_t h = static_cast<uint64_t>(key) + UINT64_C(0x9e3779b97f4a7c15);
        h = (h ^ (h >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        h = (h ^ (h >> 27)) * UINT64_C(0x94d049bb133111eb);
        return (h ^ (h >> 31)) % 3;
    };
    for (size_t partition = 0; partition < 3; ++partition) {
        int64_t key = 0;
        while (partition_of(key) != partition) ++key;
        for (int64_t group = 0; group < 180; ++group)
            db.Insert("l", Tuple({Value(group - 90), Value(key), Value(group + measures[partition])}));
        for (int64_t copy = 0; copy < copies[partition]; ++copy)
            db.Insert("r", Tuple({Value(key), Value(copy)}));
    }
    // Full consumption and cancellation exercise the same composed tree.
    for (bool cancel : {false, true}) {
        QueryContext q(db, {65536, 4096});
        Consumption l, r;
        auto join = std::make_unique<GraceHashJoin>(q, GraceHashJoinPlan{1, 0, 24576},
            std::make_unique<OnceScan>(q, "l", l), std::make_unique<OnceScan>(q, "r", r));
        auto* gh = join.get();
        auto aggregate = std::make_unique<HashAggregate>(q, AggregateSpec{0, 2, true, true, true, true, true},
            std::move(join), 32768);
        auto* ha = aggregate.get();
        auto sort = std::make_unique<ExternalSort>(q, ExternalSortPlan{0, 24576}, std::move(aggregate));
        auto* es = sort.get();
        Projection root(q, ProjectionPlan{{5, 0, 1, 2}}, std::move(sort));
        root.Init();
        VDB_CHECK(gh->Memory().partition_count == 3 && gh->Metrics().hash_pairs == 3);
        VDB_CHECK(gh->Metrics().fallback_pairs == 0 && gh->Metrics().output_rows == 1440);
        VDB_CHECK(l.inits == 1 && r.inits == 1 && l.rows == 540 && r.rows == 8);
        VDB_CHECK(l.calls == 541 && r.calls == 9);
        VDB_CHECK(ha->Metrics().spill_records > 180);
        VDB_CHECK(ha->Metrics().spill_batches >= 3);
        VDB_CHECK(ha->Metrics().peak_hash_groups == ha->Memory().hash_group_capacity);
        VDB_CHECK(ha->Metrics().used_internal_sort && ha->Metrics().sort.final_merge_streamed);
        VDB_CHECK(es->Metrics().initial_runs > 1 && es->Metrics().final_merge_streamed);
        VDB_CHECK((root.OutputOrdering() == Ordering{true, 1}));
        if (cancel) {
            Tuple row({}); VDB_CHECK(root.Next(row));
            root.Close(); root.Close();
            Throws([&] { q.FinishQuery(root); }, "EOF");
            q.Close(); VDB_CHECK(!q.Succeeded());
        } else {
            Encoded expected;
            for (int64_t group = 0; group < 180; ++group) {
                const int64_t sum = 185 + 8 * group;
                expected.push_back(EncodeTuple(root.OutputSchema(), Tuple({Value(static_cast<double>(sum) / 8),
                    Value(group - 90), Value(int64_t{8}), Value(sum)})));
            }
            VDB_CHECK(Drain(root) == expected);
            q.FinishQuery(root); VDB_CHECK(q.Succeeded());
            std::cout << "partitioned groups: 1440 rows, " << ha->Metrics().spill_batches << " batches, "
                << ha->Metrics().spill_records << " partials, 180 groups, final runs=" << es->Metrics().initial_runs << '\n';
        }
        Clean(q);
    }
    db.Close();
}

void MetricLayers() {
    Files f;
    DiskManager disk(f.db, OpenMode::CreateNew, f.temp, OpenMode::CreateNew);
    BufferPool pool(disk, 1);
    auto a = pool.NewPage(FileKind::Database);
    const auto aid = a.Page();
    a.MutableData()[100] = std::byte{42}; a.MarkDirty(); a.Drop();
    VDB_CHECK(disk.counters().database_allocations == 1 && disk.counters().database_writes == 0);
    VDB_CHECK(pool.metrics().new_pages == 1 && pool.metrics().buffer_fetches == 0);
    pool.FlushAll(); pool.FlushAll();
    VDB_CHECK(pool.metrics().explicit_flush_writes == 1 && disk.counters().database_writes == 1);
    { auto hit = pool.Fetch(aid); hit.MarkDirty(); }
    auto b = pool.NewPage(FileKind::Temporary); const auto bid = b.Page();
    b.MarkDirty(); b.Drop();
    VDB_CHECK(aid.page_id == bid.page_id && aid != bid);
    VDB_CHECK(pool.metrics().dirty_evictions == 1 && pool.metrics().dirty_eviction_writes == 1);
    { auto fetched = pool.Fetch(aid); VDB_CHECK(fetched.Data()[100] == std::byte{42}); }
    VDB_CHECK(pool.metrics().dirty_eviction_writes == 2);
    const auto m = pool.metrics();
    const auto io = disk.counters();
    VDB_CHECK(m.buffer_fetches == 2 && m.buffer_hits == 1 && m.buffer_misses == 1);
    VDB_CHECK(io.database_reads == 1 && io.temporary_reads == 0);
    VDB_CHECK(io.database_writes + io.temporary_writes == m.explicit_flush_writes + m.dirty_eviction_writes);
    VDB_CHECK(m.active_pins == 0 && m.peak_pins == 1);
    disk.Close();
}

void TempMetricsAndReset() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    db.CreateTable("empty", LeftSchema());
    const auto schema = LeftSchema();
    QueryContext q(db, {32768, 2});
    TempWriter wa(q.Temps(), schema);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
    wa.Append(Tuple({Value(int64_t{1}), Value(int64_t{2}), Value(std::string())})); auto a = wa.Finish();
    const auto fetches = db.Metrics().buffer_fetches;
    { TempReader r(a); Tuple row({}); VDB_CHECK(r.Next(row)); VDB_CHECK(!r.Next(row)); }
    VDB_CHECK(db.Metrics().buffer_fetches == fetches + 1); // One staged read, including EOF.
    { auto copy = a; VDB_CHECK(copy.RowCount() == 1); }
    VDB_CHECK(db.Metrics().buffer_fetches == fetches + 1); // No release traversal.
    VDB_CHECK(q.Db().Disk().PageCount(FileKind::Temporary) == 1);
    TempWriter wb(q.Temps(), schema);
    wb.Append(Tuple({Value(int64_t{3}), Value(int64_t{4}), Value(std::string())})); auto b = wb.Finish();
    VDB_CHECK(b.FirstPage() == 1);
    { TempWriter w(q.Temps(), schema); Throws([&] { w.Append(Tuple({Value(int64_t{5}), Value(int64_t{6}), Value(std::string())})); }, "cap"); }
    SeqScan root(q, {"empty"}); root.Init(); Tuple row({}); VDB_CHECK(!root.Next(row));
    q.FinishQuery(root);
    VDB_CHECK(q.Succeeded());
    Clean(q);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 2);
    QueryContext next(db); VDB_CHECK(next.Temps().Metrics().allocated_temp_pages == 0);
    next.Close(); Clean(next); db.Close();
}

void PrefixFailure() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    const auto schema = Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"value", TypeId::Int64}});
    db.CreateTable("outer", schema); db.CreateTable("inner", schema);
    for (int64_t i = 0; i < 2; ++i) db.Insert("outer", Tuple({Value(int64_t{7}), Value(i)}));
    for (int64_t i = 0; i < 141; ++i) db.Insert("inner", Tuple({Value(int64_t{7}), Value(i)}));
    QueryContext q(db, {24576, 4});
    auto sort = std::make_unique<ExternalSort>(q, ExternalSortPlan{1, 24576}, std::make_unique<SeqScan>(q, SeqScanPlan{"inner"}));
    VDB_CHECK(sort->Memory().run_capacity == 70 && sort->Memory().merge_fan_in == 2);
    NestedLoopJoin join(q, {0, 0}, std::make_unique<SeqScan>(q, SeqScanPlan{"outer"}), std::move(sort));
    join.Init();
    size_t emitted = 0; Tuple row({});
    Throws([&] { while (join.Next(row)) ++emitted; }, "temporary page cap");
    VDB_CHECK(emitted == 141 && !join.Exhausted()); // First inner execution completed; replay cannot allocate.
    VDB_CHECK(!q.Succeeded() && q.Db().Disk().PageCount(FileKind::Temporary) == 4);
    join.Close(); join.Close();
    Throws([&] { q.FinishQuery(join); }, "EOF");
    q.Close(); Clean(q); VDB_CHECK(!q.Succeeded());
    QueryContext next(db);
    SeqScan scan(next, {"inner"}); scan.Init(); VDB_CHECK(Drain(scan).size() == 141);
    next.FinishQuery(scan); VDB_CHECK(next.Succeeded()); Clean(next); db.Close();
    std::cout << "temp cap after 141 emitted rows: prefix invalid, FinishQuery rejected, next query clean\n";
}
} // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> cases{
        {"persistent_pipeline", PersistentPipeline}, {"partitioned_group_pipeline", PartitionedGroupPipeline},
        {"metric_layers", MetricLayers},
        {"temp_metrics_reset", TempMetricsAndReset}, {"prefix_failure", PrefixFailure}};
    size_t passed = 0;
    try {
        for (const auto& [name, run] : cases) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            std::cout << "RUN " << name << std::endl;
            run(); ++passed;
            std::cout << "PASS " << name << std::endl;
        }
        VDB_CHECK(passed > 0);
        std::cout << "integration groups passed=" << passed << '\n';
    } catch (const std::exception& e) {
        std::cerr << "integration failure: " << e.what() << '\n';
        return 1;
    }
}
