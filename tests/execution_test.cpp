#include "test_harness.h"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <type_traits>

using namespace valencedb;
namespace {
size_t cases_passed = 0;
uint64_t maximum_stream_pins = 0;
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_execution_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string(++serial));
        std::filesystem::create_directory(directory);
        db = directory / "pages.db";
        temp = directory / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(directory); }
};
void Throws(const std::function<void()>& action) {
    bool caught = false;
    try { action(); } catch (const DbError&) { caught = true; }
    VDB_CHECK(caught);
}
void Pins(Database& db) {
    VDB_CHECK(db.Metrics().active_pins == 0);
    maximum_stream_pins = std::max(maximum_stream_pins, db.Metrics().peak_pins);
    VDB_CHECK(db.Metrics().peak_pins <= 1);
}
Schema Base() {
    return Schema(SchemaKind::Base, {{"id", TypeId::Int64},
        {"text", TypeId::Varchar, 256}, {"key", TypeId::Int64}});
}
int64_t Key(int64_t i) {
    if (i == 0 || i == 2) return INT64_MIN;
    if (i == 1 || i == 3) return INT64_MAX;
    return i % 17 - 8;
}
Tuple Row(int64_t i) {
    return Tuple({Value(i), Value(std::to_string(i) + std::string(220, char('a' + i % 26))), Value(Key(i))});
}
using EncodedRows = std::vector<std::vector<std::byte>>;
EncodedRows Drain(Database& db, Executor& scan) {
    EncodedRows rows;
    Tuple out({});
    while (scan.Next(out)) { rows.push_back(EncodeTuple(scan.OutputSchema(), out)); Pins(db); }
    VDB_CHECK(!scan.Next(out));
    Pins(db);
    return rows;
}
EncodedRows Drain(Database& db, TempReader& reader, const Schema& schema) {
    EncodedRows rows;
    Tuple out({});
    while (reader.Next(out)) { rows.push_back(EncodeTuple(schema, out)); Pins(db); }
    VDB_CHECK(!reader.Next(out));
    Pins(db);
    return rows;
}
void Evict(Database& db) {
    for (PageId id = 0; id < db.Disk().PageCount(FileKind::Database); ++id) {
        auto guard = db.Buffer().Fetch({FileKind::Database, id});
    }
    Pins(db);
}
void InvalidateDatabase(Database& db) {
    for (PageId id = 0; id < db.Disk().PageCount(FileKind::Database); ++id)
        db.Buffer().Invalidate({FileKind::Database, id});
}
void CleanScratch(Database& db, const Files& files) {
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 0);
    VDB_CHECK(std::filesystem::file_size(files.temp) == 0);
    for (const auto& frame : db.Buffer().InspectFrames())
        VDB_CHECK(!frame.page || frame.page->file != FileKind::Temporary);
    Pins(db);
}
std::vector<int64_t> Populate(Database& db, bool indexed = false) {
    db.CreateTable("rows", Base());
    std::vector<RID> rids;
    for (int64_t i = 0; i < 600; ++i) rids.push_back(db.Insert("rows", Row(i)));
    std::vector<int64_t> live;
    for (int64_t i = 0; i < 600; ++i) {
        if (i >= 4 && i % 13 == 0) VDB_CHECK(db.Delete("rows", rids[i]));
        else live.push_back(i);
    }
    if (indexed) db.CreateIndex("rows", 2);
    Pins(db);
    return live;
}
EncodedRows Expected(const std::vector<int64_t>& ids) {
    EncodedRows rows;
    for (int64_t id : ids) rows.push_back(EncodeTuple(Base(), Row(id)));
    return rows;
}
void FinishEmpty(QueryContext& query, const std::string& table = "empty") {
    SeqScan root(query, {table});
    root.Init();
    Tuple out({});
    VDB_CHECK(!root.Next(out));
    query.FinishQuery(root);
    VDB_CHECK(query.Succeeded());
}

void EmptyLifecycle() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("empty", Base());
    QueryContext query(db);
    SeqScan scan(query, {"empty"});
    Tuple out({});
    Throws([&] { scan.Next(out); });
    Throws([&] { query.FinishQuery(scan); });
    scan.Close(); scan.Close();
    Throws([&] { scan.Next(out); });
    scan.Init();
    Throws([&] { scan.Init(); });
    Throws([&] { query.Close(); });
    VDB_CHECK(!scan.Next(out));
    VDB_CHECK(!scan.Next(out));
    VDB_CHECK(scan.OutputSchema().size() == 3);
    VDB_CHECK(!scan.OutputOrdering().ordered);
    scan.Close(); scan.Close();
    scan.Init();
    VDB_CHECK(!scan.Next(out));
    query.FinishQuery(scan);
    VDB_CHECK(query.Succeeded());
    Throws([&] { scan.Init(); });
    Throws([&] { scan.Next(out); });
    CleanScratch(db, files);
    db.Close();
}

void SeqScans() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const auto live = Populate(db);
    VDB_CHECK(db.Table("rows").heap_pages > 8);
    const auto expected = Expected(live);
    for (bool ring : {false, true}) {
        InvalidateDatabase(db);
        const auto reuses = db.Metrics().ring_reuses;
        const auto evictions = db.Metrics().evictions;
        QueryContext query(db);
        SeqScan scan(query, {"rows", ring});
        scan.Init();
        Tuple first({});
        VDB_CHECK(scan.Next(first));
        const auto saved = EncodeTuple(scan.OutputSchema(), first);
        Pins(db);
        scan.Close(); scan.Close();
        Throws([&] { query.FinishQuery(scan); }); // Early Close is cancellation, not EOF.
        scan.Init();
        VDB_CHECK(Drain(db, scan) == expected);
        Evict(db);
        VDB_CHECK(EncodeTuple(scan.OutputSchema(), first) == saved);
        scan.Close(); scan.Init();
        VDB_CHECK(Drain(db, scan) == expected);
        VDB_CHECK(db.Metrics().evictions > evictions);
        if (ring) VDB_CHECK(db.Metrics().ring_reuses > reuses);
        else VDB_CHECK(db.Metrics().ring_reuses == reuses);
        query.FinishQuery(scan);
        CleanScratch(db, files);
    }
    db.Close();
}

void IndexScans() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    auto live = Populate(db, true);
    std::sort(live.begin(), live.end(), [](int64_t a, int64_t b) {
        return Key(a) < Key(b) || (Key(a) == Key(b) && a < b);
    });
    db.CreateTable("empty", Base()); db.CreateIndex("empty", 2);
    db.CreateTable("noindex", Base());
    QueryContext query(db);
    Throws([&] { IndexScan scan(query, {"missing", 2}); });
    Throws([&] { IndexScan scan(query, {"noindex", 2}); });
    Throws([&] { IndexScan scan(query, {"rows", 0}); });
    Throws([&] { IndexScan scan(query, {"rows", 1}); });
    Throws([&] { IndexScan scan(query, {"rows", 3}); });
    const std::vector<IndexScanPlan> plans = {
        {"rows", 2}, {"rows", 2, 0, 0}, {"rows", 2, -3, 4},
        {"rows", 2, INT64_MIN, INT64_MIN}, {"rows", 2, INT64_MAX, INT64_MAX},
        {"rows", 2, 50, 50}, {"rows", 2, 4, -3}, {"rows", 2, 0},
        {"rows", 2, std::nullopt, 0}, {"empty", 2}};
    for (const auto& plan : plans) {
        IndexScan scan(query, plan);
        VDB_CHECK((scan.OutputOrdering() == Ordering{true, 2}));
        std::vector<int64_t> wanted;
        for (auto id : live)
            if (plan.table == "rows" && Key(id) >= plan.low.value_or(INT64_MIN) &&
                Key(id) <= plan.high.value_or(INT64_MAX)) wanted.push_back(id);
        Tuple out({});
        Throws([&] { scan.Next(out); });
        scan.Init();
        Throws([&] { scan.Init(); });
        VDB_CHECK(Drain(db, scan) == Expected(wanted));
        scan.Close(); scan.Init();
        if (!wanted.empty()) {
            VDB_CHECK(scan.Next(out));
            const auto saved = EncodeTuple(scan.OutputSchema(), out);
            Tuple next({}); scan.Next(next);
            Evict(db);
            VDB_CHECK(EncodeTuple(scan.OutputSchema(), out) == saved);
        }
        scan.Close(); scan.Close();
        Throws([&] { scan.Next(out); });
        scan.Init();
        VDB_CHECK(Drain(db, scan) == Expected(wanted));
        scan.Close();
    }
    FinishEmpty(query);
    // Tree cursor -> heap fetch peak of one proves the two page pins never overlap.
    Pins(db);
    db.Close();
}

void IndexCorruption() {
    for (int corruption = 0; corruption < 3; ++corruption) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        db.CreateTable("rows", Base()); db.CreateTable("foreign", Base());
        const RID rid = db.Insert("rows", Row(5));
        const RID foreign = db.Insert("foreign", Row(5));
        db.CreateIndex("rows", 2);
        if (corruption == 0) {
            auto guard = db.Buffer().Fetch({FileKind::Database, rid.page_id});
            VDB_CHECK(SlottedPage::Erase(guard.MutableData(), rid.slot_id));
            guard.MarkDirty();
        } else {
            auto guard = db.Buffer().Fetch({FileKind::Database, db.Index("rows").RootPage()});
            BPlusLeafPage::WriteEntry(guard.MutableData(), 0,
                {corruption == 1 ? Key(5) : int64_t{999}, corruption == 1 ? foreign : rid});
            guard.MarkDirty();
        }
        QueryContext query(db);
        IndexScan scan(query, {"rows", 2});
        scan.Init();
        Tuple out({});
        Throws([&] { scan.Next(out); });
        VDB_CHECK(!scan.Exhausted());
        Throws([&] { query.FinishQuery(scan); });
        scan.Close(); query.Close();
        VDB_CHECK(!query.Succeeded());
        Pins(db);
        db.Close();
    }
}

bool Match(int64_t value, CompareOp op, int64_t constant) {
    if (op == CompareOp::Equal) return value == constant;
    if (op == CompareOp::Less) return value < constant;
    if (op == CompareOp::LessEqual) return value <= constant;
    if (op == CompareOp::Greater) return value > constant;
    return value >= constant;
}
void Filters() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const auto live = Populate(db, true);
    QueryContext query(db);
    for (CompareOp op : {CompareOp::Equal, CompareOp::Less, CompareOp::LessEqual,
                         CompareOp::Greater, CompareOp::GreaterEqual}) {
        for (int64_t value : {INT64_MIN, int64_t{0}, INT64_MAX}) {
            Filter filter(query, {{2, op, value}}, std::make_unique<SeqScan>(query, SeqScanPlan{"rows"}));
            Tuple out({});
            Throws([&] { filter.Next(out); });
            filter.Close(); filter.Init();
            Throws([&] { filter.Init(); });
            std::vector<int64_t> wanted;
            for (int64_t id : live) if (Match(Key(id), op, value)) wanted.push_back(id);
            VDB_CHECK(Drain(db, filter) == Expected(wanted));
            VDB_CHECK(!filter.OutputOrdering().ordered);
            filter.Close(); filter.Init();
            VDB_CHECK(Drain(db, filter) == Expected(wanted));
            filter.Close(); filter.Close();
            Throws([&] { filter.Next(out); });
        }
    }
    for (size_t column : {size_t{1}, size_t{3}})
        Throws([&] { Filter bad(query, {{column, CompareOp::Equal, 0}},
            std::make_unique<SeqScan>(query, SeqScanPlan{"rows"})); });
    Throws([&] { Filter bad(query, {{2, static_cast<CompareOp>(99), 0}},
        std::make_unique<SeqScan>(query, SeqScanPlan{"rows"})); });
    Throws([&] { Filter bad(query, {{2, CompareOp::Equal, 0}}, nullptr); });
    query.Close(); Pins(db); db.Close();
}

std::unique_ptr<Executor> Pipeline(QueryContext& query, bool indexed, std::vector<size_t> columns) {
    std::unique_ptr<Executor> child;
    if (indexed) child = std::make_unique<IndexScan>(query, IndexScanPlan{"rows", 2});
    else child = std::make_unique<SeqScan>(query, SeqScanPlan{"rows", true});
    child = std::make_unique<Filter>(query, FilterPlan{{2, CompareOp::GreaterEqual, 0}}, std::move(child));
    VDB_CHECK((child->OutputOrdering() == (indexed ? Ordering{true, 2} : Ordering{})));
    return std::make_unique<Projection>(query, ProjectionPlan{std::move(columns)}, std::move(child));
}
void ProjectionsAndPipelines() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    auto live = Populate(db, true);
    db.CreateTable("empty", Base());
    QueryContext query(db);
    const std::vector<std::vector<size_t>> positions = {{0}, {0,2}, {0,1}, {1,2,0}, {2,2,1}};
    for (bool indexed : {false, true}) {
        auto ids = live;
        if (indexed) std::stable_sort(ids.begin(), ids.end(), [](auto a, auto b) { return Key(a) < Key(b); });
        for (const auto& columns : positions) {
            auto root = Pipeline(query, indexed, columns);
            VDB_CHECK(root->OutputSchema().kind() == SchemaKind::Intermediate);
            VDB_CHECK(root->OutputSchema().size() == columns.size());
            Ordering ordering;
            for (size_t j = 0; indexed && j < columns.size(); ++j)
                if (columns[j] == 2) { ordering = {true, j}; break; }
            VDB_CHECK(root->OutputOrdering() == ordering);
            for (size_t j = 0; j < columns.size(); ++j) {
                const auto& column = root->OutputSchema().column(j);
                VDB_CHECK(column.name == Base().column(columns[j]).name);
                VDB_CHECK(column.type == Base().column(columns[j]).type);
                VDB_CHECK(column.varchar_limit == Base().column(columns[j]).varchar_limit);
            }
            EncodedRows expected;
            for (auto id : ids) {
                if (Key(id) < 0) continue;
                auto input = Row(id);
                std::vector<Value> values;
                for (size_t column : columns) values.push_back(input.value(column));
                expected.push_back(EncodeTuple(root->OutputSchema(), Tuple(std::move(values))));
            }
            Tuple first({});
            Throws([&] { root->Next(first); });
            root->Close(); root->Init();
            Throws([&] { root->Init(); });
            VDB_CHECK(root->Next(first));
            const auto owned = EncodeTuple(root->OutputSchema(), first);
            root->Close(); root->Close(); Pins(db);
            root->Init();
            VDB_CHECK(Drain(db, *root) == expected);
            Evict(db);
            VDB_CHECK(EncodeTuple(root->OutputSchema(), first) == owned);
            root->Close();
            Throws([&] { root->Next(first); });
            root->Init(); VDB_CHECK(Drain(db, *root) == expected); root->Close();
        }
    }
    auto make = [&](std::vector<size_t> columns) {
        return std::make_unique<Projection>(query, ProjectionPlan{std::move(columns)},
            std::make_unique<SeqScan>(query, SeqScanPlan{"rows"}));
    };
    auto maximum = make(std::vector<size_t>(16, 0));
    maximum->Init();
    Tuple tuple({}); VDB_CHECK(maximum->Next(tuple)); VDB_CHECK(tuple.size() == 16);
    maximum->Close();
    Throws([&] { make(std::vector<size_t>(17, 0)); });
    Throws([&] { make(std::vector<size_t>(8, 1)); }); // 4 + 8*258 > 2048.
    Throws([&] { make({3}); });
    Throws([&] { make({}); });
    Throws([&] { Projection bad(query, {{0}}, nullptr); });
    FinishEmpty(query);
    CleanScratch(db, files); db.Close();
}

void QueryBoundaries() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("rows", Base());
    const auto rid = db.Insert("rows", Row(0));
    {
        QueryContext query(db);
        Throws([&] { QueryContext second(db); });
        Throws([&] { db.Insert("rows", Row(1)); });
        Throws([&] { db.Delete("rows", rid); });
        Throws([&] { db.CreateIndex("rows", 2); });
        Throws([&] { db.CreateTable("other", Base()); });
        Throws([&] { db.Close(); });
        SeqScan scan(query, {"rows"});
        scan.Init();
        Throws([&] { query.Close(); });
        VDB_CHECK(Drain(db, scan).size() == 1);
        query.FinishQuery(scan);
        VDB_CHECK(query.Succeeded());
    }
    VDB_CHECK(db.Delete("rows", rid));
    db.Insert("rows", Row(2));
    db.CreateIndex("rows", 2);
    db.CreateTable("other", Base());
    {
        QueryContext query(db);
        auto root = Pipeline(query, true, {0, 1});
        root->Init(); root->Close(); query.Close();
        VDB_CHECK(!query.Succeeded());
    }
    // Destruction of the tree precedes the context's best-effort cleanup.
    {
        QueryContext query(db);
        SeqScan scan(query, {"rows"});
        scan.Init();
    }
    db.Insert("rows", Row(3));
    CleanScratch(db, files); db.Close();

    for (size_t frames : {size_t{1}, size_t{7}}) {
        Files small;
        Database storage(small.db, OpenMode::CreateNew, small.temp, frames);
        storage.CreateTable("rows", Base());
        Throws([&] { QueryContext bad(storage); });
        storage.Insert("rows", Row(0)); // Storage-only small pools remain supported.
        storage.CreateIndex("rows", 2);
        Pins(storage); storage.Close();
    }
}

void ExecutionFailures() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Populate(db, true);
    {
        QueryContext query(db);
        SeqScan scan(query, {"rows"});
        scan.Init();
        Tuple prefix({}); VDB_CHECK(scan.Next(prefix));
        db.Buffer().Invalidate({FileKind::Database, db.Table("rows").first_page});
        db.Disk().FailNextIo(FailNext::Read);
        Throws([&] { scan.Next(prefix); });
        VDB_CHECK(!scan.Exhausted());
        Throws([&] { query.FinishQuery(scan); });
        scan.Close(); query.Close();
        VDB_CHECK(!query.Succeeded()); // Caller discards the output prefix.
        Pins(db);
    }
    {
        QueryContext query(db);
        auto root = Pipeline(query, true, {0, 2});
        InvalidateDatabase(db);
        db.Disk().FailNextIo(FailNext::Read);
        Throws([&] { root->Init(); });
        Pins(db);
        // Init unwinds child/cursor state, so an ordinary injected read error can be retried.
        root->Init();
        VDB_CHECK(!Drain(db, *root).empty());
        query.FinishQuery(*root);
    }
    CleanScratch(db, files); db.Close();
}

Schema WideSchema() {
    std::vector<Column> columns;
    for (int i = 0; i < 7; ++i) columns.push_back({"v", TypeId::Varchar, 256});
    for (int i = 0; i < 8; ++i) columns.push_back({"i", TypeId::Int64});
    columns.push_back({"tail", TypeId::Varchar, 172});
    return IntermediateSchema(std::move(columns));
}
Tuple WideRow(int64_t id) {
    std::vector<Value> values;
    for (int i = 0; i < 7; ++i) values.emplace_back(std::string(256, char('a' + id % 26)));
    for (int i = 0; i < 8; ++i) values.emplace_back(id);
    values.emplace_back(std::string(172, 'z'));
    return Tuple(std::move(values));
}
void MemoryModel() {
    const Schema narrow(SchemaKind::Base, {{"i", TypeId::Int64}});
    const Schema varchar(SchemaKind::Base, {{"v", TypeId::Varchar, 5}});
    const auto wide = WideSchema();
    VDB_CHECK(wide.size() == 16 && wide.max_encoded_bytes() == 2048);
    VDB_CHECK(EncodeTuple(wide, WideRow(0)).size() == 2048);
    VDB_CHECK(RowMemoryCharge(narrow) == 120);
    VDB_CHECK(RowMemoryCharge(varchar) == 120); // 118 rounds to 120.
    VDB_CHECK(RowMemoryCharge(wide) == 4672);
    VDB_CHECK(FixedScratchCharge(4672) == 23232);
    VDB_CHECK(HashBuildRowCharge(narrow) == 216);
    VDB_CHECK(kAggregateGroupCharge == 192);
    const auto huge = std::numeric_limits<size_t>::max();
    Throws([&] { RowMemoryCharge(huge, 1); });
    Throws([&] { RowMemoryCharge(1, huge); });
    Throws([&] { RowMemoryCharge(0, (huge - 64) / 2); }); // Rounding overflow.
    Throws([&] { CheckedAdd(huge, 1); });
    Throws([&] { CheckedMultiply(huge, 2); });
    Throws([&] { FixedScratchCharge(huge); });
    Throws([&] { RemainingMemory(0, 1); });
    Throws([&] { RemainingMemory(1, FixedScratchCharge(RowMemoryCharge(narrow))); });
    VDB_CHECK(RemainingMemory(17, 17) == 0);
    VDB_CHECK(RemainingMemory(18, 17) == 1);
    VDB_CHECK(CheckedMultiply(huge, 0) == 0);
    QueryResources resources;
    const auto first = ResolveBudget(resources), second = ResolveBudget(resources);
    VDB_CHECK(first.max_bytes == 1024 * 1024 && second.max_bytes == first.max_bytes);
    VDB_CHECK(first.max_bytes + second.max_bytes > resources.work_memory_bytes);
    VDB_CHECK(resources.max_temp_pages == 16384);
    VDB_CHECK(ResolveBudget(resources, 256 * 1024).max_bytes == 256 * 1024);
    Throws([&] { ResolveBudget(resources, 0); });
    Throws([&] { ResolveBudget(QueryResources{0, 1}); });
    Throws([&] { QueryResources{1, 0}.Validate(); });
    if (huge > kInvalidPageId) Throws([&] { QueryResources{1, huge}.Validate(); });
    VDB_CHECK(ResolveBudget(QueryResources{1, 1}).max_bytes == 1);
    ValidateInt64Key(narrow, 0);
    Throws([&] { ValidateInt64Key(narrow, 1); });
    Throws([&] { ValidateInt64Key(varchar, 0); });
    const Schema nullable(SchemaKind::Intermediate, {{"i", TypeId::Int64, 0, true}});
    const Schema real(SchemaKind::Intermediate, {{"d", TypeId::Double}});
    Throws([&] { ValidateInt64Key(nullable, 0); });
    Throws([&] { ValidateInt64Key(real, 0); });
    Throws([&] { IntermediateSchema(std::vector<Column>(17, {"i", TypeId::Int64})); });
    Throws([&] { IntermediateSchema(std::vector<Column>(8, {"v", TypeId::Varchar, 256})); });
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Throws([&] { QueryContext bad(db, {0, 1}); });
    Throws([&] { QueryContext bad(db, {1, 0}); });
    db.CreateTable("empty", narrow); // Invalid resources did not activate a query.
    QueryContext good(db, {1, 1}); FinishEmpty(good); db.Close();
}

void EmptyAndSingleTemp() {
    static_assert(!std::is_constructible_v<TempRelation, TempStore&, Schema&&>);
    static_assert(!std::is_copy_constructible_v<TempWriter>);
    static_assert(!std::is_copy_constructible_v<TempReader>);
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("empty", Base());
    const auto schema = Base();
    QueryContext query(db);
    TempRelation empty(query.Temps(), schema);
    VDB_CHECK(&empty.OutputSchema() == &schema);
    VDB_CHECK(empty.FirstPage() == kInvalidPageId && empty.LastPage() == kInvalidPageId);
    VDB_CHECK(empty.RowCount() == 0 && empty.PageCount() == 0 && empty.ModeledRowBytes() == 0);
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 0);
    Throws([&] { TempReader premature(empty); });
    TempWriter writer(empty);
    Throws([&] { TempWriter second(empty); });
    Throws([&] { empty.Release(); });
    Throws([&] { query.Close(); });
    writer.Seal(); Pins(db);
    Throws([&] { writer.Append(Row(0)); });
    Throws([&] { writer.Seal(); });
    writer.Close(); writer.Close();
    VDB_CHECK(empty.Sealed());
    VDB_CHECK(query.Temps().Metrics().allocated_temp_pages == 0);
    TempReader reader(empty);
    VDB_CHECK(Drain(db, reader, schema).empty());
    reader.Rewind(); VDB_CHECK(Drain(db, reader, schema).empty());
    reader.Close(); reader.Close();
    Tuple out({}); Throws([&] { reader.Next(out); }); Throws([&] { reader.Rewind(); });
    empty.Release(); empty.Release();
    Throws([&] { TempReader released(empty); });

    PageBytes super{};
    { auto guard = db.Buffer().Fetch({FileKind::Database, 0}); super = guard.Data(); }
    TempRelation single(query.Temps(), schema);
    TempWriter append(single);
    for (int64_t i = 0; i < 4; ++i) { append.Append(Row(i)); Pins(db); }
    append.Seal(); Pins(db);
    VDB_CHECK(single.PageCount() == 1 && single.RowCount() == 4);
    VDB_CHECK(single.FirstPage() == 0 && single.LastPage() == 0);
    VDB_CHECK(single.ModeledRowBytes() == 4 * RowMemoryCharge(schema));
    {
        auto guard = db.Buffer().Fetch({FileKind::Temporary, 0});
        SlottedPage::Validate(guard.Data(), 0, kInvalidPageId);
        VDB_CHECK(SlottedPage::Next(guard.Data()) == kInvalidPageId);
    }
    { auto guard = db.Buffer().Fetch({FileKind::Database, 0}); VDB_CHECK(guard.Data() == super); }
    TempReader read(single);
    VDB_CHECK(Drain(db, read, schema) == Expected({0,1,2,3}));
    Throws([&] { single.Release(); });
    read.Close(); single.Release();
    VDB_CHECK(query.Temps().Metrics().temp_relations_created == 2);
    VDB_CHECK(query.Temps().Metrics().allocated_temp_pages == 1);
    FinishEmpty(query); CleanScratch(db, files); db.Close();
}

void MultiPageTemp() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("empty", Base());
    const auto schema = Base();
    QueryContext query(db);
    TempRelation relation(query.Temps(), schema);
    TempWriter writer(relation);
    std::vector<int64_t> ids;
    for (int64_t i = 0; i < 600; ++i) { writer.Append(Row(i)); ids.push_back(i); Pins(db); }
    writer.Seal(); Pins(db);
    VDB_CHECK(relation.PageCount() > 8 && relation.RowCount() == 600);
    VDB_CHECK(db.Metrics().dirty_evictions > 0);
    VDB_CHECK(query.Temps().Metrics().peak_temp_extent_pages == relation.PageCount());
    size_t pages = 0, slots = 0;
    PageId id = relation.FirstPage();
    while (id != kInvalidPageId) {
        auto guard = db.Buffer().Fetch({FileKind::Temporary, id});
        SlottedPage::Validate(guard.Data(), id, kInvalidPageId);
        slots += SlottedPage::LiveCount(guard.Data());
        const auto next = SlottedPage::Next(guard.Data());
        VDB_CHECK(next == kInvalidPageId || next > id);
        if (next == kInvalidPageId) VDB_CHECK(id == relation.LastPage());
        id = next; ++pages;
        VDB_CHECK(pages <= relation.PageCount());
    }
    VDB_CHECK(pages == relation.PageCount() && slots == 600);
    TempReader a(relation), b(relation);
    Tuple owned({}), row({});
    VDB_CHECK(a.Next(owned)); Pins(db);
    const auto saved = EncodeTuple(schema, owned);
    for (int64_t i = 0; i < 600; ++i) {
        VDB_CHECK(b.Next(row)); VDB_CHECK(EncodeTuple(schema, row) == EncodeTuple(schema, Row(i))); Pins(db);
        if (i > 0) {
            VDB_CHECK(a.Next(row)); VDB_CHECK(row.value(0).as_int64() == i); Pins(db);
        }
    }
    VDB_CHECK(!a.Next(row) && !b.Next(row));
    VDB_CHECK(EncodeTuple(schema, owned) == saved);
    a.Rewind();
    for (int i = 0; i < 31; ++i) VDB_CHECK(a.Next(row));
    b.Rewind(); VDB_CHECK(b.Next(row)); VDB_CHECK(row.value(0).as_int64() == 0);
    a.Rewind(); VDB_CHECK(Drain(db, a, schema) == Expected(ids));
    VDB_CHECK(b.Next(row)); VDB_CHECK(row.value(0).as_int64() == 1);
    TempReader independent(relation);
    VDB_CHECK(Drain(db, independent, schema) == Expected(ids));
    a.Close(); b.Close(); independent.Close();
    FinishEmpty(query); CleanScratch(db, files); db.Close();
}

void InterleavedTempAndRelease() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const auto schema = WideSchema();
    QueryContext query(db, {1024 * 1024, 8});
    TempRelation a(query.Temps(), schema), b(query.Temps(), schema);
    TempWriter wa(a), wb(b);
    for (int64_t i = 0; i < 3; ++i) { wa.Append(WideRow(i)); wb.Append(WideRow(i + 10)); Pins(db); }
    wa.Seal(); wb.Seal(); Pins(db);
    VDB_CHECK(a.FirstPage() == 0 && a.LastPage() == 4);
    VDB_CHECK(b.FirstPage() == 1 && b.LastPage() == 5);
    TempReader ra(a), rb(b);
    Tuple row({});
    for (int64_t i = 0; i < 3; ++i) {
        VDB_CHECK(ra.Next(row)); VDB_CHECK(EncodeTuple(schema, row) == EncodeTuple(schema, WideRow(i)));
        VDB_CHECK(rb.Next(row)); VDB_CHECK(EncodeTuple(schema, row) == EncodeTuple(schema, WideRow(i + 10)));
        Pins(db);
    }
    VDB_CHECK(!ra.Next(row) && !rb.Next(row));
    ra.Close(); rb.Close();
    db.Disk().Flush(FileKind::Temporary);
    const auto extent = std::filesystem::file_size(files.temp);
    const auto reads = db.Metrics().buffer_fetches;
    a.Release(); b.Release();
    VDB_CHECK(db.Metrics().buffer_fetches == reads); // Release never walks the chain.
    VDB_CHECK(std::filesystem::file_size(files.temp) == extent && extent == 6 * kPageSize);
    TempRelation c(query.Temps(), schema);
    TempWriter wc(c); wc.Append(WideRow(20)); wc.Seal();
    VDB_CHECK(c.FirstPage() == 6 && c.LastPage() == 6);
    VDB_CHECK(query.Temps().Metrics().allocated_temp_pages == 7);
    c.Release(); query.Close(); CleanScratch(db, files); db.Close();
}

void TempCapAndAbandonment() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const auto schema = WideSchema();
    QueryContext query(db, {1024 * 1024, 2});
    TempRelation relation(query.Temps(), schema);
    TempWriter writer(relation);
    writer.Append(WideRow(1)); writer.Append(WideRow(2));
    const auto allocations = db.Disk().counters().temporary_allocations;
    Throws([&] { writer.Append(WideRow(3)); }); Pins(db);
    VDB_CHECK(db.Disk().counters().temporary_allocations == allocations);
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 2);
    VDB_CHECK(relation.PageCount() == 2 && relation.RowCount() == 2);
    writer.Close(); writer.Close(); // Abandoned unsealed tail is never exposed to readers.
    Throws([&] { TempReader bad(relation); });
    Throws([&] { TempWriter reopen(relation); });
    relation.Release();
    TempRelation later(query.Temps(), schema);
    TempWriter second(later);
    Throws([&] { second.Append(WideRow(4)); });
    VDB_CHECK(later.RowCount() == 0 && later.PageCount() == 0);
    VDB_CHECK(query.Temps().Metrics().allocated_temp_pages == 2);
    db.Disk().Flush(FileKind::Temporary);
    VDB_CHECK(std::filesystem::file_size(files.temp) == 2 * kPageSize);
    second.Close(); query.Close();
    VDB_CHECK(!query.Succeeded());
    CleanScratch(db, files); db.Close();
}

void SafeResetAndFailure() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("empty", Base());
    const auto schema = Base();
    {
        QueryContext query(db);
        TempRelation relation(query.Temps(), schema);
        TempWriter writer(relation); writer.Append(Row(0)); writer.Seal();
        bool dirty = false;
        for (const auto& frame : db.Buffer().InspectFrames())
            if (frame.page == PageRef{FileKind::Temporary, 0}) dirty = frame.dirty;
        VDB_CHECK(dirty);
        TempReader reader(relation);
        SeqScan root(query, {"empty"}); root.Init();
        Tuple out({}); VDB_CHECK(!root.Next(out));
        Throws([&] { query.FinishQuery(root); }); // Open reader prevents reset, even without pins.
        VDB_CHECK(!query.Succeeded());
        reader.Close();
        const auto writes = db.Disk().counters().temporary_writes;
        db.Disk().FailNextIo(FailNext::ResetTemporary);
        Throws([&] { query.FinishQuery(root); });
        VDB_CHECK(!query.Succeeded());
        VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 1);
        // The focused failure occurs inside reset, after all cached scratch pages are discarded.
        for (const auto& frame : db.Buffer().InspectFrames())
            VDB_CHECK(!frame.page || frame.page->file != FileKind::Temporary);
        VDB_CHECK(db.Disk().counters().temporary_writes == writes);
        Throws([&] { QueryContext second(db); });
        Throws([&] { db.Insert("empty", Row(1)); });
        query.Close(); // This one-shot injected failure permits a normal checked cleanup retry.
        VDB_CHECK(!query.Succeeded());
        db.Buffer().FlushAll();
        VDB_CHECK(db.Disk().counters().temporary_writes == writes);
        CleanScratch(db, files);
        Throws([&] { db.Buffer().Fetch({FileKind::Temporary, 0}); });
        Throws([&] { TempReader stale(relation); });
        Throws([&] { TempRelation stale(query.Temps(), schema); });
    }
    {
        QueryContext next(db);
        TempRelation relation(next.Temps(), schema);
        TempWriter writer(relation); writer.Append(Row(99)); writer.Seal();
        VDB_CHECK(relation.FirstPage() == 0); // IDs restart only in a new query.
        TempReader reader(relation);
        VDB_CHECK(Drain(db, reader, schema) == Expected({99}));
        reader.Close();
        FinishEmpty(next);
        VDB_CHECK(next.Temps().Metrics().allocated_temp_pages == 1); // Historical metric survives reset.
        CleanScratch(db, files);
    }
    // Startup also discards dirty legacy Temporary mappings before resetting their file.
    {
        auto guard = db.Buffer().NewPage(FileKind::Temporary);
        guard.MutableData().fill(std::byte{0x7a}); guard.MarkDirty();
    }
    { QueryContext clean(db); CleanScratch(db, files); FinishEmpty(clean); }
    db.Close();
}

void PinnedReset() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("empty", Base());
    const auto schema = Base();
    QueryContext query(db);
    TempRelation relation(query.Temps(), schema);
    TempWriter writer(relation); writer.Append(Row(1)); writer.Seal();
    auto pin = db.Buffer().Fetch({FileKind::Temporary, relation.FirstPage()});
    SeqScan root(query, {"empty"}); root.Init();
    Tuple out({}); VDB_CHECK(!root.Next(out));
    const auto extent = db.Disk().PageCount(FileKind::Temporary);
    Throws([&] { query.FinishQuery(root); });
    VDB_CHECK(!query.Succeeded());
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == extent);
    VDB_CHECK(db.Metrics().active_pins == 1);
    VDB_CHECK(SlottedPage::LiveCount(pin.Data()) == 1); // Guard and dirty cached bytes are intact.
    Throws([&] { QueryContext next(db); });
    pin.Drop(); Pins(db);
    query.FinishQuery(root);
    VDB_CHECK(query.Succeeded());
    CleanScratch(db, files); db.Close();
}

void TempIoErrors() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const auto schema = Base();
    {
        QueryContext query(db);
        TempRelation relation(query.Temps(), schema);
        TempWriter writer(relation);
        Throws([&] { writer.Append(Tuple({Value(int64_t{1})})); });
        VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 0);
        writer.Append(Row(0));
        db.Buffer().Invalidate({FileKind::Temporary, relation.LastPage()});
        db.Disk().FailNextIo(FailNext::Read);
        Throws([&] { writer.Seal(); });
        VDB_CHECK(!relation.Sealed()); Pins(db);
        writer.Close(); query.Close();
        CleanScratch(db, files);
    }
    {
        QueryContext query(db);
        TempRelation relation(query.Temps(), schema);
        TempWriter writer(relation); writer.Append(Row(0)); writer.Seal();
        db.Buffer().Invalidate({FileKind::Temporary, relation.FirstPage()});
        TempReader reader(relation);
        db.Disk().FailNextIo(FailNext::Read);
        Tuple out({});
        Throws([&] { reader.Next(out); }); Pins(db);
        reader.Close(); query.Close();
        CleanScratch(db, files);
    }
    // Private copies do not mask a corrupt forward chain as successful EOF.
    {
        QueryContext query(db);
        TempRelation relation(query.Temps(), schema);
        TempWriter writer(relation); writer.Append(Row(0)); writer.Seal();
        {
            auto guard = db.Buffer().Fetch({FileKind::Temporary, relation.FirstPage()});
            SlottedPage::SetNext(guard.MutableData(), relation.FirstPage()); guard.MarkDirty();
        }
        TempReader reader(relation);
        Tuple out({}); VDB_CHECK(reader.Next(out));
        Throws([&] { reader.Next(out); }); Pins(db);
        reader.Close(); query.Close(); CleanScratch(db, files);
    }
    db.Close();
}

void FailedAllocationStillCounts() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Populate(db);
    const auto schema = Base();
    QueryContext query(db, {1024 * 1024, 1});
    std::vector<PageGuard> external_pins;
    for (PageId id = 0; id < 8; ++id) external_pins.push_back(db.Buffer().Fetch({FileKind::Database, id}));
    TempRelation relation(query.Temps(), schema);
    TempWriter writer(relation);
    Throws([&] { writer.Append(Row(0)); }); // BufferPool allocates before finding all frames pinned.
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 1);
    VDB_CHECK(query.Temps().Metrics().allocated_temp_pages == 1);
    VDB_CHECK(query.Temps().Metrics().current_temp_extent_pages == 1);
    VDB_CHECK(query.Temps().Metrics().peak_temp_extent_pages == 1);
    VDB_CHECK(relation.PageCount() == 0 && relation.RowCount() == 0);
    external_pins.clear();
    VDB_CHECK(db.Metrics().active_pins == 0);
    Throws([&] { writer.Append(Row(1)); });
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 1);
    writer.Close(); relation.Release(); query.Close();
    VDB_CHECK(query.Temps().Metrics().current_temp_extent_pages == 0);
    VDB_CHECK(db.Metrics().active_pins == 0);
    VDB_CHECK(std::filesystem::file_size(files.temp) == 0);
    db.Close();
    std::cout << "allocation-failure fixture: intentional external peak pins=8, final active pins=0\n";
}

void StartupResetFailure() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.Flush();
    db.Disk().FailNextIo(FailNext::ResetTemporary);
    Throws([&] { QueryContext failed(db); });
    Throws([&] { QueryContext next(db); });
    Throws([&] { db.CreateTable("forbidden", Base()); });
    Pins(db);
    // Construction failed after activation. No context owns cleanup; abandon this instance.
}
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"empty_lifecycle", EmptyLifecycle}, {"seq_scans", SeqScans},
        {"index_scans", IndexScans}, {"index_corruption", IndexCorruption},
        {"filters", Filters}, {"projections_pipelines", ProjectionsAndPipelines},
        {"query_boundaries", QueryBoundaries}, {"execution_failures", ExecutionFailures},
        {"memory_model", MemoryModel}, {"empty_single_temp", EmptyAndSingleTemp},
        {"multi_page_temp", MultiPageTemp}, {"interleaved_release", InterleavedTempAndRelease},
        {"temp_cap", TempCapAndAbandonment}, {"safe_reset_failure", SafeResetAndFailure},
        {"pinned_reset", PinnedReset}, {"temp_io_errors", TempIoErrors},
        {"allocation_failure", FailedAllocationStillCounts}, {"startup_failure", StartupResetFailure}};
    try {
        for (const auto& [name, run] : tests) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            run(); ++cases_passed; std::cout << "PASS " << name << '\n';
        }
        VDB_CHECK(cases_passed != 0);
        std::cout << cases_passed << " execution groups passed; streaming peak pins=" << maximum_stream_pins
                  << ", relevant scopes final active pins=0\n";
    } catch (const std::exception& error) {
        std::cerr << "execution failure after " << cases_passed << " groups: " << error.what() << '\n';
        return 1;
    }
}
