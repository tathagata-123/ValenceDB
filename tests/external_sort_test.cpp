#include "test_harness.h"
#include "valencedb/execution/external_sort.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <random>

using namespace valencedb;
namespace {
size_t cases_passed = 0;
uint64_t maximum_pins = 0;
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_sort_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string(++serial));
        std::filesystem::create_directory(directory);
        db = directory / "pages.db";
        temp = directory / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(directory); }
};
void Throws(const std::function<void()>& action, const std::string& message = "") {
    bool caught = false;
    try { action(); } catch (const DbError& error) {
        caught = true;
        VDB_CHECK(std::string(error.what()).find(message) != std::string::npos);
    }
    VDB_CHECK(caught);
}
void Pins(Database& db) {
    VDB_CHECK(db.Metrics().active_pins == 0);
    maximum_pins = std::max(maximum_pins, db.Metrics().peak_pins);
    VDB_CHECK(db.Metrics().peak_pins <= 1);
}
void Clean(Database& db, const Files& files) {
    Pins(db);
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 0);
    VDB_CHECK(std::filesystem::file_size(files.temp) == 0);
    for (const auto& frame : db.Buffer().InspectFrames())
        VDB_CHECK(!frame.page || frame.page->file != FileKind::Temporary);
}
Schema PayloadSchema() {
    return Schema(SchemaKind::Base, {{"id", TypeId::Int64},
        {"payload", TypeId::Varchar, 256}, {"key", TypeId::Int64}});
}
Schema WideSchema() {
    std::vector<Column> columns;
    for (int i = 0; i < 7; ++i) columns.push_back({"v", TypeId::Varchar, 256});
    for (int i = 0; i < 8; ++i) columns.push_back({"i", TypeId::Int64});
    columns.push_back({"tail", TypeId::Varchar, 172});
    return IntermediateSchema(std::move(columns));
}
Tuple Row(int64_t id, int64_t key, std::string payload = "") {
    return Tuple({Value(id), Value(std::move(payload)), Value(key)});
}
std::vector<Tuple> Rows(size_t count, uint64_t seed = 707, bool full_width = false) {
    std::mt19937_64 rng(seed);
    std::vector<Tuple> result;
    for (size_t i = 0; i < count; ++i) {
        const size_t width = full_width ? 256 : rng() % 257;
        std::string payload(width, 'x');
        for (auto& c : payload) c = static_cast<char>(rng() % 256); // Includes embedded NULs.
        int64_t key = static_cast<int64_t>(rng() % 31) - 15;
        if (i % 37 == 0) key = INT64_MIN;
        if (i % 41 == 0) key = INT64_MAX;
        result.push_back(Row(static_cast<int64_t>(i), key, std::move(payload)));
        if (i > 0 && i % 19 == 0) result.back() = result[i - 1]; // Identical duplicate rows too.
    }
    return result;
}
struct SourceState {
    size_t inits = 0, next_calls = 0, delivered = 0, allowed_inits = 1;
    size_t fail_at = std::numeric_limits<size_t>::max();
};
// No rewinds exist: restarting within one execution is an explicit test failure.
class Source final : public Executor {
public:
    Source(QueryContext& context, Schema schema, const std::vector<Tuple>& rows, SourceState& state)
        : Executor(context), schema_(std::move(schema)), rows_(rows), state_(state) {}
    ~Source() noexcept override { Close(); }
    void DoInit() override {
        ++state_.inits;
        if (state_.inits > state_.allowed_inits) { Close(); throw DbError("illegal child replay"); }
        position_ = 0;
    }
    bool DoNext(Tuple& out) override {
        Pins(context_.Db());
        ++state_.next_calls;
        if (position_ == state_.fail_at) throw DbError("injected child failure");
        if (position_ == rows_.size()) return false;
        out = rows_[position_++];
        ++state_.delivered;
        return true;
    }
    void DoClose() noexcept override { }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
private:
    Schema schema_;
    const std::vector<Tuple>& rows_;
    SourceState& state_;
    size_t position_ = 0;
};
std::unique_ptr<Source> Child(QueryContext& q, const std::vector<Tuple>& rows, SourceState& state) {
    return std::make_unique<Source>(q, PayloadSchema(), rows, state);
}
using Bag = std::vector<std::vector<std::byte>>;
Bag Canonical(const Schema& schema, const std::vector<Tuple>& rows) {
    Bag result;
    for (const auto& row : rows) result.push_back(EncodeTuple(schema, row));
    std::sort(result.begin(), result.end());
    return result;
}
void Oracle(const Schema& schema, size_t key, const std::vector<Tuple>& input,
            const std::vector<Tuple>& output, bool unique = false) {
    VDB_CHECK(input.size() == output.size());
    auto reference = input;
    std::sort(reference.begin(), reference.end(), [key](const Tuple& a, const Tuple& b) {
        return a.value(key).as_int64() < b.value(key).as_int64();
    });
    for (size_t i = 0; i < output.size(); ++i) {
        if (i != 0) VDB_CHECK(output[i - 1].value(key).as_int64() <= output[i].value(key).as_int64());
        VDB_CHECK(reference[i].value(key).as_int64() == output[i].value(key).as_int64());
        if (unique) VDB_CHECK(EncodeTuple(schema, reference[i]) == EncodeTuple(schema, output[i]));
    }
    VDB_CHECK(Canonical(schema, input) == Canonical(schema, output));
}
std::vector<Tuple> Drain(Database& db, Executor& sort) {
    std::vector<Tuple> result;
    Tuple out({});
    while (sort.Next(out)) { result.push_back(out); Pins(db); }
    const auto before = result.empty() ? std::vector<std::byte>{} : EncodeTuple(sort.OutputSchema(), out);
    VDB_CHECK(!sort.Next(out));
    if (!result.empty()) VDB_CHECK(before == EncodeTuple(sort.OutputSchema(), out));
    Pins(db);
    return result;
}
void Bounds(const ExternalSort& sort) {
    const auto& m = sort.Memory();
    const auto& metrics = sort.Metrics();
    VDB_CHECK(metrics.peak_retained_rows <= std::max(m.run_capacity, m.merge_fan_in));
    VDB_CHECK(metrics.peak_run_descriptors <= m.descriptor_capacity);
    VDB_CHECK(metrics.peak_merge_heads <= m.merge_fan_in);
    VDB_CHECK(metrics.modeled_peak_bytes <= m.max_bytes);
}
void Report(const std::string& name, const ExternalSort& sort, QueryContext& q) {
    const auto& m = sort.Memory();
    const auto& s = sort.Metrics();
    const auto& io = q.Db().Disk().counters();
    std::cout << name << ": M=" << m.max_bytes << " T=" << m.row_bytes << " S=" << m.scratch_bytes
        << " D=" << m.descriptor_capacity << " R=" << m.descriptor_bytes << " C=" << m.run_capacity
        << " k=" << m.merge_fan_in << " initial=" << s.initial_runs << " materialized=" << s.materialized_runs
        << " passes=" << s.intermediate_merge_passes << " final=" << s.final_merge_fan_in
        << " retained=" << s.peak_retained_rows << " modeled_peak=" << s.modeled_peak_bytes
        << " extent=" << q.Temps().Metrics().allocated_temp_pages
        << " physical_reads=" << io.temporary_reads << " writes=" << io.temporary_writes
        << " allocation_writes=" << io.temporary_allocations << '\n';
}

void MemoryValidation() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db);
    const std::vector<Tuple> empty;
    SourceState state;
    ExternalSort defaults(q, {2}, Child(q, empty, state));
    const auto& m = defaults.Memory();
    VDB_CHECK(m.max_bytes == 1048576 && m.row_bytes == 720 && m.scratch_bytes == 11376);
    VDB_CHECK(m.descriptor_capacity == 1024 && m.descriptor_bytes == 131072);
    VDB_CHECK(m.run_capacity == 1258 && m.merge_fan_in == 64);
    ExternalSort local(q, {2, 24576}, Child(q, empty, state));
    VDB_CHECK(local.Memory().run_capacity == 14 && local.Memory().merge_fan_in == 2);
    ExternalSort huge(q, {2, std::numeric_limits<size_t>::max()}, Child(q, empty, state));
    VDB_CHECK(huge.Memory().descriptor_capacity == 4096 && huge.Memory().merge_fan_in == 64);
    for (size_t bytes : {size_t{0}, size_t{1}, size_t{1024}, size_t{10000}, size_t{13000}})
        Throws([&] { ExternalSort bad(q, {2, bytes}, Child(q, empty, state)); });
    Throws([&] { ExternalSort bad(q, {3}, Child(q, empty, state)); });
    Throws([&] { ExternalSort bad(q, {1}, Child(q, empty, state)); }, "NONNULL INT64");
    Throws([&] { ExternalSort bad(q, {0}, nullptr); });
    for (const auto& schema : {Schema(SchemaKind::Intermediate, {{"key", TypeId::Int64, 0, true}}),
                               Schema(SchemaKind::Intermediate, {{"key", TypeId::Double}})}) {
        const std::vector<Tuple> nonnull = {Tuple({schema.column(0).type == TypeId::Int64
            ? Value(int64_t{1}) : Value(1.0)})};
        Throws([&] { ExternalSort bad(q, {0}, std::make_unique<Source>(q, schema, nonnull, state)); },
               "NONNULL INT64");
    }
    const auto wide = WideSchema();
    VDB_CHECK(wide.max_encoded_bytes() == 2048 && RowMemoryCharge(wide) == 4672);
    Throws([&] { ExternalSort bad(q, {7, 4 * kPageSize},
                                 std::make_unique<Source>(q, wide, empty, state)); }, "scratch");
    VDB_CHECK(state.inits == 0 && state.next_calls == 0);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
    Files other_files;
    Database other(other_files.db, OpenMode::CreateNew, other_files.temp, 8);
    QueryContext other_q(other);
    Throws([&] { ExternalSort bad(q, {2}, Child(other_q, empty, state)); }, "this query");
    other_q.Close(); other.Close();
    q.Close(); Clean(db, files); db.Close();
}

void PrivateAndBoundaries() {
    for (size_t count : {size_t{0}, size_t{1}, size_t{13}, size_t{14}, size_t{15}, size_t{28}, size_t{29}}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db, {24576, 16384});
        auto rows = Rows(count);
        SourceState state;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        VDB_CHECK(sort.OutputOrdering() == (Ordering{true, 2}));
        VDB_CHECK(sort.OutputSchema().size() == 3 && sort.OutputSchema().column(1).varchar_limit == 256);
        Tuple out = Row(0, 0, "sentinel");
        Throws([&] { sort.Next(out); }, "Init");
        sort.Close(); sort.Close();
        sort.Init(); Pins(db);
        Throws([&] { sort.Init(); }, "Close");
        Throws([&] { q.FinishQuery(sort); }, "EOF");
        std::vector<Tuple> output;
        while (sort.Next(out)) { output.push_back(out); Pins(db); }
        const auto last = EncodeTuple(sort.OutputSchema(), out);
        VDB_CHECK(!sort.Next(out) && EncodeTuple(sort.OutputSchema(), out) == last);
        Oracle(sort.OutputSchema(), 2, rows, output);
        VDB_CHECK(state.inits == 1 && state.delivered == count && state.next_calls == count + 1);
        const size_t runs = (count + 13) / 14;
        VDB_CHECK(sort.Metrics().initial_runs == runs);
        VDB_CHECK(sort.Metrics().pending_boundary_tuples == (runs == 0 ? 0 : runs - 1));
        if (count <= 14) {
            VDB_CHECK(!sort.Metrics().final_merge_streamed && sort.Metrics().materialized_runs == 0);
            VDB_CHECK(q.Temps().Metrics().temp_relations_created == 0);
            VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
        } else VDB_CHECK(sort.Metrics().final_merge_streamed);
        Bounds(sort);
        sort.Close(); sort.Close(); // Close preserves normal EOF for FinishQuery.
        Throws([&] { sort.Next(out); }, "Init");
        q.FinishQuery(sort); VDB_CHECK(q.Succeeded()); Clean(db, files); db.Close();
    }
}

// This fixture has no interleaved allocations. Independently decode every physical run chain,
// including obsolete input runs, and compare initial chains against exact input chunks.
void InspectRuns(Database& db, const ExternalSort& sort, const std::vector<Tuple>& source) {
    PageId page = 0;
    const PageId extent = db.Disk().PageCount(FileKind::Temporary);
    size_t run = 0;
    while (page < extent) {
        std::vector<Tuple> rows;
        for (;;) {
            PageId next;
            {
                auto guard = db.Buffer().Fetch({FileKind::Temporary, page});
                SlottedPage::Validate(guard.Data(), page, kInvalidPageId);
                for (uint16_t slot = 0; slot < SlottedPage::SlotCount(guard.Data()); ++slot) {
                    auto bytes = SlottedPage::Fetch(guard.Data(), slot);
                    VDB_CHECK(bytes.has_value());
                    rows.push_back(DecodeTuple(sort.OutputSchema(), *bytes));
                }
                next = SlottedPage::Next(guard.Data());
            }
            ++page;
            if (next == kInvalidPageId) break;
            VDB_CHECK(next == page && next < extent);
        }
        VDB_CHECK(!rows.empty() && rows.size() < source.size()); // No final relation.
        for (size_t i = 1; i < rows.size(); ++i)
            VDB_CHECK(rows[i - 1].value(2).as_int64() <= rows[i].value(2).as_int64());
        if (run < sort.Metrics().initial_runs) {
            const size_t first = run * sort.Memory().run_capacity;
            const size_t end = std::min(first + sort.Memory().run_capacity, source.size());
            const std::vector<Tuple> chunk(source.begin() + first, source.begin() + end);
            Oracle(sort.OutputSchema(), 2, chunk, rows); // Includes every pending boundary row.
        }
        ++run;
    }
    VDB_CHECK(run == sort.Metrics().materialized_runs);
    Pins(db);
}

void MultiplePasses() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {24576, 16384});
    auto rows = Rows(309, 707, true); // C=14, 23 runs, k=2: 23 -> 12 -> 6 -> 3 -> 2.
    SourceState state;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    sort.Init(); Pins(db);
    VDB_CHECK(sort.Metrics().initial_runs == 23 && sort.Metrics().intermediate_merge_passes == 4);
    VDB_CHECK(sort.Metrics().final_merge_fan_in == 2 && sort.Metrics().materialized_runs == 44);
    VDB_CHECK(sort.Metrics().pending_boundary_tuples == 22);
    VDB_CHECK(sort.Metrics().peak_retained_rows == 14 && sort.Metrics().modeled_peak_bytes == 24528);
    const auto pages = q.Temps().Metrics().allocated_temp_pages;
    VDB_CHECK(pages == 107 && q.Temps().Metrics().allocated_temp_pages == pages);
    VDB_CHECK(q.Temps().Metrics().temp_relations_created == 44);
    Oracle(sort.OutputSchema(), 2, rows, Drain(db, sort));
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == pages); // Next never allocates final output.
    VDB_CHECK(q.Temps().Metrics().temp_relations_created == 44);
    VDB_CHECK(state.inits == 1 && state.next_calls == rows.size() + 1);
    Bounds(sort); Report("two_way_four_pass", sort, q);
    InspectRuns(db, sort, rows);
    q.FinishQuery(sort); Clean(db, files); db.Close();
}

void MultiwayAndUnique() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {32768, 16384});
    std::vector<Tuple> rows;
    for (int64_t i = 264; i >= 0; --i) rows.push_back(Row(i, i - 130, std::string(256, 'a')));
    SourceState state;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    sort.Init();
    VDB_CHECK(sort.Memory().run_capacity == 24 && sort.Memory().merge_fan_in == 3);
    VDB_CHECK(sort.Metrics().initial_runs == 12 && sort.Metrics().intermediate_merge_passes == 2);
    VDB_CHECK(sort.Metrics().final_merge_fan_in == 2);
    Oracle(sort.OutputSchema(), 2, rows, Drain(db, sort), true);
    Bounds(sort); Report("three_way", sort, q);
    InspectRuns(db, sort, rows);
    q.FinishQuery(sort); Clean(db, files); db.Close();
}

void FanInExceedsFrames() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {131072, 16384});
    auto rows = Rows(1431, 123456, true);
    SourceState state;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    sort.Init(); Pins(db);
    VDB_CHECK(sort.Memory().run_capacity == 143 && sort.Memory().merge_fan_in == 20);
    VDB_CHECK(sort.Metrics().initial_runs == 11 && sort.Metrics().intermediate_merge_passes == 0);
    VDB_CHECK(sort.Metrics().final_merge_fan_in == 11 && sort.Metrics().peak_merge_heads == 11);
    VDB_CHECK(sort.Metrics().final_merge_fan_in > db.Buffer().FrameCount());
    const auto pages = q.Temps().Metrics().allocated_temp_pages;
    const auto relations = q.Temps().Metrics().temp_relations_created;
    Tuple owned({}); VDB_CHECK(sort.Next(owned));
    const auto saved = EncodeTuple(sort.OutputSchema(), owned);
    auto output = Drain(db, sort);
    output.insert(output.begin(), owned);
    VDB_CHECK(EncodeTuple(sort.OutputSchema(), owned) == saved);
    Oracle(sort.OutputSchema(), 2, rows, output);
    VDB_CHECK(pages == q.Temps().Metrics().allocated_temp_pages);
    VDB_CHECK(relations == q.Temps().Metrics().temp_relations_created);
    VDB_CHECK(db.Metrics().dirty_evictions > 0 && db.Disk().counters().temporary_reads > 0);
    Bounds(sort); Report("eleven_readers_eight_frames", sort, q);
    q.FinishQuery(sort); Clean(db, files); db.Close();
}

void FanInAndDescriptorErrors() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    {
        QueryContext q(db, {16384, 16384});
        const Schema narrow(SchemaKind::Base, {{"key", TypeId::Int64}});
        std::vector<Tuple> rows;
        for (int64_t i = 38; i >= 0; --i) rows.emplace_back(std::vector<Value>{Value(i)});
        SourceState state;
        ExternalSort one(q, {0}, std::make_unique<Source>(q, narrow, rows, state));
        VDB_CHECK(one.Memory().run_capacity == 39 && one.Memory().merge_fan_in == 1);
        one.Init(); Oracle(narrow, 0, rows, Drain(db, one)); one.Close();
        rows.emplace_back(std::vector<Value>{Value(int64_t{-1})});
        SourceState fail;
        ExternalSort many(q, {0}, std::make_unique<Source>(q, narrow, rows, fail));
        Throws([&] { many.Init(); }, "fan-in");
        VDB_CHECK(!many.Exhausted() && fail.inits == 1 && fail.delivered == 40);
        VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
        many.Close(); q.Close(); Clean(db, files);
    }
    {
        QueryContext q(db, {24576, 16384});
        auto rows = Rows(14 * 24 + 1);
        SourceState state;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        Throws([&] { sort.Init(); }, "descriptor cap");
        VDB_CHECK(sort.Memory().descriptor_capacity == 24);
        VDB_CHECK(sort.Metrics().initial_runs == 24 && sort.Metrics().peak_run_descriptors == 24);
        VDB_CHECK(q.Temps().Metrics().temp_relations_created == 24);
        VDB_CHECK(state.inits == 1 && state.delivered == rows.size());
        VDB_CHECK(!sort.Exhausted()); Bounds(sort); Pins(db);
        sort.Close(); q.Close(); Clean(db, files);
    }
    db.Close();
}

void ReplayAndEarlyClose() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {24576, 16384});
    auto rows = Rows(85);
    SourceState state; state.allowed_inits = 3;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    sort.Init();
    const auto extent = q.Temps().Metrics().allocated_temp_pages;
    const auto initial = sort.Metrics().initial_runs;
    Tuple owned({}); VDB_CHECK(sort.Next(owned));
    const auto saved = EncodeTuple(sort.OutputSchema(), owned);
    sort.Close(); sort.Close(); Pins(db);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == extent);
    sort.Init();
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == extent * 2);
    VDB_CHECK(sort.Metrics().initial_runs == initial); // Metrics reset per execution.
    auto second = Drain(db, sort);
    Oracle(sort.OutputSchema(), 2, rows, second);
    sort.Close(); sort.Init();
    auto third = Drain(db, sort);
    Oracle(sort.OutputSchema(), 2, second, third);
    VDB_CHECK(EncodeTuple(sort.OutputSchema(), owned) == saved);
    VDB_CHECK(state.inits == 3 && state.delivered == rows.size() * 3);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == extent * 3);
    Bounds(sort); q.FinishQuery(sort); Clean(db, files);
    {
        QueryContext early(db, {24576, 16384});
        SourceState fresh;
        { ExternalSort partial(early, {2}, Child(early, rows, fresh)); partial.Init();
          VDB_CHECK(partial.Next(owned)); } // Destructor closes final handles.
        early.Close(); VDB_CHECK(!early.Succeeded()); Clean(db, files);
    }
    db.Close();
}

void CumulativeTempLimit() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {24576, 3});
    auto rows = Rows(15, 99, true);
    SourceState state; state.allowed_inits = 2;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    sort.Init(); Oracle(sort.OutputSchema(), 2, rows, Drain(db, sort));
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 2);
    sort.Close(); Pins(db);
    Throws([&] { sort.Init(); }, "temporary page cap");
    VDB_CHECK(!sort.Exhausted());
    VDB_CHECK(sort.Metrics().initial_runs == 1); // The replay's first run used the third page.
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 3);
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 3);
    Throws([&] { q.FinishQuery(sort); }, "EOF");
    sort.Close(); q.Close(); VDB_CHECK(!q.Succeeded()); Clean(db, files); db.Close();
}

void TempLimitDuringMerge() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {24576, 6});
    auto rows = Rows(57, 99, true); // Five initial one-page runs fit; first merge needs two more pages.
    SourceState state;
    ExternalSort sort(q, {2}, Child(q, rows, state));
    Throws([&] { sort.Init(); }, "temporary page cap");
    VDB_CHECK(sort.Metrics().initial_runs == 5 && sort.Metrics().materialized_runs == 5);
    VDB_CHECK(sort.Metrics().intermediate_merge_passes == 0 && !sort.Metrics().final_merge_streamed);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 6);
    VDB_CHECK(q.Temps().Metrics().temp_relations_created == 6);
    VDB_CHECK(state.inits == 1 && state.delivered == rows.size());
    Pins(db); sort.Close(); q.Close(); Clean(db, files); db.Close();
}

void FailureCleanup() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    auto rows = Rows(1431, 707, true);
    {
        QueryContext q(db, {24576, 16384});
        SourceState state; state.fail_at = 43;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        Throws([&] { sort.Init(); }, "child failure");
        VDB_CHECK(sort.Metrics().initial_runs > 0 && !sort.Exhausted());
        Pins(db); q.Close(); Clean(db, files);
    }
    {
        QueryContext q(db, {131072, 16384});
        SourceState state;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        db.Disk().FailNextIo(FailNext::Write);
        Throws([&] { sort.Init(); }, "injected page write");
        VDB_CHECK(!sort.Exhausted()); Pins(db); q.Close(); Clean(db, files);
    }
    {
        QueryContext q(db, {131072, 16384});
        SourceState state;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        sort.Init();
        for (PageId id = 0; id < db.Disk().PageCount(FileKind::Temporary); ++id)
            db.Buffer().Invalidate({FileKind::Temporary, id});
        db.Disk().FailNextIo(FailNext::Read);
        size_t prefix = 0;
        Tuple row({});
        Throws([&] { while (sort.Next(row)) ++prefix; }, "injected page read");
        VDB_CHECK(prefix > 0 && prefix < rows.size() && !sort.Exhausted());
        Throws([&] { q.FinishQuery(sort); }, "EOF");
        Pins(db); q.Close(); Clean(db, files); // Whole prefix is deliberately discarded.
    }
    db.Close();
}

void RandomizedOracle() {
    for (uint64_t seed : {uint64_t{7}, uint64_t{707}, uint64_t{0xdecafbad}, uint64_t{812381}}) {
        std::cout << "random seed=" << seed << '\n';
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db, {65536, 16384});
        auto rows = Rows(2500, seed);
        SourceState state;
        ExternalSort sort(q, {2}, Child(q, rows, state));
        sort.Init();
        VDB_CHECK(sort.Metrics().initial_runs > sort.Memory().merge_fan_in);
        VDB_CHECK(sort.Metrics().intermediate_merge_passes > 0);
        const auto extent = q.Temps().Metrics().allocated_temp_pages;
        Oracle(sort.OutputSchema(), 2, rows, Drain(db, sort));
        VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == extent);
        VDB_CHECK(state.inits == 1 && state.delivered == rows.size());
        Bounds(sort); q.FinishQuery(sort); Clean(db, files); db.Close();
    }
}

void WideRowsAndNullablePayload() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    {
        QueryContext q(db, {49152, 16384});
        const auto schema = WideSchema();
        std::vector<Tuple> rows;
        for (int64_t id = 24; id >= 0; --id) {
            std::vector<Value> values;
            for (int i = 0; i < 7; ++i) values.emplace_back(std::string(256, char('a' + id % 26)));
            for (int i = 0; i < 8; ++i) values.emplace_back(id);
            values.emplace_back(std::string(172, '\0'));
            rows.emplace_back(std::move(values));
        }
        SourceState state;
        ExternalSort sort(q, {7}, std::make_unique<Source>(q, schema, rows, state));
        VDB_CHECK(sort.Memory().run_capacity == 4 && sort.Memory().merge_fan_in == 2);
        sort.Init(); Oracle(schema, 7, rows, Drain(db, sort), true);
        VDB_CHECK(sort.Metrics().initial_runs == 7 && sort.Metrics().intermediate_merge_passes == 2);
        Bounds(sort); Report("wide_rows", sort, q);
        q.FinishQuery(sort); Clean(db, files);
    }
    {
        QueryContext q(db);
        const auto schema = IntermediateSchema({{"key", TypeId::Int64},
            {"nullable", TypeId::Varchar, 256, true}, {"real", TypeId::Double, 0, true}});
        std::vector<Tuple> rows;
        for (int64_t id = 99; id >= 0; --id)
            rows.emplace_back(std::vector<Value>{Value(id % 3),
                id % 2 == 0 ? Value::Null(TypeId::Varchar) : Value(std::string("a\0b", 3)),
                id % 2 == 0 ? Value::Null(TypeId::Double) : Value(double(id) / 7)});
        SourceState state;
        ExternalSort sort(q, {0, 24576}, std::make_unique<Source>(q, schema, rows, state));
        sort.Init(); Oracle(schema, 0, rows, Drain(db, sort));
        VDB_CHECK(sort.Metrics().final_merge_streamed);
        Bounds(sort); q.FinishQuery(sort); Clean(db, files);
    }
    db.Close();
}

void PipelineAndPayloads() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("input", PayloadSchema());
    std::vector<Tuple> source;
    for (int64_t i = 0; i < 180; ++i) {
        const int64_t key = i < 100 ? 5 : i == 100 ? INT64_MIN : i == 101 ? INT64_MAX : i - 140;
        source.push_back(Row(i, key, std::string("a\0b", 3) + std::to_string(i)));
        db.Insert("input", source.back());
    }
    QueryContext q(db, {24576, 16384});
    auto projection = std::make_unique<Projection>(q, ProjectionPlan{{1, 2, 0}},
        std::make_unique<SeqScan>(q, SeqScanPlan{"input", true}));
    ExternalSort sort(q, {1}, std::move(projection));
    VDB_CHECK(sort.OutputOrdering() == (Ordering{true, 1}));
    VDB_CHECK(sort.OutputSchema().column(0).name == "payload");
    std::vector<Tuple> expected;
    for (const auto& row : source) expected.emplace_back(std::vector<Value>{row.value(1), row.value(2), row.value(0)});
    sort.Init(); Oracle(sort.OutputSchema(), 1, expected, Drain(db, sort));
    Bounds(sort); q.FinishQuery(sort); Clean(db, files); db.Close();
}
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"memory_validation", MemoryValidation}, {"private_boundaries_pending", PrivateAndBoundaries},
        {"multiple_passes_runs", MultiplePasses}, {"multiway_unique", MultiwayAndUnique},
        {"fanin_exceeds_frames", FanInExceedsFrames}, {"fanin_descriptor_errors", FanInAndDescriptorErrors},
        {"replay_early_close", ReplayAndEarlyClose}, {"cumulative_temp_limit", CumulativeTempLimit},
        {"temp_limit_during_merge", TempLimitDuringMerge}, {"failure_cleanup", FailureCleanup},
        {"randomized_oracle", RandomizedOracle}, {"wide_nullable_payload", WideRowsAndNullablePayload},
        {"pipeline_payloads", PipelineAndPayloads}};
    try {
        for (const auto& [name, run] : tests) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            run(); ++cases_passed; std::cout << "PASS " << name << '\n';
        }
        VDB_CHECK(cases_passed != 0);
        std::cout << cases_passed << " external sort groups passed; peak pins=" << maximum_pins
                  << ", all relevant boundaries active pins=0\n";
    } catch (const std::exception& error) {
        std::cerr << "external sort failure after " << cases_passed << " groups: " << error.what() << '\n';
        return 1;
    }
}
