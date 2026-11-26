#include "test_harness.h"
#include "valencedb/common/error.hpp"
#include "valencedb/execution/aggregate.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/execution/grace_hash_join.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <random>

using namespace valencedb;

namespace valencedb {
struct AggregateTestAccess {
    static void HashPhase(const HashAggregate& a) {
        if (a.Metrics().used_internal_sort) {
            VDB_CHECK(!a.table_ && a.sort_);
            VDB_CHECK(a.sort_->Memory().max_bytes == a.Memory().sort_memory_bytes);
        } else if (a.spec_.group_column) {
            VDB_CHECK(a.table_ && !a.sort_);
            VDB_CHECK(a.table_->size() <= a.Memory().hash_group_capacity);
        } else VDB_CHECK(!a.table_ && !a.sort_);
    }
    static void Released(const HashAggregate& a) {
        VDB_CHECK(!a.table_ && !a.sort_ && !a.lookahead_);
    }
    static void Released(const SortAggregate& a) { VDB_CHECK(!a.lookahead_); }
    static void Partials(HashAggregate& a) {
        a.child_->Init();
        const auto partials = a.BuildGroups();
        VDB_CHECK(partials && partials->RowCount() == a.Metrics().spill_records);
        VDB_CHECK(!a.table_ && !a.sort_);
        VDB_CHECK(&partials->OutputSchema() == &a.partial_schema_);
        TempReader reader(*partials);
        Tuple row({});
        size_t count = 0;
        while (reader.Next(row)) {
            VDB_CHECK(row.size() == 6);
            for (size_t c = 0; c < 6; ++c) VDB_CHECK(row.value(c).type() == TypeId::Int64);
            if (!a.spec_.count && !a.spec_.avg) VDB_CHECK(row.value(1).as_int64() == 0);
            if (!a.spec_.sum && !a.spec_.avg) VDB_CHECK(row.value(2).as_int64() == 0);
            if (!a.spec_.min) VDB_CHECK(row.value(3).as_int64() == 0);
            if (!a.spec_.max) VDB_CHECK(row.value(4).as_int64() == 0);
            VDB_CHECK(row.value(5).as_int64() == (a.spec_.sum || a.spec_.min || a.spec_.max || a.spec_.avg));
            ++count;
        }
        VDB_CHECK(count == a.Metrics().spill_records);
    }
};
}

namespace {
constexpr size_t kMemory = 32768;
constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
uint64_t maximum_pins = 0;
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static size_t serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_aggregate_" +
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
    try { action(); } catch (const DbError& e) {
        caught = true;
        VDB_CHECK(std::string(e.what()).find(message) != std::string::npos);
    }
    VDB_CHECK(caught);
}
void Pins(Database& db) {
    VDB_CHECK(db.Metrics().active_pins == 0);
    maximum_pins = std::max(maximum_pins, db.Metrics().peak_pins);
    VDB_CHECK(db.Metrics().peak_pins <= 1);
}
void Clean(Database& db) {
    Pins(db);
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 0);
    for (const auto& frame : db.Buffer().InspectFrames())
        VDB_CHECK(!frame.page || frame.page->file != FileKind::Temporary);
}
Schema InputSchema() { return Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"value", TypeId::Int64}}); }
Tuple Row(int64_t key, int64_t value = 1) { return Tuple({Value(key), Value(value)}); }
AggregateSpec All(bool grouped = true) {
    return {grouped ? std::optional<size_t>(0) : std::nullopt, 1, true, true, true, true, true};
}
std::vector<AggregateSpec> Specs() {
    return {{0, {}, true}, {0, 1, true}, {0, 1, false, true}, {0, 1, false, false, true},
        {0, 1, false, false, false, true}, {0, 1, false, false, false, false, true},
        {0, 1, true, true}, {0, 1, true, false, true, true},
        {0, 1, false, true, true, true}, {0, 1, true, true, false, false, true}, All()};
}
struct SourceState {
    size_t inits = 0, delivered = 0, calls = 0, closes = 0, allowed = 1;
    size_t fail_at = std::numeric_limits<size_t>::max();
    bool open = false, fail_init = false;
};
class Source final : public Executor {
public:
    Source(QueryContext& q, Schema schema, const std::vector<Tuple>& rows, SourceState& state,
           Ordering order = {})
        : Executor(q), schema_(std::move(schema)), rows_(rows), state_(state), order_(order) {}
    ~Source() noexcept override { Close(); }
    void DoInit() override {
        state_.open = true;
        pos_ = 0;
        if (++state_.inits > state_.allowed || state_.fail_init) throw DbError("child replay or Init failure");
    }
    bool DoNext(Tuple& out) override {
        Pins(context_.Db());
        ++state_.calls;
        if (pos_ == state_.fail_at) throw DbError("child Next failure");
        if (pos_ == rows_.size()) return false;
        out = rows_[pos_++];
        ++state_.delivered;
        return true;
    }
    void DoClose() noexcept override {
        if (state_.open) ++state_.closes;
        state_.open = false;
    }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return order_; }
private:
    Schema schema_;
    const std::vector<Tuple>& rows_;
    SourceState& state_;
    Ordering order_;
    size_t pos_ = 0;
};
std::unique_ptr<Executor> MakeSource(QueryContext& q, const std::vector<Tuple>& rows,
                                     SourceState& state, Ordering order = {}) {
    return std::make_unique<Source>(q, InputSchema(), rows, state, order);
}
using Bag = std::vector<std::vector<std::byte>>;
Bag Canonical(const Schema& schema, const std::vector<Tuple>& rows) {
    Bag result;
    for (const auto& row : rows) result.push_back(EncodeTuple(schema, row));
    std::sort(result.begin(), result.end());
    return result;
}
// Independent oracle retains test values, then reduces each requested output separately.
std::vector<Tuple> Reference(const std::vector<Tuple>& rows, const AggregateSpec& s) {
    std::map<int64_t, std::vector<int64_t>> groups;
    if (!s.group_column) groups[0] = {};
    for (const auto& row : rows) {
        const auto key = s.group_column ? row.value(*s.group_column).as_int64() : 0;
        groups[key].push_back(s.measure_column ? row.value(*s.measure_column).as_int64() : 0);
    }
    std::vector<Tuple> result;
    for (const auto& [key, values] : groups) {
        int64_t sum = 0;
        if (s.sum || s.avg) for (auto v : values) {
            VDB_CHECK(!((v > 0 && sum > kMax - v) || (v < 0 && sum < kMin - v)));
            sum += v;
        }
        std::vector<Value> output;
        if (s.group_column) output.emplace_back(key);
        if (s.count) output.emplace_back(static_cast<int64_t>(values.size()));
        if (s.sum) output.push_back(values.empty() ? Value::Null(TypeId::Int64) : Value(sum));
        if (s.min) output.push_back(values.empty() ? Value::Null(TypeId::Int64) : Value(*std::min_element(values.begin(), values.end())));
        if (s.max) output.push_back(values.empty() ? Value::Null(TypeId::Int64) : Value(*std::max_element(values.begin(), values.end())));
        if (s.avg) output.push_back(values.empty() ? Value::Null(TypeId::Double) : Value(static_cast<double>(sum) / values.size()));
        result.emplace_back(std::move(output));
    }
    return result;
}
std::vector<Tuple> Drain(Executor& a) {
    std::vector<Tuple> rows;
    Tuple out({});
    while (a.Next(out)) {
        Pins(a.Context().Db());
        rows.push_back(out);
    }
    auto last = out;
    VDB_CHECK(!a.Next(out) && a.Exhausted());
    if (!rows.empty()) VDB_CHECK(EncodeTuple(a.OutputSchema(), last) == EncodeTuple(a.OutputSchema(), out));
    return rows;
}
void CheckResult(Executor& a, const std::vector<Tuple>& got, const std::vector<Tuple>& input,
                 const AggregateSpec& spec) {
    VDB_CHECK(Canonical(a.OutputSchema(), got) == Canonical(a.OutputSchema(), Reference(input, spec)));
    if (a.OutputOrdering().ordered) {
        for (size_t i = 1; i < got.size(); ++i)
            VDB_CHECK(got[i-1].value(0).as_int64() < got[i].value(0).as_int64());
    }
}
AggregateMetrics Run(Database& db, const std::vector<Tuple>& rows, AggregateSpec spec,
                     bool hash, Ordering order = {}) {
    QueryContext q(db, {kMemory, 16384});
    SourceState source;
    std::unique_ptr<Executor> a;
    if (hash) a = std::make_unique<HashAggregate>(q, spec, MakeSource(q, rows, source, order));
    else a = std::make_unique<SortAggregate>(q, spec, MakeSource(q, rows, source, order));
    VDB_CHECK(a->OutputSchema().size() <= 6 && a->OutputSchema().max_encoded_bytes() <= 52);
    a->Init();
    if (hash) AggregateTestAccess::HashPhase(static_cast<HashAggregate&>(*a));
    auto result = Drain(*a);
    CheckResult(*a, result, rows, spec);
    const auto metrics = hash ? static_cast<HashAggregate&>(*a).Metrics() : static_cast<SortAggregate&>(*a).Metrics();
    VDB_CHECK(metrics.input_rows == rows.size() && metrics.output_groups == result.size());
    VDB_CHECK(source.inits == 1 && source.delivered == rows.size() && source.calls == rows.size() + 1);
    VDB_CHECK(a->OutputOrdering() == (hash || !spec.group_column ? Ordering{} : Ordering{true, 0}));
    if (hash) {
        auto& h = static_cast<HashAggregate&>(*a);
        VDB_CHECK(metrics.peak_hash_groups <= h.Memory().hash_group_capacity);
        VDB_CHECK(metrics.modeled_peak_hash_bytes <= h.Memory().max_bytes);
    }
    if (!spec.group_column) {
        VDB_CHECK(metrics.peak_hash_groups == 0 && !metrics.used_internal_sort);
        VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
    }
    q.FinishQuery(*a);
    VDB_CHECK(q.Succeeded() && source.closes == 1 && !source.open);
    Clean(db);
    return metrics;
}

void Semantics() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> rows{Row(kMin, -8), Row(4, 10), Row(4, -4), Row(kMax, 3), Row(kMax, 5)};
    for (auto spec : Specs()) for (bool grouped : {false, true}) {
        if (!grouped) spec.group_column.reset();
        for (bool hash : {true, false}) {
            Run(db, {}, spec, hash);
            Run(db, rows, spec, hash);
        }
    }
    for (bool hash : {true, false}) {
        Run(db, {Row(0, kMin), Row(1, kMax)}, All(), hash);
        Run(db, {Row(0, kMin), Row(0, kMax)}, All(false), hash); // sum=-1, no overflow
    }
    // COUNT(*) can count rows whose payload is not an INT64 measure.
    QueryContext q(db);
    std::vector<Tuple> strings{Tuple({Value(std::string("x"))}), Tuple({Value(std::string("y"))})};
    SourceState state;
    HashAggregate a(q, {{}, {}, true}, std::make_unique<Source>(q,
        Schema(SchemaKind::Base, {{"payload", TypeId::Varchar, 256}}), strings, state));
    a.Init();
    auto got = Drain(a);
    VDB_CHECK(got.size() == 1 && got[0].value(0).as_int64() == 2);
    q.FinishQuery(a);
    Clean(db);
}

void Capacity() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (int offset : {-1, 0, 1}) {
        QueryContext q(db, {kMemory, 16384});
        std::vector<Tuple> rows;
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state));
        const size_t cap = a.Memory().hash_group_capacity;
        VDB_CHECK(cap == 117 && a.Memory().scratch_bytes == 10296 && a.Memory().sort_memory_bytes == 26440);
        const size_t groups = static_cast<size_t>(static_cast<int>(cap) + offset);
        for (size_t i = 0; i < groups; ++i) rows.push_back(Row(static_cast<int64_t>(i), 2));
        if (offset == 0) for (size_t i = 0; i < 2000; ++i) rows.push_back(Row(static_cast<int64_t>(i % cap), -1));
        a.Init();
        AggregateTestAccess::HashPhase(a);
        auto got = Drain(a);
        CheckResult(a, got, rows, All());
        VDB_CHECK(a.Metrics().peak_hash_groups == std::min(cap, groups));
        VDB_CHECK(a.Metrics().spill_batches == (offset > 0 ? 2 : 0));
        VDB_CHECK(a.Metrics().overflow_flushes == (offset > 0 ? 1 : 0));
        VDB_CHECK(a.Metrics().spill_records == (offset > 0 ? groups : 0));
        VDB_CHECK(a.Metrics().used_internal_sort == (offset > 0));
        VDB_CHECK(state.inits == 1 && state.delivered == rows.size());
        if (offset <= 0) VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
        q.FinishQuery(a);
        Clean(db);
    }
    std::vector<Tuple> many;
    for (size_t i = 0; i < 20000; ++i) many.push_back(Row(static_cast<int64_t>(i % 2), -3));
    VDB_CHECK(Run(db, many, All(), true).spill_batches == 0);
    Run(db, many, All(false), true);
    Run(db, many, All(false), false);
}

void MultiSpill() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {kMemory, 16384});
    std::vector<Tuple> rows;
    SourceState source;
    HashAggregate a(q, All(), MakeSource(q, rows, source));
    const auto cap = a.Memory().hash_group_capacity;
    rows = {Row(7, 10), Row(7, 20), Row(8, 4)};
    for (size_t i = 0; i < cap - 2; ++i) rows.push_back(Row(1000 + static_cast<int64_t>(i)));
    for (int batch : {2, 3}) {
        rows.push_back(Row(batch * 1000)); // New key at a full map: retain exactly once.
        rows.push_back(Row(8, batch == 2 ? -400 : 900));
        for (size_t i = 1; i <= cap - 2; ++i) rows.push_back(Row(batch * 1000 + static_cast<int64_t>(i)));
    }
    rows.push_back(Row(4000)); // Third overflow, also must appear exactly once.
    rows.push_back(Row(8, 5));
    for (int i = 0; i < 8; ++i) rows.push_back(Row(7, 100));
    rows.push_back(Row(1000, -2));
    a.Init();
    AggregateTestAccess::HashPhase(a);
    VDB_CHECK(a.Metrics().overflow_flushes == 3 && a.Metrics().spill_batches == 4);
    VDB_CHECK(a.Metrics().spill_records == 3 * cap + 4);
    VDB_CHECK(a.Metrics().sort.initial_runs > 2 && a.Metrics().sort.intermediate_merge_passes > 1);
    VDB_CHECK(a.Metrics().sort.final_merge_streamed);
    VDB_CHECK(source.inits == 1 && source.calls == rows.size() + 1 && source.closes == 1);
    auto got = Drain(a);
    CheckResult(a, got, rows, All());
    for (const auto& row : got) {
        const auto key = row.value(0).as_int64();
        if (key == 7) {
            VDB_CHECK(row.value(1).as_int64() == 10 && row.value(2).as_int64() == 830);
            VDB_CHECK(row.value(5).as_double() == 83.0);
        }
        if (key == 8) VDB_CHECK(row.value(3).as_int64() == -400 && row.value(4).as_int64() == 900);
        if (key == 2000 || key == 3000 || key == 4000) VDB_CHECK(row.value(1).as_int64() == 1);
    }
    const auto& m = a.Metrics();
    std::cout << "spill evidence M=" << a.Memory().max_bytes << " S=" << a.Memory().scratch_bytes
        << " cap=" << cap << " peak=" << m.peak_hash_groups << " modeled=" << m.modeled_peak_hash_bytes
        << " overflow=" << m.overflow_flushes << " batches=" << m.spill_batches << " records=" << m.spill_records
        << " groups=" << m.output_groups << " runs=" << m.sort.initial_runs
        << " passes=" << m.sort.intermediate_merge_passes << " temp=" << q.Temps().Metrics().allocated_temp_pages
        << " avg=83\n";
    q.FinishQuery(a);
    AggregateTestAccess::Released(a);
    Clean(db);
}

// Inspect the sealed BuildGroups result directly, without retaining it in the executor.
void RawPartials() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (auto spec : Specs()) {
        QueryContext q(db, {kMemory, 16384});
        std::vector<Tuple> rows;
        for (int64_t i = 0; i < 600; ++i) rows.push_back(Row(i % 200, i % 17));
        SourceState source;
        HashAggregate a(q, spec, MakeSource(q, rows, source));
        AggregateTestAccess::Partials(a);
        VDB_CHECK(a.Metrics().spill_batches > 2);
        VDB_CHECK(source.inits == 1 && source.delivered == rows.size() && source.closes == 1);
        a.Close();
        q.Close(); // No writer or reader survived BuildGroups / the inspection scope.
        Clean(db);
    }
}

void Overflow() {
    AggregateSpec count{{}, {}, true}, sum{{}, 1, false, true}, avg{{}, 1, false, false, false, false, true};
    AggregateState s;
    s.count = kMax;
    Throws([&] { s.Update(0, count); }, "overflow");
    Throws([&] { s.Combine({1, 0, 0, 0, false}, count); }, "overflow");
    for (auto limit : {kMin, kMax}) {
        const int64_t increment = limit == kMin ? -1 : 1;
        s = {0, limit, 0, 0, true};
        Throws([&] { s.Update(increment, sum); }, "overflow");
        Throws([&] { s.Combine({0, increment, 0, 0, true}, sum); }, "overflow");
    }
    s = {kMax, 0, 0, 0, true};
    Throws([&] { s.Combine({1, 0, 0, 0, true}, avg); }, "overflow");
    // Identity and selected-field semantics of the raw combinable state.
    s = {};
    const AggregateSpec extrema{{}, 1, false, false, true, true};
    s.Combine({kMax, kMax, -7, 9, true}, extrema);
    s.Combine({}, extrema);
    VDB_CHECK(s.count == 0 && s.sum == 0 && s.min == -7 && s.max == 9 && s.has_value);

    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (bool hash : {true, false}) for (bool grouped : {false, true}) {
        auto c = count;
        auto minimum = AggregateSpec{{}, 1, false, false, true};
        auto maximum = AggregateSpec{{}, 1, false, false, false, true};
        if (grouped) { c.group_column = 0; minimum.group_column = 0; maximum.group_column = 0; }
        Run(db, {Row(0, kMax), Row(0, kMax)}, c, hash);
        Run(db, {Row(0, kMax), Row(0, kMax)}, minimum, hash);
        Run(db, {Row(0, kMax), Row(0, kMax)}, maximum, hash);
        for (auto spec : {sum, avg}) {
            if (grouped) spec.group_column = 0;
            for (int64_t limit : {kMin, kMax}) {
                QueryContext q(db);
                std::vector<Tuple> rows{Row(0, limit), Row(0, limit == kMin ? -1 : 1)};
                SourceState state;
                std::unique_ptr<Executor> a;
                if (hash) a = std::make_unique<HashAggregate>(q, spec, MakeSource(q, rows, state));
                else a = std::make_unique<SortAggregate>(q, spec, MakeSource(q, rows, state));
                Throws([&] { a->Init(); Drain(*a); }, "overflow");
                VDB_CHECK(!a->Exhausted() && !state.open);
                q.Close();
                Clean(db);
            }
        }
    }
    for (int64_t limit : {kMin, kMax}) {
        QueryContext q(db, {kMemory, 16384});
        std::vector<Tuple> rows{Row(0, limit)};
        SourceState state;
        auto spec = sum;
        spec.group_column = 0;
        HashAggregate a(q, spec, MakeSource(q, rows, state));
        for (size_t i = 1; i <= a.Memory().hash_group_capacity; ++i) rows.push_back(Row(static_cast<int64_t>(i), 0));
        rows.push_back(Row(0, limit == kMin ? -1 : 1));
        a.Init(); // Each partial sum is valid; only recombination overflows.
        VDB_CHECK(a.Metrics().spill_batches == 2 && a.Metrics().used_internal_sort);
        Throws([&] { Drain(a); }, "overflow");
        VDB_CHECK(!a.Exhausted() && state.inits == 1);
        AggregateTestAccess::Released(a);
        q.Close();
        Clean(db);
    }
    // Unused SUM stays zero in real spilled partial records, too.
    std::vector<Tuple> huge;
    for (size_t i = 0; i < 400; ++i) huge.push_back(Row(static_cast<int64_t>(i % 180), kMax));
    for (auto spec : {AggregateSpec{0, {}, true}, AggregateSpec{0, 1, false, false, true},
                     AggregateSpec{0, 1, false, false, false, true}})
        VDB_CHECK(Run(db, huge, spec, true).spill_batches > 1);
}

void Validation() {
    Files files, foreign_files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Database foreign_db(foreign_files.db, OpenMode::CreateNew, foreign_files.temp, 8);
    QueryContext q(db), foreign(foreign_db);
    std::vector<Tuple> rows{Row(0)};
    SourceState state;
    auto reject = [&](Schema schema, AggregateSpec spec, size_t memory = kMemory) {
        for (bool hash : {true, false}) Throws([&] {
            auto child = std::make_unique<Source>(q, schema, rows, state);
            if (hash) { HashAggregate a(q, spec, std::move(child), memory); }
            else { SortAggregate a(q, spec, std::move(child), memory); }
        });
        VDB_CHECK(state.inits == 0 && state.delivered == 0);
    };
    reject(InputSchema(), {});
    reject(InputSchema(), {2, {}, true});
    reject(InputSchema(), {0, 2, true});
    reject(InputSchema(), {0, {}, false, true});
    reject(InputSchema(), All(), 0);
    reject(InputSchema(), All(), 1);
    reject(InputSchema(), All(false), 1);
    for (TypeId type : {TypeId::Varchar, TypeId::Double, TypeId::Int64}) {
        const Column invalid{"invalid", type, static_cast<uint16_t>(type == TypeId::Varchar ? 256 : 0), type == TypeId::Int64};
        reject(IntermediateSchema({invalid, {"m", TypeId::Int64}}), All());
        reject(IntermediateSchema({{"g", TypeId::Int64}, invalid}), All());
        reject(IntermediateSchema({{"g", TypeId::Int64}, invalid}), {0, 1, true});
    }
    Throws([&] { HashAggregate a(q, All(), nullptr); });
    Throws([&] { SortAggregate a(q, All(), nullptr); });
    Throws([&] { HashAggregate a(q, All(), MakeSource(foreign, rows, state)); });
    Throws([&] { SortAggregate a(q, All(), MakeSource(foreign, rows, state)); });
    for (size_t bytes : {size_t{10296}, size_t{10487}})
        Throws([&] { HashAggregate a(q, All(), MakeSource(q, rows, state), bytes); });
    Throws([&] { SortAggregate a(q, All(), MakeSource(q, rows, state), 11000); });
    // Empty global results are nullable: sorting a SUM/MIN/MAX key must reject up front.
    for (size_t column : {size_t{1}, size_t{2}, size_t{3}, size_t{4}}) Throws([&] {
        ExternalSort sort(q, {column}, std::make_unique<HashAggregate>(q, All(false), MakeSource(q, rows, state)));
    });
    HashAggregate maximum(q, All(), MakeSource(q, rows, state));
    VDB_CHECK(maximum.OutputSchema().size() == 6 && maximum.OutputSchema().max_encoded_bytes() == 52);
    for (size_t i = 0; i < 6; ++i) VDB_CHECK(!maximum.OutputSchema().column(i).nullable);
    const std::vector<std::string> names{"group", "count", "sum", "min", "max", "avg"};
    for (size_t i = 0; i < names.size(); ++i) VDB_CHECK(maximum.OutputSchema().column(i).name == names[i]);
    VDB_CHECK(state.inits == 0);
    Throws([] { QueryResources{1, 0}.Validate(); });
    Throws([] { QueryResources{1, size_t{kInvalidPageId} + 1}.Validate(); });
    Throws([] { CheckedAdd(std::numeric_limits<size_t>::max(), 1); });
    Throws([] { FixedScratchCharge(std::numeric_limits<size_t>::max()); });
    q.Close();
    foreign.Close();
    Clean(db);
}

void CapacityOne() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (bool spill : {false, true}) {
        QueryContext q(db);
        std::vector<Tuple> rows(20, Row(7));
        if (spill) rows.push_back(Row(8));
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state), 10488);
        VDB_CHECK(a.Memory().hash_group_capacity == 1);
        if (spill) {
            Throws([&] { a.Init(); }, "sort memory");
            VDB_CHECK(a.Metrics().spill_batches == 2 && state.inits == 1 && state.delivered == rows.size());
            AggregateTestAccess::Released(a);
            VDB_CHECK(!a.Exhausted());
            q.Close();
        } else {
            a.Init();
            CheckResult(a, Drain(a), rows, All());
            VDB_CHECK(a.Metrics().spill_batches == 0 && q.Temps().Metrics().allocated_temp_pages == 0);
            q.FinishQuery(a);
        }
        Clean(db);
    }
}

void OrderedSort() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("ordered", InputSchema());
    std::vector<Tuple> rows{Row(10, 1), Row(20, 1), Row(-7, 2), Row(8, 2), Row(3, 2), Row(2, 3), Row(9, 4), Row(6, 4)};
    for (const auto& row : rows) db.Insert("ordered", row);
    db.CreateIndex("ordered", 1); // Nonzero input column becomes group output column zero.
    QueryContext q(db);
    auto spec = All();
    spec.group_column = 1;
    spec.measure_column = 0;
    SortAggregate a(q, spec, std::make_unique<IndexScan>(q, IndexScanPlan{"ordered", 1}), 2104);
    a.Init();
    CheckResult(a, Drain(a), rows, spec);
    VDB_CHECK(a.Metrics().input_order_reused && !a.Metrics().used_internal_sort);
    VDB_CHECK(a.Metrics().sort.initial_runs == 0 && a.Metrics().output_groups == 4);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 0);
    std::cout << "IndexScan -> SortAggregate groups=4 sort_runs=0 temp=0 ordering_column=0\n";
    q.FinishQuery(a);
    Clean(db);

    for (auto sequence : {std::vector<Tuple>{Row(1, 3), Row(1, 5), Row(2, 7)},
                          std::vector<Tuple>(100, Row(1, -4))}) {
        QueryContext stream(db);
        SourceState state;
        SortAggregate b(stream, All(), MakeSource(stream, sequence, state, {true, 0}), 2104);
        b.Init();
        VDB_CHECK(state.delivered == 0); // Ordered Init does not preconsume/materialize output.
        Tuple first({});
        VDB_CHECK(b.Next(first));
        VDB_CHECK(state.delivered == sequence.size());
        std::vector<Tuple> got{first};
        auto rest = Drain(b);
        got.insert(got.end(), rest.begin(), rest.end());
        CheckResult(b, got, sequence, All());
        VDB_CHECK(state.delivered == sequence.size() && state.inits == 1);
        stream.FinishQuery(b);
        Clean(db);
    }
}

void UnorderedSort() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("unordered", InputSchema());
    std::vector<Tuple> rows;
    for (int64_t i = 0; i < 1200; ++i) rows.push_back(Row(i % 300 - 150, i % 17 - 8));
    std::mt19937 rng(1010);
    std::shuffle(rows.begin(), rows.end(), rng);
    for (const auto& row : rows) db.Insert("unordered", row);
    QueryContext q(db);
    SortAggregate a(q, All(), std::make_unique<SeqScan>(q, SeqScanPlan{"unordered", true}), kMemory);
    a.Init();
    CheckResult(a, Drain(a), rows, All());
    const auto& m = a.Metrics();
    VDB_CHECK(m.used_internal_sort && !m.input_order_reused);
    VDB_CHECK(m.sort.initial_runs > 2 && m.sort.intermediate_merge_passes >= 1 && m.sort.final_merge_streamed);
    VDB_CHECK(m.output_groups == 300);
    std::cout << "SeqScan -> SortAggregate M=" << a.Memory().max_bytes << " Msort=" << a.Memory().sort_memory_bytes
        << " runs=" << m.sort.initial_runs << " passes=" << m.sort.intermediate_merge_passes
        << " groups=" << m.output_groups << " temp=" << q.Temps().Metrics().allocated_temp_pages << '\n';
    q.FinishQuery(a);
    Clean(db);
    VDB_CHECK(Run(db, {Row(2), Row(1)}, All(), false, {true, 1}).used_internal_sort);
}

void Lifecycle() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (int path = 0; path < 4; ++path) {
        QueryContext q(db, {kMemory, 16384});
        std::vector<Tuple> rows;
        for (int64_t i = 0; i < (path == 0 ? 10 : 300); ++i) rows.push_back(Row(i, i % 13));
        SourceState state;
        state.allowed = 3;
        const bool hash = path < 2;
        std::unique_ptr<Executor> a;
        if (hash) a = std::make_unique<HashAggregate>(q, All(), MakeSource(q, rows, state));
        else a = std::make_unique<SortAggregate>(q, All(), MakeSource(q, rows, state, path == 2 ? Ordering{true, 0} : Ordering{}));
        Tuple out({});
        Throws([&] { a->Next(out); });
        a->Close();
        size_t previous_extent = 0;
        for (int execution = 0; execution < 3; ++execution) {
            a->Init();
            Throws([&] { a->Init(); });
            if (execution == 0) {
                VDB_CHECK(a->Next(out));
                Throws([&] { q.FinishQuery(*a); });
            } else CheckResult(*a, Drain(*a), rows, All());
            const auto saved = EncodeTuple(a->OutputSchema(), out);
            a->Close();
            a->Close();
            VDB_CHECK(EncodeTuple(a->OutputSchema(), out) == saved && !state.open);
            if (hash) AggregateTestAccess::Released(static_cast<HashAggregate&>(*a));
            else AggregateTestAccess::Released(static_cast<SortAggregate&>(*a));
            const auto extent = q.Temps().Metrics().allocated_temp_pages;
            if (path == 1 || path == 3) VDB_CHECK(extent > previous_extent);
            else VDB_CHECK(extent == 0);
            VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == extent);
            previous_extent = extent;
            Pins(db);
            Throws([&] { a->Next(out); });
        }
        VDB_CHECK(state.inits == 3 && state.closes == 3);
        if (path != 2) VDB_CHECK(state.delivered == 3 * rows.size());
        q.FinishQuery(*a);
        Clean(db);
    }
}

void TempLimits() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    std::vector<Tuple> rows;
    for (int64_t i = 0; i < 355; ++i) rows.push_back(Row(i));
    for (size_t cap : {size_t{1}, size_t{6}}) {
        QueryContext q(db, {kMemory, cap});
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state));
        Throws([&] { a.Init(); }, "temporary page cap");
        VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == cap && !a.Exhausted() && !state.open);
        VDB_CHECK(a.Metrics().used_internal_sort == (cap == 6));
        AggregateTestAccess::Released(a);
        std::cout << "temp exhaustion cap=" << cap << " extent=" << db.Disk().PageCount(FileKind::Temporary)
            << " during_sort=" << a.Metrics().used_internal_sort << '\n';
        q.Close();
        Clean(db);
    }
    rows.erase(rows.begin() + 118, rows.end());
    size_t extent = 0;
    {
        QueryContext q(db, {kMemory, 16384});
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state));
        a.Init();
        Drain(a);
        extent = q.Temps().Metrics().allocated_temp_pages;
        q.FinishQuery(a);
    }
    QueryContext q(db, {kMemory, extent + 1});
    SourceState state;
    state.allowed = 2;
    HashAggregate a(q, All(), MakeSource(q, rows, state));
    a.Init();
    Drain(a);
    a.Close();
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == extent);
    Throws([&] { a.Init(); }, "temporary page cap");
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == extent + 1);
    VDB_CHECK(state.inits == 2 && !state.open && !a.Exhausted());
    std::cout << "append-only replay extent=" << extent << " -> " << extent + 1 << " then DbError\n";
    q.Close();
    Clean(db);
}

void Failures() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    std::vector<Tuple> rows;
    for (int64_t i = 0; i < 592; ++i) rows.push_back(Row(i));
    for (int path = 0; path < 3; ++path) for (bool init_failure : {false, true}) {
        QueryContext q(db, {kMemory, 16384});
        SourceState state;
        state.fail_init = init_failure;
        state.fail_at = 250;
        std::unique_ptr<Executor> a;
        if (path == 0) a = std::make_unique<HashAggregate>(q, All(), MakeSource(q, rows, state));
        else a = std::make_unique<SortAggregate>(q, All(), MakeSource(q, rows, state, path == 1 ? Ordering{true, 0} : Ordering{}));
        Throws([&] { a->Init(); Drain(*a); }, "child");
        VDB_CHECK(!a->Exhausted() && !state.open && !q.Succeeded());
        q.Close();
        Clean(db);
    }
    {
        QueryContext q(db, {kMemory, 16384});
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state));
        db.Disk().FailNextIo(FailNext::Write);
        Throws([&] { a.Init(); }, "write");
        AggregateTestAccess::Released(a);
        VDB_CHECK(!a.Exhausted() && !state.open);
        q.Close();
        Clean(db);
    }
    {
        QueryContext q(db, {kMemory, 16384});
        SourceState state;
        HashAggregate a(q, All(), MakeSource(q, rows, state));
        a.Init();
        Tuple out({});
        VDB_CHECK(a.Next(out));
        // Evict cached pages through normal BufferPool reads; do not discard dirty scratch.
        for (PageId p = 0; p < db.Disk().PageCount(FileKind::Temporary); ++p)
            db.Buffer().Invalidate({FileKind::Temporary, p});
        db.Disk().FailNextIo(FailNext::Read);
        Throws([&] { Drain(a); }, "read");
        AggregateTestAccess::Released(a);
        VDB_CHECK(!a.Exhausted() && !q.Succeeded());
        q.Close();
        Clean(db);
    }
    { // Destructor after early output releases children/handles just like explicit Close.
        QueryContext q(db, {kMemory, 16384});
        SourceState state;
        {
            HashAggregate a(q, All(), MakeSource(q, rows, state));
            a.Init();
            Tuple out({});
            VDB_CHECK(a.Next(out));
        }
        VDB_CHECK(!state.open);
        q.Close();
        Clean(db);
    }
}

void Integration() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    std::vector<Tuple> left, right, joined;
    for (int64_t key = -150; key < 150; ++key) {
        left.push_back(Row(key, key % 11));
        right.push_back(Row(key, 2));
        right.push_back(Row(key, 3));
        joined.emplace_back(std::vector<Value>{Value(key), Value(key % 11), Value(key), Value(int64_t{2})});
        joined.emplace_back(std::vector<Value>{Value(key), Value(key % 11), Value(key), Value(int64_t{3})});
    }
    for (bool final_sort : {false, true}) {
        QueryContext q(db, {1024 * 1024, 16384});
        SourceState l, r;
        auto join = std::make_unique<GraceHashJoin>(q, GraceHashJoinPlan{0, 0, 65536},
            MakeSource(q, left, l), MakeSource(q, right, r));
        auto* gh = join.get();
        auto aggregate = std::make_unique<HashAggregate>(q, All(), std::move(join), kMemory);
        auto* ha = aggregate.get();
        VDB_CHECK(!ha->OutputOrdering().ordered);
        std::unique_ptr<Executor> root;
        if (final_sort) root = std::make_unique<ExternalSort>(q, ExternalSortPlan{0, kMemory}, std::move(aggregate));
        else root = std::move(aggregate);
        root->Init();
        CheckResult(*root, Drain(*root), joined, All());
        VDB_CHECK(ha->Metrics().spill_batches >= 3 && ha->Metrics().used_internal_sort);
        VDB_CHECK(gh->Metrics().output_rows == 600 && ha->Metrics().output_groups == 300);
        VDB_CHECK(l.inits == 1 && r.inits == 1 && l.delivered == left.size() && r.delivered == right.size());
        VDB_CHECK(l.calls == left.size() + 1 && r.calls == right.size() + 1);
        VDB_CHECK(!l.open && !r.open && ha->Memory().max_bytes == kMemory && gh->Memory().max_bytes == 65536);
        if (final_sort) {
            auto& sort = static_cast<ExternalSort&>(*root);
            VDB_CHECK(sort.Memory().max_bytes == kMemory && sort.Metrics().materialized_runs > 1);
            std::cout << "Sort(HashAggregate(GHJ)) join_rows=" << gh->Metrics().output_rows
                << " hash_batches=" << ha->Metrics().spill_batches << " hash_records=" << ha->Metrics().spill_records
                << " groups=" << ha->Metrics().output_groups << " final_runs=" << sort.Metrics().initial_runs
                << " temp=" << q.Temps().Metrics().allocated_temp_pages << '\n';
        }
        q.FinishQuery(*root);
        Clean(db);
    }
}

void Randomized() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    size_t comparisons = 0;
    for (unsigned seed : {10U, 1010U, 3737844653U, 92831U}) {
        std::cout << "random seed=" << seed << '\n';
        std::mt19937_64 rng(seed);
        for (size_t cardinality : {size_t{7}, size_t{230}}) {
            std::vector<Tuple> rows;
            for (size_t i = 0; i < 500; ++i) {
                auto key = static_cast<int64_t>(rng() % cardinality) - static_cast<int64_t>(cardinality / 2);
                if (key == 0) key = kMin;
                if (key == 1) key = kMax;
                int64_t measure = static_cast<int64_t>(rng() % 2000000000001ULL) - 1000000000000LL;
                if (i % 37 == 0) measure = (i % 2 ? 1 : -1) * (kMax / 4096);
                rows.push_back(Row(key, measure));
            }
            for (auto spec : Specs()) {
                const auto h = Run(db, rows, spec, true);
                const auto s = Run(db, rows, spec, false);
                VDB_CHECK(h.output_groups == s.output_groups);
                VDB_CHECK((h.spill_batches > 1) == (cardinality == 230));
                ++comparisons;
            }
            Run(db, rows, All(false), true);
            Run(db, rows, All(false), false);
            ++comparisons;
        }
    }
    std::cout << "random reference comparisons=" << comparisons << " operator executions=" << comparisons * 2 << '\n';
}

void ScaleWidth() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    {
        QueryContext q(db);
        std::vector<Tuple> rows;
        SourceState source;
        HashAggregate a(q, All(), MakeSource(q, rows, source), 65536);
        const auto cap = a.Memory().hash_group_capacity;
        for (size_t i = 0; i < 20 * cap + 1; ++i) rows.push_back(Row(static_cast<int64_t>(i), -2));
        a.Init();
        CheckResult(a, Drain(a), rows, All());
        VDB_CHECK(a.Metrics().overflow_flushes == 20 && a.Metrics().spill_batches == 21);
        VDB_CHECK(a.Metrics().spill_records == rows.size() && a.Metrics().peak_hash_groups == cap);
        VDB_CHECK(source.inits == 1 && source.delivered == rows.size());
        std::cout << "scale M=65536 cap=" << cap << " batches=" << a.Metrics().spill_batches
            << " records=" << a.Metrics().spill_records << " runs=" << a.Metrics().sort.initial_runs << '\n';
        q.FinishQuery(a);
        Clean(db);
    }
    std::vector<Column> columns{{"key", TypeId::Int64}, {"measure", TypeId::Int64}};
    for (int i = 0; i < 7; ++i) columns.push_back({"payload", TypeId::Varchar, 256});
    for (int i = 0; i < 6; ++i) columns.push_back({"other", TypeId::Int64});
    columns.push_back({"tail", TypeId::Varchar, 172});
    const auto schema = IntermediateSchema(std::move(columns));
    VDB_CHECK(schema.size() == 16 && schema.max_encoded_bytes() == 2048 && RowMemoryCharge(schema) == 4672);
    std::vector<Tuple> rows;
    for (int64_t i = 0; i < 450; ++i) {
        std::vector<Value> values{Value(i), Value(i % 9 - 4)};
        for (int j = 0; j < 7; ++j) values.emplace_back(std::string(256, 'x'));
        for (int j = 0; j < 6; ++j) values.emplace_back(int64_t{0});
        values.emplace_back(std::string(172, 'y'));
        rows.emplace_back(std::move(values));
    }
    std::reverse(rows.begin(), rows.end());
    for (bool hash : {true, false}) {
        QueryContext q(db);
        SourceState source;
        std::unique_ptr<Executor> a;
        if (hash) a = std::make_unique<HashAggregate>(q, All(),
            std::make_unique<Source>(q, schema, rows, source), 65536);
        else a = std::make_unique<SortAggregate>(q, All(),
            std::make_unique<Source>(q, schema, rows, source), 131072);
        a->Init();
        CheckResult(*a, Drain(*a), rows, All());
        const auto& memory = hash ? static_cast<HashAggregate&>(*a).Memory() : static_cast<SortAggregate&>(*a).Memory();
        VDB_CHECK(memory.maximum_row_bytes == 4672 && memory.scratch_bytes == 23232 && memory.combine_bytes == 15040);
        if (hash) {
            const auto& h = static_cast<HashAggregate&>(*a);
            VDB_CHECK(h.Metrics().spill_batches == 3 && h.Memory().hash_group_capacity == 220);
        } else VDB_CHECK(static_cast<SortAggregate&>(*a).Metrics().sort.materialized_runs > 1);
        VDB_CHECK(source.inits == 1 && source.delivered == rows.size());
        q.FinishQuery(*a);
        Clean(db);
    }
}
}

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> cases{
        {"semantics", Semantics}, {"capacity", Capacity}, {"multi_spill", MultiSpill},
        {"raw_partials", RawPartials}, {"overflow", Overflow}, {"validation", Validation}, {"capacity_one", CapacityOne},
        {"ordered_sort", OrderedSort}, {"unordered_sort", UnorderedSort}, {"lifecycle", Lifecycle},
        {"temp_limits", TempLimits}, {"failures", Failures}, {"integration", Integration},
        {"randomized", Randomized}, {"scale_width", ScaleWidth}};
    size_t passed = 0;
    try {
        for (const auto& [name, run] : cases) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            std::cout << "RUN " << name << std::endl;
            run();
            ++passed;
            std::cout << "PASS " << name << std::endl;
        }
        VDB_CHECK(passed > 0);
        std::cout << "aggregation groups passed=" << passed << " active_pins=0 peak_pins=" << maximum_pins << '\n';
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "aggregation failure: " << e.what() << '\n';
        return 1;
    }
}
