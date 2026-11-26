#include "test_harness.h"
#include "valencedb/common/error.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/execution/block_nested_loop_join.hpp"
#include "valencedb/execution/temp_scan.hpp"

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
constexpr PhysicalOuter sides[] = {PhysicalOuter::Left, PhysicalOuter::Right};
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_nested_join_" +
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
Schema LeftSchema() {
    return Schema(SchemaKind::Base, {{"left_payload", TypeId::Varchar, 24}, {"left_key", TypeId::Int64}});
}
Schema RightSchema() {
    return Schema(SchemaKind::Base, {{"right_key", TypeId::Int64}, {"right_id", TypeId::Int64},
                                    {"right_payload", TypeId::Varchar, 64}});
}
Tuple L(int64_t key, std::string payload) { return Tuple({Value(std::move(payload)), Value(key)}); }
Tuple R(int64_t key, int64_t id, std::string payload = "") {
    return Tuple({Value(key), Value(id), Value(std::move(payload))});
}
Schema Combined(const Schema& left, const Schema& right) {
    std::vector<Column> columns;
    for (size_t i = 0; i < left.size(); ++i) columns.push_back(left.column(i));
    for (size_t i = 0; i < right.size(); ++i) columns.push_back(right.column(i));
    return IntermediateSchema(std::move(columns));
}
void CheckSchema(const Schema& actual, const Schema& expected) {
    VDB_CHECK(actual.kind() == SchemaKind::Intermediate);
    VDB_CHECK(actual.size() == expected.size());
    VDB_CHECK(actual.max_encoded_bytes() == expected.max_encoded_bytes());
    for (size_t i = 0; i < actual.size(); ++i) {
        const auto& a = actual.column(i);
        const auto& e = expected.column(i);
        VDB_CHECK(a.name == e.name && a.type == e.type);
        VDB_CHECK(a.varchar_limit == e.varchar_limit && a.nullable == e.nullable);
    }
}
using Bag = std::vector<std::vector<std::byte>>;
Bag Canonical(const Schema& schema, const std::vector<Tuple>& rows) {
    Bag result;
    for (const auto& row : rows) result.push_back(EncodeTuple(schema, row));
    std::sort(result.begin(), result.end()); // Preserve identical duplicates.
    return result;
}
Bag Reference(const Schema& schema, const std::vector<Tuple>& left, const std::vector<Tuple>& right,
              size_t left_key, size_t right_key) {
    std::vector<Tuple> result;
    for (const auto& l : left) {
        for (const auto& r : right) {
            if (l.value(left_key).as_int64() != r.value(right_key).as_int64()) continue;
            std::vector<Value> values;
            for (size_t i = 0; i < l.size(); ++i) values.push_back(l.value(i));
            for (size_t i = 0; i < r.size(); ++i) values.push_back(r.value(i));
            result.emplace_back(std::move(values));
        }
    }
    return Canonical(schema, result);
}
struct SourceState {
    size_t inits = 0, next_calls = 0, delivered = 0, closes = 0;
    size_t allowed_inits = std::numeric_limits<size_t>::max();
    size_t fail_at = std::numeric_limits<size_t>::max();
    bool open = false, fail_init = false;
};
class Source final : public Executor {
public:
    Source(QueryContext& q, Schema schema, const std::vector<Tuple>& rows, SourceState& state)
        : Executor(q), schema_(std::move(schema)), rows_(rows), state_(state) {}
    ~Source() noexcept override { Close(); }
    void DoInit() override {
        state_.open = true;
        ++state_.inits;
        position_ = 0;
        if (state_.inits > state_.allowed_inits || state_.fail_init) {
            Close(); throw DbError("injected child Init failure");
        }
    }
    bool DoNext(Tuple& out) override {
        Pins(context_.Db());
        ++state_.next_calls;
        if (position_ == state_.fail_at) throw DbError("injected child Next failure");
        if (position_ == rows_.size()) return false;
        out = rows_[position_++];
        ++state_.delivered;
        return true;
    }
    void DoClose() noexcept override {
        if (state_.open) ++state_.closes;
        state_.open = false;
    }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {true, 0}; }
private:
    Schema schema_;
    const std::vector<Tuple>& rows_;
    SourceState& state_;
    size_t position_ = 0;
};
std::unique_ptr<Executor> MakeJoin(QueryContext& q, bool block, JoinPlan plan,
    std::unique_ptr<Executor> left, std::unique_ptr<Executor> right, std::optional<size_t> bytes = {}) {
    if (block) return std::make_unique<BlockNestedLoopJoin>(q, plan, std::move(left), std::move(right), bytes);
    return std::make_unique<NestedLoopJoin>(q, plan, std::move(left), std::move(right));
}
std::vector<Tuple> Drain(Database& db, Executor& join) {
    std::vector<Tuple> result;
    Tuple out({Value(int64_t{123})});
    while (join.Next(out)) { result.push_back(out); Pins(db); }
    VDB_CHECK(join.Exhausted());
    const auto before = result.empty() ? std::vector<std::byte>{} : EncodeTuple(join.OutputSchema(), out);
    VDB_CHECK(!join.Next(out));
    if (result.empty()) VDB_CHECK(out.size() == 1 && out.value(0).as_int64() == 123);
    else VDB_CHECK(before == EncodeTuple(join.OutputSchema(), out));
    Pins(db);
    return result;
}
size_t Allowance(const Schema& left, const Schema& right, PhysicalOuter side, size_t capacity) {
    const auto output = Combined(left, right);
    const size_t maximum = std::max({RowMemoryCharge(left), RowMemoryCharge(right), RowMemoryCharge(output)});
    return FixedScratchCharge(maximum) + capacity * RowMemoryCharge(side == PhysicalOuter::Left ? left : right);
}
void CheckMetrics(Executor& join, bool block, size_t outer, size_t inner, size_t outputs) {
    if (block) {
        const auto& b = dynamic_cast<BlockNestedLoopJoin&>(join);
        const auto& m = b.Memory();
        const auto& s = b.Metrics();
        const size_t blocks = outer / m.block_capacity + (outer % m.block_capacity != 0);
        VDB_CHECK(s.outer_rows == outer && s.outer_blocks == blocks && s.inner_scans == blocks);
        VDB_CHECK(s.candidate_comparisons == outer * inner && s.output_rows == outputs);
        VDB_CHECK(s.peak_block_rows == std::min(outer, m.block_capacity));
        VDB_CHECK(s.pending_boundary_tuples == (blocks ? blocks - 1 : 0));
        VDB_CHECK(s.modeled_peak_bytes == m.scratch_bytes + m.block_capacity * m.outer_row_bytes);
        VDB_CHECK(s.modeled_peak_bytes <= m.max_bytes);
    } else {
        const auto& s = dynamic_cast<NestedLoopJoin&>(join).Metrics();
        VDB_CHECK(s.outer_rows == outer && s.inner_scans == outer);
        VDB_CHECK(s.candidate_comparisons == outer * inner && s.output_rows == outputs);
    }
}
Bag Run(Database& db, bool block, PhysicalOuter side, const std::vector<Tuple>& left,
        const std::vector<Tuple>& right, size_t capacity = 3, bool report = false,
        const Schema& ls = LeftSchema(), const Schema& rs = RightSchema(), size_t lk = 1, size_t rk = 0,
        std::optional<size_t> allowance = {}) {
    QueryContext q(db);
    SourceState lstate, rstate;
    auto& outer = side == PhysicalOuter::Left ? lstate : rstate;
    auto& inner = side == PhysicalOuter::Left ? rstate : lstate;
    outer.allowed_inits = 1; // Boundary handling must never replay the outer.
    const size_t bytes = allowance.value_or(Allowance(ls, rs, side, capacity));
    auto join = MakeJoin(q, block, {lk, rk, side}, std::make_unique<Source>(q, ls, left, lstate),
        std::make_unique<Source>(q, rs, right, rstate), bytes);
    const auto output = Combined(ls, rs);
    CheckSchema(join->OutputSchema(), output);
    VDB_CHECK(!join->OutputOrdering().ordered);
    join->Init();
    VDB_CHECK(lstate.next_calls == 0 && rstate.next_calls == 0 && inner.inits == 0);
    const auto result = Drain(db, *join);
    const auto bag = Canonical(output, result);
    VDB_CHECK(bag == Reference(output, left, right, lk, rk));
    const size_t on = side == PhysicalOuter::Left ? left.size() : right.size();
    const size_t in = side == PhysicalOuter::Left ? right.size() : left.size();
    CheckMetrics(*join, block, on, in, result.size());
    VDB_CHECK(outer.inits == 1 && outer.delivered == on && outer.next_calls == on + 1);
    const size_t scans = block ? dynamic_cast<BlockNestedLoopJoin&>(*join).Metrics().inner_scans : on;
    VDB_CHECK(inner.inits == scans && inner.delivered == in * scans);
    VDB_CHECK(inner.next_calls == (in + 1) * scans);
    if (report) {
        std::cout << (block ? "BNLJ" : "NLJ") << " outer=" << (side == PhysicalOuter::Left ? "Left" : "Right");
        if (block) {
            const auto& b = dynamic_cast<BlockNestedLoopJoin&>(*join);
            const auto& m = b.Memory();
            std::cout << " M=" << m.max_bytes << " S=" << m.scratch_bytes << " Touter=" << m.outer_row_bytes
                      << " C=" << m.block_capacity << " blocks=" << b.Metrics().outer_blocks
                      << " pending=" << b.Metrics().pending_boundary_tuples;
        }
        std::cout << " outer_rows=" << on << " scans=" << scans << " comparisons=" << on * in
                  << " outputs=" << result.size() << '\n';
    }
    q.FinishQuery(*join);
    VDB_CHECK(!lstate.open && !rstate.open && q.Succeeded());
    Pins(db);
    return bag;
}

void EmptyAndDisjoint() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> left{L(1, "a"), L(2, "b")}, right{R(3, 0), R(4, 1)}, empty;
    for (bool block : {false, true}) for (auto side : sides) {
        Run(db, block, side, empty, right);
        Run(db, block, side, left, empty);
        Run(db, block, side, empty, empty);
        VDB_CHECK(Run(db, block, side, left, right).empty());
        VDB_CHECK(Run(db, block, side, {L(4, "one")}, {R(4, 8)}).size() == 1);
    }
    Clean(db, files); db.Close();
}
void UnequalSchemasAndExtremeKeys() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> left{L(INT64_MIN, "min"), L(-1, "neg"), L(0, "zero"),
        L(INT64_MAX, "max"), L(99, "unmatched")};
    const std::vector<Tuple> right{R(INT64_MAX, 0, "MAX"), R(-1, 1, "NEG"), R(0, 2, "ZERO"),
        R(INT64_MIN, 3, "MIN"), R(45, 4)};
    Bag baseline;
    for (bool block : {false, true}) for (auto side : sides) {
        auto bag = Run(db, block, side, left, right);
        VDB_CHECK(bag.size() == 4);
        if (baseline.empty()) baseline = bag;
        VDB_CHECK(bag == baseline);
    }
    Clean(db, files); db.Close();
}
void DuplicateMultiplicity() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    std::vector<Tuple> left{L(1, "A"), L(1, "B"), L(1, "C")}, right{R(1, 0, "X"), R(1, 1, "Y")};
    for (auto side : sides) VDB_CHECK(Run(db, false, side, left, right, 3, true).size() == 6);
    left.clear(); right.clear();
    for (int i = 0; i < 11; ++i) left.push_back(L(7, i % 3 == 0 ? "same" : std::to_string(i)));
    for (int i = 0; i < 8; ++i) right.push_back(R(7, i % 4, std::string("v\0x", 3)));
    Bag baseline;
    for (bool block : {false, true}) for (auto side : sides) {
        auto bag = Run(db, block, side, left, right, 3, true);
        VDB_CHECK(bag.size() == 88);
        if (baseline.empty()) baseline = bag;
        VDB_CHECK(bag == baseline);
    }
    Clean(db, files); db.Close();
}
void BlockBoundaries() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (size_t capacity : {size_t{1}, size_t{3}}) {
        for (size_t count : {size_t{0}, size_t{1}, capacity, capacity + 1, 2 * capacity, 2 * capacity + 1}) {
            std::vector<Tuple> left, right;
            for (size_t i = 0; i < count; ++i) {
                left.push_back(L(5, std::to_string(i)));
                right.push_back(R(5, static_cast<int64_t>(i)));
            }
            for (auto side : sides) Run(db, true, side, left, right, capacity);
        }
    }
    Clean(db, files); db.Close();
}

void ResumeAndPending() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> left{L(7, "A"), L(7, "B"), L(7, "C"), L(7, "PENDING"), L(7, "E")};
    const std::vector<Tuple> right{R(7, 0), R(7, 1), R(7, 2), R(7, 3), R(7, 4)};
    for (bool block : {false, true}) for (auto side : sides) {
        QueryContext q(db);
        SourceState ls, rs;
        auto& os = side == PhysicalOuter::Left ? ls : rs;
        auto& is = side == PhysicalOuter::Left ? rs : ls;
        os.allowed_inits = 1;
        auto join = MakeJoin(q, block, {1, 0, side}, std::make_unique<Source>(q, LeftSchema(), left, ls),
            std::make_unique<Source>(q, RightSchema(), right, rs), Allowance(LeftSchema(), RightSchema(), side, 3));
        join->Init();
        std::vector<Tuple> output;
        Tuple row({});
        for (size_t i = 0; i < 3; ++i) {
            VDB_CHECK(join->Next(row)); output.push_back(row); Pins(db);
            VDB_CHECK(is.inits == 1);
            VDB_CHECK(os.delivered == (block ? 4 : 1));
            VDB_CHECK(is.delivered == (block ? 1 : i + 1));
            if (block) {
                const auto& b = dynamic_cast<BlockNestedLoopJoin&>(*join);
                VDB_CHECK(b.Metrics().pending_boundary_tuples == 1 && b.Metrics().outer_blocks == 1);
                VDB_CHECK(b.Metrics().candidate_comparisons == i + 1);
            }
        }
        const auto saved = EncodeTuple(join->OutputSchema(), output.front());
        auto rest = Drain(db, *join);
        output.insert(output.end(), rest.begin(), rest.end());
        VDB_CHECK(output.size() == 25);
        VDB_CHECK(Canonical(join->OutputSchema(), output) == Reference(join->OutputSchema(), left, right, 1, 0));
        CheckMetrics(*join, block, 5, 5, 25);
        q.FinishQuery(*join);
        VDB_CHECK(saved == EncodeTuple(join->OutputSchema(), output.front()));
    }
    Clean(db, files); db.Close();
}

void Lifecycle() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> left{L(1, "a"), L(1, "b"), L(1, "c"), L(1, "d")};
    const std::vector<Tuple> right{R(1, 0), R(1, 1), R(1, 2), R(1, 3)};
    for (bool block : {false, true}) for (auto side : sides) {
        QueryContext q(db);
        SourceState ls, rs;
        auto join = MakeJoin(q, block, {1, 0, side}, std::make_unique<Source>(q, LeftSchema(), left, ls),
            std::make_unique<Source>(q, RightSchema(), right, rs), Allowance(LeftSchema(), RightSchema(), side, 3));
        Tuple row({});
        Throws([&] { join->Next(row); }, "requires Init");
        join->Close(); join->Close();
        join->Init();
        Throws([&] { join->Init(); }, "requires Close"); // Must leave the active stream usable.
        VDB_CHECK(join->Next(row));
        const Tuple owned = row;
        const auto saved = EncodeTuple(join->OutputSchema(), owned);
        Throws([&] { q.FinishQuery(*join); }, "requires root EOF");
        VDB_CHECK(ls.open && rs.open);
        join->Close(); join->Close(); // Middle of current inner and block; pending is live for BNLJ.
        VDB_CHECK(!ls.open && !rs.open);
        Pins(db);
        Throws([&] { join->Next(row); }, "requires Init");
        Bag first;
        for (int replay = 0; replay < 2; ++replay) {
            join->Init();
            const auto output = Drain(db, *join);
            const auto bag = Canonical(join->OutputSchema(), output);
            VDB_CHECK(bag == Reference(join->OutputSchema(), left, right, 1, 0));
            if (replay == 0) first = bag;
            VDB_CHECK(bag == first);
            CheckMetrics(*join, block, 4, 4, 16);
            Throws([&] { join->Init(); }, "requires Close");
            join->Close();
            CheckMetrics(*join, block, 4, 4, 16); // Counters survive Close.
        }
        VDB_CHECK(ls.inits == ls.closes && rs.inits == rs.closes);
        VDB_CHECK(saved == EncodeTuple(join->OutputSchema(), owned));
        q.FinishQuery(*join);
        Throws([&] { join->Init(); }, "closed");
    }
    // Destruction alone closes a partially consumed tree, then checked cancellation succeeds.
    for (bool block : {false, true}) {
        QueryContext q(db);
        SourceState ls, rs;
        {
            auto join = MakeJoin(q, block, {1, 0}, std::make_unique<Source>(q, LeftSchema(), left, ls),
                std::make_unique<Source>(q, RightSchema(), right, rs), Allowance(LeftSchema(), RightSchema(), sides[0], 3));
            join->Init(); Tuple row({}); VDB_CHECK(join->Next(row));
        }
        VDB_CHECK(!ls.open && !rs.open);
        q.Close(); VDB_CHECK(!q.Succeeded());
    }
    Clean(db, files); db.Close();
}

void InvalidConfiguration() {
    Files files, other_files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Database other(other_files.db, OpenMode::CreateNew, other_files.temp, 8);
    QueryContext q(db), foreign(other);
    const std::vector<Tuple> empty;
    for (bool block : {false, true}) {
        auto reject = [&](Schema left, Schema right, JoinPlan plan, const std::string& error = "") {
            SourceState ls, rs;
            ls.allowed_inits = rs.allowed_inits = 0;
            Throws([&] { MakeJoin(q, block, plan, std::make_unique<Source>(q, left, empty, ls),
                std::make_unique<Source>(q, right, empty, rs)); }, error);
            VDB_CHECK(ls.inits == 0 && rs.inits == 0 && ls.next_calls == 0 && rs.next_calls == 0);
        };
        for (auto side : sides) {
            reject(LeftSchema(), RightSchema(), {2, 0, side}, "index");
            reject(LeftSchema(), RightSchema(), {1, 3, side}, "index");
            reject(LeftSchema(), RightSchema(), {0, 0, side}, "NONNULL INT64");
            reject(LeftSchema(), RightSchema(), {1, 2, side}, "NONNULL INT64");
            for (auto type : {TypeId::Double, TypeId::Int64}) {
                const auto bad = IntermediateSchema({{"bad", type, 0, type == TypeId::Int64}});
                reject(bad, RightSchema(), {0, 0, side}, "NONNULL INT64");
                reject(LeftSchema(), bad, {1, 0, side}, "NONNULL INT64");
            }
            const auto nine = IntermediateSchema(std::vector<Column>(9, {"i", TypeId::Int64}));
            reject(nine, nine, {0, 0, side}, "column count");
            std::vector<Column> wide{{"key", TypeId::Int64}};
            for (int i = 0; i < 4; ++i) wide.push_back({"v", TypeId::Varchar, 256});
            const auto width = IntermediateSchema(wide);
            reject(width, width, {0, 0, side}, "size limit");
        }
        reject(LeftSchema(), RightSchema(), {1, 0, static_cast<PhysicalOuter>(55)}, "outer side");
        SourceState state;
        Throws([&] { MakeJoin(q, block, {1, 0}, nullptr,
            std::make_unique<Source>(q, RightSchema(), empty, state)); }, "children");
        Throws([&] { MakeJoin(q, block, {1, 0}, std::make_unique<Source>(q, LeftSchema(), empty, state),
            nullptr); }, "children");
        Throws([&] { MakeJoin(q, block, {1, 0}, std::make_unique<Source>(q, LeftSchema(), empty, state),
            std::make_unique<Source>(foreign, RightSchema(), empty, state)); }, "children");
        VDB_CHECK(state.inits == 0 && state.next_calls == 0);
    }
    q.Close(); foreign.Close(); Clean(db, files); db.Close(); other.Close();
}

void MemoryValidation() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {20000, 16384});
    const std::vector<Tuple> empty;
    SourceState ls, rs;
    const auto left = LeftSchema(), right = RightSchema(), output = Combined(left, right);
    VDB_CHECK(left.max_encoded_bytes() == 38 && right.max_encoded_bytes() == 86);
    VDB_CHECK(output.max_encoded_bytes() == 120 && RowMemoryCharge(output) == 464);
    VDB_CHECK(RowMemoryCharge(left) == 208 && RowMemoryCharge(right) == 336);
    constexpr size_t scratch = 10608;
    for (auto side : sides) {
        const size_t charge = side == PhysicalOuter::Left ? 208 : 336;
        for (size_t bytes : {size_t{0}, size_t{1}, scratch - 1, scratch, scratch + charge - 1}) {
            Throws([&] { BlockNestedLoopJoin b(q, {1, 0, side},
                std::make_unique<Source>(q, left, empty, ls), std::make_unique<Source>(q, right, empty, rs), bytes); });
            VDB_CHECK(ls.inits == 0 && rs.inits == 0 && ls.next_calls == 0 && rs.next_calls == 0);
        }
        for (auto bytes : {std::optional<size_t>{}, std::optional<size_t>{scratch + 3 * charge + charge - 1},
                           std::optional<size_t>{std::numeric_limits<size_t>::max()}}) {
            BlockNestedLoopJoin b(q, {1, 0, side}, std::make_unique<Source>(q, left, empty, ls),
                std::make_unique<Source>(q, right, empty, rs), bytes);
            const auto& m = b.Memory();
            VDB_CHECK(m.max_bytes == bytes.value_or(20000) && m.scratch_bytes == scratch);
            VDB_CHECK(m.outer_row_bytes == charge && m.block_capacity == (m.max_bytes - scratch) / charge);
            // Inspect SIZE_MAX arithmetic without attempting its enormous reserve.
        }
    }
    const size_t maximum = std::numeric_limits<size_t>::max();
    Throws([&] { RowMemoryCharge(maximum, 2048); }, "overflow");
    Throws([&] { RowMemoryCharge(16, maximum); }, "overflow");
    Throws([&] { FixedScratchCharge(maximum); }, "overflow");
    Throws([&] { RemainingMemory(0, scratch); }, "scratch");
    q.Close(); Clean(db, files); db.Close();
}

void WideOuterAndPayloads() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const Schema narrow(SchemaKind::Base, {{"key", TypeId::Int64}});
    const Schema wide(SchemaKind::Base, {{"key", TypeId::Int64}, {"a", TypeId::Varchar, 256},
        {"b", TypeId::Varchar, 256}, {"c", TypeId::Varchar, 256}});
    std::vector<Tuple> left, right;
    for (int64_t i = 0; i < 40; ++i) left.emplace_back(std::vector<Value>{Value(i % 3)});
    for (int64_t i = 0; i < 5; ++i) right.emplace_back(std::vector<Value>{Value(i % 3),
        Value(std::string(static_cast<size_t>(i * 64), 'a')), Value(std::string("b\0c", 3)), Value(std::string(256, 'z'))});
    const size_t bytes = Allowance(narrow, wide, PhysicalOuter::Right, 2);
    auto l = Run(db, true, PhysicalOuter::Left, left, right, 2, true, narrow, wide, 0, 0, bytes);
    auto r = Run(db, true, PhysicalOuter::Right, left, right, 2, true, narrow, wide, 0, 0, bytes);
    VDB_CHECK(l == r);
    VDB_CHECK(RowMemoryCharge(narrow) == 120 && RowMemoryCharge(wide) == 1768);
    // Nullable/DOUBLE payloads are supported even though keys must be NONNULL INT64.
    const auto payload = IntermediateSchema({{"key", TypeId::Int64}, {"d", TypeId::Double, 0, true},
        {"v", TypeId::Varchar, 256, true}});
    const std::vector<Tuple> rows{Tuple({Value(int64_t{0}), Value::Null(TypeId::Double), Value::Null(TypeId::Varchar)}),
        Tuple({Value(int64_t{1}), Value(1.25), Value(std::string("\0xyz", 4))})};
    for (bool block : {false, true}) for (auto side : sides)
        Run(db, block, side, left, rows, 3, false, narrow, payload, 0, 0);
    Clean(db, files); db.Close();
}

void RandomizedOracle() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    for (uint64_t seed : {8ULL, 808ULL, 3737844653ULL, 812381ULL}) {
        std::cout << "random seed=" << seed << '\n';
        std::mt19937_64 rng(seed);
        std::vector<Tuple> left, right;
        for (int i = 0; i < 73; ++i) {
            std::string payload(rng() % 25, 'x');
            for (auto& c : payload) c = static_cast<char>(rng() % 256);
            left.push_back(L(static_cast<int64_t>(rng() % 21) - 10, payload));
            if (i > 0 && i % 9 == 0) left.back() = left[i - 1];
        }
        for (int i = 0; i < 57; ++i) {
            std::string payload(rng() % 65, 'y');
            for (auto& c : payload) c = static_cast<char>(rng() % 256);
            right.push_back(R(static_cast<int64_t>(rng() % 27) - 13, i, payload));
            if (i > 0 && i % 7 == 0) right.back() = right[i - 1];
        }
        Bag baseline;
        for (bool block : {false, true}) for (auto side : sides) {
            const auto bag = Run(db, block, side, left, right, 7);
            if (baseline.empty()) baseline = bag;
            VDB_CHECK(bag == baseline);
        }
    }
    Clean(db, files); db.Close();
}

void MaximumOutputSchema() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    std::vector<Column> lc(7, {"large", TypeId::Varchar, 256});
    lc.push_back({"left_key", TypeId::Int64});
    std::vector<Column> rc(7, {"right_int", TypeId::Int64});
    rc.push_back({"tail", TypeId::Varchar, 172});
    const auto ls = IntermediateSchema(lc), rs = IntermediateSchema(rc), output = Combined(ls, rs);
    VDB_CHECK(output.size() == 16 && output.max_encoded_bytes() == 2048);
    VDB_CHECK(RowMemoryCharge(output) == 4672);
    VDB_CHECK(FixedScratchCharge(RowMemoryCharge(output)) == 23232);
    std::vector<Tuple> left, right;
    for (int i = 0; i < 3; ++i) {
        std::vector<Value> values(7, Value(std::string(256, static_cast<char>('a' + i))));
        values.emplace_back(int64_t{1}); left.emplace_back(std::move(values));
    }
    for (int i = 0; i < 4; ++i) {
        std::vector<Value> values(7, Value(int64_t{1}));
        values.emplace_back(std::string(172, static_cast<char>('A' + i))); right.emplace_back(std::move(values));
    }
    for (bool block : {false, true}) for (auto side : sides) {
        const auto bag = Run(db, block, side, left, right, 2, false, ls, rs, 7, 0);
        VDB_CHECK(bag.size() == 12);
        for (const auto& encoded : bag) VDB_CHECK(encoded.size() == 2048);
    }
    {
        QueryContext q(db);
        SourceState lstate, rstate;
        Throws([&] { BlockNestedLoopJoin b(q, {7, 0}, std::make_unique<Source>(q, ls, left, lstate),
            std::make_unique<Source>(q, rs, right, rstate), 16384); }, "scratch");
        VDB_CHECK(lstate.inits == 0 && rstate.inits == 0);
        q.Close();
    }
    Clean(db, files); db.Close();
}

void ChildFailure() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const std::vector<Tuple> left{L(1, "A"), L(1, "B"), L(1, "C"), L(1, "D"), L(1, "E")};
    const std::vector<Tuple> right{R(1, 0), R(1, 1), R(1, 2), R(1, 3), R(1, 4)};
    for (bool block : {false, true}) for (auto side : sides) for (bool fail_outer : {false, true}) {
        QueryContext q(db);
        SourceState ls, rs;
        auto& os = side == PhysicalOuter::Left ? ls : rs;
        auto& is = side == PhysicalOuter::Left ? rs : ls;
        auto& failure = fail_outer ? os : is;
        auto join = MakeJoin(q, block, {1, 0, side}, std::make_unique<Source>(q, LeftSchema(), left, ls),
            std::make_unique<Source>(q, RightSchema(), right, rs), Allowance(LeftSchema(), RightSchema(), side, 3));
        failure.fail_init = true;
        Throws([&] { join->Init(); Tuple row({}); join->Next(row); }, "Init failure");
        VDB_CHECK(!ls.open && !rs.open && !join->Exhausted());
        failure.fail_init = false;
        failure.fail_at = fail_outer ? 4 : 1;
        join->Init();
        Tuple row({}); VDB_CHECK(join->Next(row));
        Throws([&] { while (join->Next(row)) {} }, "Next failure");
        VDB_CHECK(!ls.open && !rs.open && !join->Exhausted());
        Throws([&] { q.FinishQuery(*join); }, "EOF");
        join->Close(); join->Close(); Pins(db);
        failure.fail_at = std::numeric_limits<size_t>::max();
        join->Init();
        VDB_CHECK(Canonical(join->OutputSchema(), Drain(db, *join)) == Reference(join->OutputSchema(), left, right, 1, 0));
        q.FinishQuery(*join);
    }
    Clean(db, files); db.Close();
}

void DiskPipelines() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    const Schema ls(SchemaKind::Base, {{"lp", TypeId::Varchar, 256}, {"lk", TypeId::Int64}});
    const Schema rs(SchemaKind::Base, {{"rk", TypeId::Int64}, {"rid", TypeId::Int64}, {"rp", TypeId::Varchar, 200}});
    db.CreateTable("left", ls); db.CreateTable("right", rs);
    std::vector<Tuple> left, right;
    for (int64_t i = 0; i < 90; ++i) {
        const int64_t key = i % 13 - 6;
        Tuple row = L(key, std::string(256, static_cast<char>('a' + i % 20)));
        db.Insert("left", row);
        if (key >= -4) left.emplace_back(std::vector<Value>{row.value(1), row.value(0)});
    }
    for (int64_t i = 0; i < 110; ++i) {
        const int64_t key = i % 13 - 6;
        Tuple row = R(key, i, std::string(200, static_cast<char>('A' + i % 20)));
        db.Insert("right", row);
        if (key >= -3 && key <= 5) right.emplace_back(std::vector<Value>{row.value(2), row.value(1), row.value(0)});
    }
    db.CreateIndex("right", 0);
    VDB_CHECK(db.Table("left").heap_pages + db.Table("right").heap_pages > 8);
    for (bool block : {false, true}) for (auto side : sides) {
        QueryContext q(db);
        auto l = std::make_unique<Projection>(q, ProjectionPlan{{1, 0}},
            std::make_unique<Filter>(q, FilterPlan{{1, CompareOp::GreaterEqual, -4}},
                std::make_unique<SeqScan>(q, SeqScanPlan{"left", true})));
        auto r = std::make_unique<Projection>(q, ProjectionPlan{{2, 1, 0}},
            std::make_unique<IndexScan>(q, IndexScanPlan{"right", 0, -3, 5}));
        const size_t bytes = Allowance(l->OutputSchema(), r->OutputSchema(), side, 3);
        const auto expected_schema = Combined(l->OutputSchema(), r->OutputSchema());
        auto join = MakeJoin(q, block, {0, 2, side}, std::move(l), std::move(r), bytes);
        CheckSchema(join->OutputSchema(), expected_schema);
        VDB_CHECK(!join->OutputOrdering().ordered);
        const auto reads = db.Disk().counters().database_reads;
        join->Init(); Pins(db);
        VDB_CHECK(Canonical(join->OutputSchema(), Drain(db, *join)) == Reference(join->OutputSchema(), left, right, 0, 2));
        const size_t count = Reference(join->OutputSchema(), left, right, 0, 2).size();
        CheckMetrics(*join, block, side == PhysicalOuter::Left ? left.size() : right.size(),
                     side == PhysicalOuter::Left ? right.size() : left.size(), count);
        VDB_CHECK(db.Disk().counters().database_reads > reads);
        std::cout << "F=8 " << (block ? "BNLJ" : "NLJ") << " outer=" << (side == PhysicalOuter::Left ? "Left" : "Right")
                  << " outputs=" << count << " file_reads=" << db.Disk().counters().database_reads - reads << '\n';
        q.FinishQuery(*join); Clean(db, files);
    }
    db.Close();
}

void TempInputsAndFailure() {
    Files files, other_files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    Database other(other_files.db, OpenMode::CreateNew, other_files.temp, 8);
    const Schema schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"payload", TypeId::Varchar, 256}});
    std::vector<Tuple> rows;
    for (int64_t i = 0; i < 80; ++i)
        rows.emplace_back(std::vector<Value>{Value(i % 5), Value(std::string(256, static_cast<char>(i)))});
    for (bool block : {false, true}) for (auto side : sides) {
        QueryContext q(db);
        TempWriter writer(q.Temps(), schema);
        for (const auto& row : rows) writer.Append(row);
        auto relation = writer.Finish();
        { QueryContext foreign(other); Throws([&] { TempScan scan(foreign, relation); }, "query"); foreign.Close(); }
        auto join = MakeJoin(q, block, {0, 0, side}, std::make_unique<TempScan>(q, relation),
            std::make_unique<TempScan>(q, relation), Allowance(schema, schema, side, 7));
        const size_t pages = relation.PageCount();
        VDB_CHECK(pages > 1);
        join->Init(); Tuple row({}); VDB_CHECK(join->Next(row));
        const Tuple owned = row;
        const auto saved = EncodeTuple(join->OutputSchema(), row);
        join->Close(); Pins(db);
        for (int i = 0; i < 2; ++i) {
            join->Init();
            const auto result = Drain(db, *join);
            VDB_CHECK(result.size() == 1280);
            VDB_CHECK(Canonical(join->OutputSchema(), result) == Reference(join->OutputSchema(), rows, rows, 0, 0));
            CheckMetrics(*join, block, 80, 80, 1280);
            join->Close();
            VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == pages); // Rescan creates no scratch.
        }
        VDB_CHECK(saved == EncodeTuple(join->OutputSchema(), owned));
        q.FinishQuery(*join); Clean(db, files);
    }
    // A real TempReader read fails after a valid prefix, with no EOF or pin leak.
    {
        QueryContext q(db);
        TempWriter writer(q.Temps(), schema);
        for (const auto& row : rows) writer.Append(row);
        auto relation = writer.Finish();
        BlockNestedLoopJoin join(q, {0, 0}, std::make_unique<TempScan>(q, relation),
            std::make_unique<TempScan>(q, relation), Allowance(schema, schema, PhysicalOuter::Left, 3));
        join.Init(); Tuple row({}); VDB_CHECK(join.Next(row));
        for (const auto& frame : db.Buffer().InspectFrames())
            if (frame.page && frame.page->file == FileKind::Temporary) db.Buffer().Invalidate(*frame.page);
        db.Disk().FailNextIo(FailNext::Read);
        Throws([&] { while (join.Next(row)) {} });
        VDB_CHECK(!join.Exhausted());
        Throws([&] { q.FinishQuery(join); }, "EOF");
        join.Close(); join.Close(); Pins(db);
        q.Close(); VDB_CHECK(!q.Succeeded()); Clean(db, files);
        Throws([&] { TempScan scan(q, relation); }, "closed");
    }
    db.Close(); other.Close();
}
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"empty_disjoint", EmptyAndDisjoint}, {"unequal_extreme", UnequalSchemasAndExtremeKeys},
        {"duplicate_multiplicity", DuplicateMultiplicity}, {"block_boundaries", BlockBoundaries},
        {"resume_pending", ResumeAndPending}, {"lifecycle", Lifecycle},
        {"invalid_configuration", InvalidConfiguration}, {"memory_validation", MemoryValidation},
        {"wide_outer_payloads", WideOuterAndPayloads}, {"randomized_oracle", RandomizedOracle},
        {"maximum_output_schema", MaximumOutputSchema},
        {"child_failure", ChildFailure}, {"disk_pipelines", DiskPipelines}, {"temp_inputs_failure", TempInputsAndFailure}};
    try {
        for (const auto& [name, run] : tests) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            run(); ++cases_passed; std::cout << "PASS " << name << '\n';
        }
        VDB_CHECK(cases_passed != 0);
        std::cout << cases_passed << " nested-loop join groups passed; peak pins=" << maximum_pins
                  << ", all relevant boundaries active pins=0\n";
    } catch (const std::exception& error) {
        std::cerr << "nested-loop join failure after " << cases_passed << " groups: " << error.what() << '\n';
        return 1;
    }
}
