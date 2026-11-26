#include "test_harness.h"
#include "valencedb/common/error.hpp"
#include "valencedb/execution/grace_hash_join.hpp"
#include "valencedb/execution/external_sort.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/execution/temp_scan.hpp"
#include "valencedb/storage/slotted_page.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>

using namespace valencedb;

namespace {
uint64_t maximum_pins = 0;
size_t cases_passed = 0;
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_grace_join_" +
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
    for (const auto& f : db.Buffer().InspectFrames())
        VDB_CHECK(!f.page || f.page->file != FileKind::Temporary);
}
Schema Narrow() { return Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"id", TypeId::Int64}}); }
Schema Wide() {
    return Schema(SchemaKind::Base, {{"payload", TypeId::Varchar, 256},
                                    {"key", TypeId::Int64}, {"id", TypeId::Int64}});
}
Tuple N(int64_t key, int64_t id = 0) { return Tuple({Value(key), Value(id)}); }
Tuple W(int64_t key, int64_t id = 0, std::string payload = "") {
    return Tuple({Value(std::move(payload)), Value(key), Value(id)});
}
std::vector<Tuple> Rows(size_t count, int64_t key, bool wide = false) {
    std::vector<Tuple> rows;
    for (size_t i = 0; i < count; ++i)
        rows.push_back(wide ? W(key, static_cast<int64_t>(i), std::string(256, 'w')) : N(key, static_cast<int64_t>(i)));
    return rows;
}
Schema Combined(const Schema& left, const Schema& right) {
    std::vector<Column> columns;
    for (size_t i = 0; i < left.size(); ++i) columns.push_back(left.column(i));
    for (size_t i = 0; i < right.size(); ++i) columns.push_back(right.column(i));
    return IntermediateSchema(std::move(columns));
}
using Bag = std::vector<std::vector<std::byte>>;
Bag Canonical(const Schema& schema, const std::vector<Tuple>& rows) {
    Bag result;
    for (const auto& row : rows) result.push_back(EncodeTuple(schema, row));
    std::sort(result.begin(), result.end());
    return result;
}
Bag Reference(const Schema& schema, const std::vector<Tuple>& left, const std::vector<Tuple>& right,
              size_t lk, size_t rk) {
    std::vector<Tuple> result;
    for (const auto& l : left) for (const auto& r : right) {
        if (l.value(lk).as_int64() != r.value(rk).as_int64()) continue;
        std::vector<Value> values;
        for (size_t i = 0; i < l.size(); ++i) values.push_back(l.value(i));
        for (size_t i = 0; i < r.size(); ++i) values.push_back(r.value(i));
        result.emplace_back(std::move(values));
    }
    return Canonical(schema, result);
}
struct SourceState {
    size_t inits = 0, calls = 0, delivered = 0, closes = 0, allowed = 1;
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
        pos_ = 0;
        if (state_.inits > state_.allowed || state_.fail_init) throw DbError("child replay or Init failure");
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
    Ordering OutputOrdering() const override { return {true, 0}; }
private:
    const Schema schema_;
    const std::vector<Tuple>& rows_;
    SourceState& state_;
    size_t pos_ = 0;
};
std::unique_ptr<Source> Child(QueryContext& q, const Schema& schema,
                             const std::vector<Tuple>& rows, SourceState& state) {
    return std::make_unique<Source>(q, schema, rows, state);
}
std::vector<Tuple> Drain(Database& db, Executor& join) {
    std::vector<Tuple> result;
    Tuple out({Value(int64_t{123})});
    while (join.Next(out)) {
        result.push_back(out);
        Pins(db);
    }
    VDB_CHECK(join.Exhausted());
    const auto before = result.empty() ? std::vector<std::byte>{} : EncodeTuple(join.OutputSchema(), out);
    VDB_CHECK(!join.Next(out));
    if (result.empty()) VDB_CHECK(out.size() == 1 && out.value(0).as_int64() == 123);
    else VDB_CHECK(before == EncodeTuple(join.OutputSchema(), out));
    Pins(db);
    return result;
}
void SingleConsumption(const SourceState& state, size_t rows, size_t executions = 1) {
    VDB_CHECK(state.inits == executions && state.calls == executions * (rows + 1));
    VDB_CHECK(state.delivered == executions * rows && state.closes == executions && !state.open);
}
// Independent reference for the fixed partition mapping; no executor internals exposed.
size_t PartitionNumber(int64_t key, size_t count) {
    uint64_t h = static_cast<uint64_t>(key) + UINT64_C(0x9e3779b97f4a7c15);
    h = (h ^ (h >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    h = (h ^ (h >> 27)) * UINT64_C(0x94d049bb133111eb);
    return (h ^ (h >> 31)) % count;
}
struct PartitionInfo {
    size_t rows = 0, pages = 0;
    size_t RowCount() const { return rows; }
    size_t PageCount() const { return pages; }
};
using Parts = std::vector<PartitionInfo>;
Parts ExpectedParts(const Schema& schema, const std::vector<Tuple>& rows, size_t key, size_t count) {
    Parts parts(count);
    std::vector<size_t> used(count, kPageSize);
    for (const auto& row : rows) {
        const auto p = PartitionNumber(row.value(key).as_int64(), count);
        const auto bytes = EncodeTuple(schema, row).size() + SlottedPage::kSlotSize;
        if (used[p] + bytes > kPageSize) {
            ++parts[p].pages;
            used[p] = SlottedPage::kHeaderSize;
        }
        used[p] += bytes;
        ++parts[p].rows;
    }
    return parts;
}
// Decode the public scratch page format, including interleaved partition chains.
void AuditPartitions(GraceHashJoin& join, const Schema& ls, const Schema& rs,
                     const std::vector<Tuple>& left, const std::vector<Tuple>& right,
                     size_t lk, size_t rk) {
    auto& db = join.Context().Db();
    const size_t count = join.Memory().partition_count;
    PageId page = 0;
    for (bool is_left : {true, false}) {
        const auto& schema = is_left ? ls : rs;
        const auto& original = is_left ? left : right;
        const auto key = is_left ? lk : rk;
        const auto expected = ExpectedParts(schema, original, key, count);
        size_t pages = 0;
        for (const auto& p : expected) pages += p.pages;
        const PageId first = page, end = page + pages;
        Parts actual(count);
        std::vector<size_t> partition(pages), incoming(pages), tails(count);
        std::vector<PageId> links(pages);
        std::vector<Tuple> decoded;
        for (; page < end; ++page) {
            auto guard = db.Buffer().Fetch({FileKind::Temporary, page});
            const auto& data = guard.Data();
            SlottedPage::Validate(data, page, kInvalidPageId);
            VDB_CHECK(SlottedPage::SlotCount(data) > 0);
            for (uint16_t slot = 0; slot < SlottedPage::SlotCount(data); ++slot) {
                auto bytes = SlottedPage::Fetch(data, slot);
                VDB_CHECK(bytes.has_value());
                auto row = DecodeTuple(schema, *bytes);
                const auto p = PartitionNumber(row.value(key).as_int64(), count);
                if (slot == 0) { partition[page - first] = p; ++actual[p].pages; }
                VDB_CHECK(partition[page - first] == p);
                ++actual[p].rows;
                decoded.push_back(std::move(row));
            }
            links[page - first] = SlottedPage::Next(data);
        }
        for (size_t i = 0; i < pages; ++i) {
            if (links[i] == kInvalidPageId) { ++tails[partition[i]]; continue; }
            VDB_CHECK(links[i] > first + i && links[i] < end);
            VDB_CHECK(++incoming[links[i] - first] == 1);
            VDB_CHECK(partition[links[i] - first] == partition[i]);
        }
        for (size_t i = 0; i < count; ++i) {
            VDB_CHECK(actual[i].rows == expected[i].rows && actual[i].pages == expected[i].pages);
            VDB_CHECK(tails[i] == (expected[i].rows ? 1 : 0));
        }
        VDB_CHECK(Canonical(schema, decoded) == Canonical(schema, original));
    }
    VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == page);
    VDB_CHECK(join.Metrics().peak_partition_writers == count);
    VDB_CHECK(join.Metrics().output_rows == 0 && join.Metrics().hash_pairs == 0);
}
// Predict decisions and page transfers from independently packed input partitions.
size_t PairMetrics(const GraceHashJoin& join, const Parts& left = {}, const Parts& right = {}) {
    const auto& metrics = join.Metrics();
    const auto& memory = join.Memory();
    VDB_CHECK(metrics.hash_pairs + metrics.fallback_pairs + metrics.empty_pairs == memory.partition_count);
    VDB_CHECK(metrics.left_build_pairs + metrics.right_build_pairs == metrics.hash_pairs);
    VDB_CHECK(metrics.left_fallback_pairs + metrics.right_fallback_pairs == metrics.fallback_pairs);
    VDB_CHECK(metrics.peak_build_rows <= std::max(memory.left_build_capacity, memory.right_build_capacity));
    const size_t reserve = memory.scratch_bytes + memory.descriptor_bytes;
    VDB_CHECK(reserve + memory.left_build_capacity * (memory.left_row_bytes + 96) <= memory.max_bytes);
    VDB_CHECK(reserve + memory.right_build_capacity * (memory.right_row_bytes + 96) <= memory.max_bytes);
    if (left.empty()) return 0; // A consuming parent has already closed this child.
    size_t reads = 0, hashes = 0, fallbacks = 0, empty = 0, scans = 0;
    size_t left_builds = 0, right_builds = 0, left_fallbacks = 0, right_fallbacks = 0;
    for (size_t i = 0; i < left.size(); ++i) {
        const auto& l = left[i];
        const auto& r = right[i];
        if (!l.RowCount() || !r.RowCount()) { ++empty; continue; }
        const bool build_left = l.RowCount() * (memory.left_row_bytes + 96) <=
                                r.RowCount() * (memory.right_row_bytes + 96);
        const size_t rows = build_left ? l.RowCount() : r.RowCount();
        const size_t capacity = build_left ? memory.left_build_capacity : memory.right_build_capacity;
        if (rows <= capacity) {
            ++hashes;
            if (build_left) ++left_builds; else ++right_builds;
            reads += l.PageCount() + r.PageCount();
        } else {
            ++fallbacks;
            const size_t left_blocks = (l.RowCount() - 1) / memory.left_block_capacity + 1;
            const size_t right_blocks = (r.RowCount() - 1) / memory.right_block_capacity + 1;
            if (left_blocks <= right_blocks) {
                ++left_fallbacks;
                scans += left_blocks;
                reads += l.PageCount() + left_blocks * r.PageCount();
            } else {
                ++right_fallbacks;
                scans += right_blocks;
                reads += r.PageCount() + right_blocks * l.PageCount();
            }
        }
    }
    VDB_CHECK(hashes == metrics.hash_pairs && fallbacks == metrics.fallback_pairs && empty == metrics.empty_pairs);
    VDB_CHECK(left_builds == metrics.left_build_pairs && right_builds == metrics.right_build_pairs);
    VDB_CHECK(left_fallbacks == metrics.left_fallback_pairs && right_fallbacks == metrics.right_fallback_pairs);
    VDB_CHECK(scans == metrics.completed_fallback_inner_scans);
    return reads;
}
GraceHashJoinMetrics Run(const Schema& ls, const Schema& rs, const std::vector<Tuple>& left,
    const std::vector<Tuple>& right, size_t lk, size_t rk, size_t memory = 20000) {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db);
    SourceState a, b;
    GraceHashJoin join(q, {lk, rk, memory}, Child(q, ls, left, a), Child(q, rs, right, b));
    VDB_CHECK(join.OutputOrdering() == Ordering{});
    const auto combined = Combined(ls, rs);
    for (size_t i = 0; i < combined.size(); ++i) {
        const auto& c = combined.column(i);
        const auto& actual = join.OutputSchema().column(i);
        VDB_CHECK(c.name == actual.name && c.type == actual.type && c.varchar_limit == actual.varchar_limit && c.nullable == actual.nullable);
    }
    join.Init();
    AuditPartitions(join, ls, rs, left, right, lk, rk);
    const auto lp = ExpectedParts(ls, left, lk, join.Memory().partition_count);
    const auto rp = ExpectedParts(rs, right, rk, join.Memory().partition_count);
    const auto fetches = db.Metrics().buffer_fetches;
    const auto output = Drain(db, join);
    VDB_CHECK(Canonical(join.OutputSchema(), output) == Reference(join.OutputSchema(), left, right, lk, rk));
    VDB_CHECK(output.size() == join.Metrics().output_rows);
    VDB_CHECK(db.Metrics().buffer_fetches - fetches == PairMetrics(join, lp, rp));
    SingleConsumption(a, left.size());
    SingleConsumption(b, right.size());
    const auto result = join.Metrics();
    q.FinishQuery(join);

    Clean(db);
    return result;
}
int64_t KeyFor(size_t partition, size_t count) {
    for (int64_t key = -10000; key < 10000; ++key)
        if (PartitionNumber(key, count) == partition) return key;
    throw std::runtime_error("test partition key search failed");
}

void EmptyDisjoint() {
    for (bool l : {false, true}) for (bool r : {false, true}) {
        const auto m = Run(Narrow(), Narrow(), Rows(l ? 4 : 0, 7), Rows(r ? 3 : 0, 7), 0, 0);
        VDB_CHECK(m.output_rows == (l && r ? 12 : 0));
        VDB_CHECK(m.empty_pairs == (l && r ? 1 : 2));
    }
    VDB_CHECK(Run(Narrow(), Narrow(), Rows(1, 7), Rows(1, 7), 0, 0).output_rows == 1);
    const auto m = Run(Narrow(), Narrow(), Rows(70, -1), Rows(80, 42), 0, 0);
    VDB_CHECK(m.output_rows == 0 && m.hash_pairs + m.fallback_pairs + m.empty_pairs == 2);
    // Disjoint keys deliberately in one pair: exercise a real empty equal_range.
    const int64_t first = KeyFor(0, 2);
    int64_t second = first + 1;
    while (PartitionNumber(second, 2) != 0) ++second;
    VDB_CHECK(Run(Narrow(), Narrow(), Rows(3, first), Rows(4, second), 0, 0).hash_pairs == 1);
    const auto fallback = Run(Narrow(), Narrow(), Rows(70, first), Rows(80, second), 0, 0);
    VDB_CHECK(fallback.fallback_pairs == 1 && fallback.output_rows == 0);
}
void ExtremeDuplicates() {
    std::vector<Tuple> left, right;
    for (int64_t key : {INT64_MIN, int64_t{-1}, int64_t{0}, int64_t{1}, INT64_MAX}) {
        left.push_back(W(key, 1, std::string("a\0b", 3)));
        left.push_back(left.back());
        right.push_back(N(key, 11)); right.push_back(N(key, 12)); right.push_back(N(key, 12));
    }
    const auto m = Run(Wide(), Narrow(), left, right, 1, 0, 32768);
    VDB_CHECK(m.output_rows == 30 && m.right_build_pairs > 0 && m.fallback_pairs == 0);
    VDB_CHECK(Run(Narrow(), Narrow(), Rows(2, 5), Rows(3, 5), 0, 0).output_rows == 6);
}
void BuildChoice() {
    auto m = Run(Wide(), Narrow(), Rows(10, 7, true), Rows(12, 7), 1, 0, 32768);
    VDB_CHECK(m.right_build_pairs == 1 && m.peak_build_rows == 12 && m.output_rows == 120);
    std::cout << "wide choice: left 10*(720+96)=8160; right 12*(168+96)=3168; build Right; output=120\n";
    m = Run(Narrow(), Wide(), Rows(12, 7), Rows(10, 7, true), 0, 1, 32768);
    VDB_CHECK(m.left_build_pairs == 1 && m.peak_build_rows == 12 && m.output_rows == 120);
    m = Run(Narrow(), Narrow(), Rows(3, 7), Rows(3, 7), 0, 0);
    VDB_CHECK(m.left_build_pairs == 1); // Equal modeled footprint ties choose Left.
}
void BoundariesAndSkew() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db);
    SourceState a, b;
    const std::vector<Tuple> empty;
    GraceHashJoin model(q, {0, 0, 20000}, Child(q, Narrow(), empty, a), Child(q, Narrow(), empty, b));
    const auto memory = model.Memory();
    VDB_CHECK(memory.left_row_bytes == 168 && memory.right_row_bytes == 168);
    VDB_CHECK(memory.scratch_bytes == 10008 && memory.partition_count == 2 && memory.descriptor_bytes == 256);
    VDB_CHECK(memory.left_build_capacity == 36 && memory.right_build_capacity == 36);
    VDB_CHECK(memory.fallback_memory == 19744 && memory.left_block_capacity == 57);
    std::cout << "model: M=20000 S=10008 p=2 Dghj=256 Tleft=Tright=168 capacity=36/36 fallback=19744 Cblock=57\n";
    q.Close();
    for (size_t n : {size_t{35}, size_t{36}, size_t{37}}) {
        const auto m = Run(Narrow(), Narrow(), Rows(n, 7), Rows(n + 1, 7), 0, 0);
        VDB_CHECK(m.hash_pairs == (n <= 36 ? 1 : 0));
        VDB_CHECK(m.fallback_pairs == (n > 36 ? 1 : 0));
        VDB_CHECK(m.peak_build_rows == (n <= 36 ? n : 0));
        std::cout << "boundary rows=" << n << " hash=" << m.hash_pairs << " fallback=" << m.fallback_pairs
                  << " outputs=" << m.output_rows << '\n';
    }
    const auto m = Run(Narrow(), Narrow(), Rows(120, 7), Rows(130, 7), 0, 0);
    VDB_CHECK(m.fallback_pairs == 1 && m.hash_pairs == 0 && m.peak_build_rows == 0 && m.output_rows == 15600);
    VDB_CHECK(m.left_fallback_pairs == 1 && m.completed_fallback_inner_scans == 3);
    std::cout << "all-equal skew: 120x130=15600; fallback_pairs=1; Cblock=57; scans=3\n";
}
void FallbackRightAndStandalone() {
    const auto left = Rows(120, 7, true), right = Rows(150, 7);
    const auto m = Run(Wide(), Narrow(), left, right, 1, 0, 22000);
    VDB_CHECK(m.fallback_pairs == 1 && m.output_rows == 18000);
    VDB_CHECK(m.right_fallback_pairs == 1);
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db);
    SourceState a, b; a.allowed = 100;
    BlockNestedLoopJoin bnlj(q, {1, 0, PhysicalOuter::Right}, Child(q, Wide(), left, a),
                            Child(q, Narrow(), right, b), 22000 - 256);
    bnlj.Init();
    const auto actual = Drain(db, bnlj);
    VDB_CHECK(Canonical(bnlj.OutputSchema(), actual) == Reference(bnlj.OutputSchema(), left, right, 1, 0));
    VDB_CHECK(bnlj.Memory().block_capacity == (22000 - 256 - bnlj.Memory().scratch_bytes) / RowMemoryCharge(Narrow()));
    VDB_CHECK(bnlj.Metrics().inner_scans == m.completed_fallback_inner_scans);
    q.FinishQuery(bnlj); Clean(db);
}
void BuildCountMismatch() {
    for (bool extra : {false, true}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db);
        const auto left = Rows(8, 7), right = Rows(9, 7);
        SourceState a, b;
        GraceHashJoin join(q, {0, 0, 20000}, Child(q, Narrow(), left, a), Child(q, Narrow(), right, b));
        join.Init();
        // Corrupt the build input through its ordinary page representation.
        {
            auto guard = db.Buffer().Fetch({FileKind::Temporary, 0});
            if (extra) VDB_CHECK(SlottedPage::Insert(guard.MutableData(), EncodeTuple(Narrow(), left.front())));
            else VDB_CHECK(SlottedPage::Erase(guard.MutableData(), 0));
            guard.MarkDirty();
        }
        Tuple row({});
        Throws([&] { join.Next(row); }, "row count");
        VDB_CHECK(join.Metrics().fallback_pairs == 0 && join.Metrics().output_rows == 0);
        VDB_CHECK(join.Metrics().peak_build_rows <= left.size());
        VDB_CHECK(!join.Exhausted() && !q.Succeeded());
        SingleConsumption(a, 8); SingleConsumption(b, 9);
        q.Close(); Clean(db);
    }
}
void ManyPartitionsAndMixedPairs() {
    auto m = Run(Narrow(), Narrow(), Rows(2, 7), Rows(3, 7), 0, 0, 1024 * 1024);
    VDB_CHECK(m.peak_partition_writers == 64 && m.empty_pairs == 63 && m.hash_pairs == 1);
    std::vector<Tuple> left, right;
    for (size_t p = 0; p < 12; ++p) {
        const int64_t key = KeyFor(p, 12);
        const auto l = Rows(p + 1, key), r = Rows(14 - p, key);
        left.insert(left.end(), l.begin(), l.end());
        right.insert(right.end(), r.begin(), r.end());
    }
    // Narrow/Narrow S=10008: M=63000 gives p=12 > F=8.
    m = Run(Narrow(), Narrow(), left, right, 0, 0, 63000);
    VDB_CHECK(m.hash_pairs == 12 && m.peak_partition_writers == 12);
    VDB_CHECK(m.left_build_pairs > 0 && m.right_build_pairs > 0 && m.peak_build_rows == 7);
    std::cout << "F=8 p=12: hash_pairs=12 peak_build_rows=7 peak_pins=" << maximum_pins << '\n';
    left = Rows(2, KeyFor(0, 3)); right = Rows(3, KeyFor(0, 3));
    const auto large_left = Rows(90, KeyFor(1, 3)), large_right = Rows(95, KeyFor(1, 3));
    left.insert(left.end(), large_left.begin(), large_left.end());
    right.insert(right.end(), large_right.begin(), large_right.end());
    m = Run(Narrow(), Narrow(), left, right, 0, 0, 23300);
    VDB_CHECK(m.peak_partition_writers == 3 && m.hash_pairs == 1 && m.fallback_pairs == 1 && m.empty_pairs == 1);
    VDB_CHECK(m.peak_build_rows == 2 && m.output_rows == 8556);
    std::cout << "mixed p=3: hash=1 fallback=1 empty=1 output=8556\n";
}
void LifecycleReplay() {
    for (bool fallback : {false, true}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db);
        auto left = Rows(fallback ? 90 : 4, KeyFor(0, 2));
        auto right = Rows(fallback ? 95 : 5, KeyFor(0, 2));
        left.push_back(N(KeyFor(1, 2), 99)); right.push_back(N(KeyFor(1, 2), 100));
        SourceState a, b; a.allowed = b.allowed = 3;
        GraceHashJoin join(q, {0, 0, 20000}, Child(q, Narrow(), left, a), Child(q, Narrow(), right, b));
        Tuple out({});
        Throws([&] { join.Next(out); });
        join.Close(); join.Close();
        join.Init();
        Throws([&] { join.Init(); }, "Close"); // Does not damage the live stream.
        VDB_CHECK(join.Next(out));
        const auto owned = EncodeTuple(join.OutputSchema(), out);
        if (fallback) VDB_CHECK(join.Metrics().fallback_pairs == 1);
        else VDB_CHECK(join.Metrics().peak_build_rows == 4);
        Throws([&] { q.FinishQuery(join); }, "EOF");
        const size_t extent = q.Temps().Metrics().allocated_temp_pages;
        join.Close(); join.Close();

        VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == extent);
        VDB_CHECK(EncodeTuple(join.OutputSchema(), out) == owned);
        Throws([&] { join.Next(out); });
        for (size_t execution = 2; execution <= 3; ++execution) {
            join.Init();
            VDB_CHECK(join.Metrics().output_rows == 0);
            VDB_CHECK(Canonical(join.OutputSchema(), Drain(db, join)) == Reference(join.OutputSchema(), left, right, 0, 0));
            PairMetrics(join);
            VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == execution * extent);
            SingleConsumption(a, left.size(), execution); SingleConsumption(b, right.size(), execution);
            join.Close();
        }
        std::cout << "replay " << (fallback ? "fallback" : "hash") << ": extent=" << extent << " -> " << 3 * extent << '\n';
        q.FinishQuery(join); Clean(db);
    }
    // Destructor closes an active hash/probe; QueryContext sees no surviving handles.
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db);
    const auto rows = Rows(3, 7);
    SourceState a, b;
    {
        GraceHashJoin join(q, {0, 0, 20000}, Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
        join.Init(); Tuple out({}); VDB_CHECK(join.Next(out));
    }
    q.Close(); Clean(db);
}
void InvalidConfiguration() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {20000, 100});
    const std::vector<Tuple> rows;
    SourceState a, b;
    auto invalid = [&](const Schema& ls, const Schema& rs, size_t lk, size_t rk, size_t bytes) {
        Throws([&] { GraceHashJoin join(q, {lk, rk, bytes}, Child(q, ls, rows, a), Child(q, rs, rows, b)); });
        VDB_CHECK(a.inits == 0 && b.inits == 0 && a.calls == 0 && b.calls == 0);
    };
    invalid(Narrow(), Narrow(), 2, 0, 20000);
    invalid(Narrow(), Narrow(), 0, 2, 20000);
    for (TypeId type : {TypeId::Varchar, TypeId::Double, TypeId::Int64}) {
        Schema bad(SchemaKind::Intermediate, {{"bad", type, uint16_t(type == TypeId::Varchar ? 20 : 0), type == TypeId::Int64}});
        invalid(bad, Narrow(), 0, 0, 20000); invalid(Narrow(), bad, 0, 0, 20000);
    }
    for (size_t bytes : {size_t{0}, size_t{1}, size_t{10007}, size_t{10008}, size_t{10009}, size_t{18455}, size_t{18711}})
        invalid(Narrow(), Narrow(), 0, 0, bytes);
    GraceHashJoin enormous(q, {0, 0, std::numeric_limits<size_t>::max()},
        Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
    VDB_CHECK(enormous.Memory().partition_count == 64); // Inspect without allocating huge modeled capacity.
    std::vector<Column> columns(9, {"v", TypeId::Int64});
    invalid(IntermediateSchema(columns), IntermediateSchema(columns), 0, 0, 1000000);
    columns = {{"key", TypeId::Int64}};
    for (int i = 0; i < 4; ++i) columns.push_back({"wide", TypeId::Varchar, 256});
    invalid(IntermediateSchema(columns), IntermediateSchema(columns), 0, 0, 1000000);
    Throws([&] { GraceHashJoin join(q, {0, 0}, nullptr, Child(q, Narrow(), rows, b)); });
    Files foreign_files;
    Database foreign_db(foreign_files.db, OpenMode::CreateNew, foreign_files.temp, 8);
    QueryContext foreign(foreign_db);
    Throws([&] { GraceHashJoin join(q, {0, 0}, Child(foreign, Narrow(), rows, a), Child(q, Narrow(), rows, b)); });
    GraceHashJoin defaulted(q, {0, 0}, Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
    VDB_CHECK(defaulted.Memory().max_bytes == 20000);
    GraceHashJoin minimum(q, {0, 0, 18712}, Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
    VDB_CHECK(minimum.Memory().partition_count == 2);
    VDB_CHECK(minimum.Memory().fallback_memory - minimum.Memory().scratch_bytes == 8448);
    VDB_CHECK(minimum.Memory().left_block_capacity >= 1 && minimum.Memory().right_block_capacity >= 1);
    VDB_CHECK(a.inits == 0 && b.inits == 0);
    q.Close(); foreign.Close();
    Throws([&] { QueryContext zero_cap(db, {20000, 0}); }, "positive");
    Throws([&] { CheckedMultiply(std::numeric_limits<size_t>::max(), 2); }, "overflow");
    Throws([&] { CheckedAdd(std::numeric_limits<size_t>::max(), 1); }, "overflow");
    Throws([&] { RemainingMemory(1, 2); }, "scratch");
}
void MaximumSchema() {
    std::vector<Column> columns{{"key", TypeId::Int64}};
    for (size_t i = 0; i < 3; ++i) columns.push_back({"text", TypeId::Varchar, 256});
    columns.push_back({"text", TypeId::Varchar, 214});
    for (size_t i = 0; i < 3; ++i) columns.push_back({"number", TypeId::Int64});
    const auto schema = IntermediateSchema(columns); // 8 columns, max encoded 1026.
    VDB_CHECK(Combined(schema, schema).max_encoded_bytes() == 2048);
    std::vector<Value> values{Value(int64_t{7})};
    for (size_t i = 0; i < 3; ++i) values.emplace_back(std::string(256, 'x'));
    values.emplace_back(std::string(214, 'y'));
    for (size_t i = 0; i < 3; ++i) values.emplace_back(int64_t(i));
    const std::vector<Tuple> rows{Tuple(values)};
    // Maximum legal output T=4672, S=23232. p=2 minimum still leaves 8448 for a block.
    const auto m = Run(schema, schema, rows, rows, 0, 0, 23232 + 2 * 4352);
    VDB_CHECK(m.hash_pairs == 1 && m.output_rows == 1);
    Schema payload(SchemaKind::Intermediate, {{"key", TypeId::Int64}, {"double", TypeId::Double, 0, true},
                                             {"text", TypeId::Varchar, 8, true}});
    const std::vector<Tuple> special{Tuple({Value(int64_t{7}), Value::Null(TypeId::Double), Value::Null(TypeId::Varchar)}),
                                    Tuple({Value(int64_t{7}), Value(2.5), Value(std::string("\0", 1))})};
    VDB_CHECK(Run(payload, Narrow(), special, Rows(3, 7), 0, 0, 23300).output_rows == 6);
}
void TempLimits() {
    for (size_t cap : {size_t{1}, size_t{3}}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db, {20000, cap});
        const auto rows = Rows(8, 7);
        SourceState a, b; a.allowed = b.allowed = 2;
        GraceHashJoin join(q, {0, 0}, Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
        if (cap == 3) {
            join.Init(); VDB_CHECK(Drain(db, join).size() == 64); join.Close();
            VDB_CHECK(db.Disk().PageCount(FileKind::Temporary) == 2);
        }
        Throws([&] { join.Init(); }, "cap");
        VDB_CHECK(!join.Exhausted() && !q.Succeeded());
        VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == cap);

        join.Close(); q.Close(); Clean(db);
    }
    // Released pairs do not free the extent; a downstream sort exceeds the remaining cap.
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    QueryContext q(db, {20000, 5});
    auto left = Rows(2, KeyFor(0, 2)), right = left;
    const auto more = Rows(12, KeyFor(1, 2));
    left.insert(left.end(), more.begin(), more.end()); right = left;
    SourceState a, b;
    auto join = std::make_unique<GraceHashJoin>(q, GraceHashJoinPlan{0, 0, 20000},
                                              Child(q, Narrow(), left, a), Child(q, Narrow(), right, b));
    auto* observed = join.get();
    ExternalSort sort(q, {0, 24576}, std::move(join));
    Throws([&] { sort.Init(); }, "cap");
    VDB_CHECK(observed->Metrics().hash_pairs == 2 && observed->Metrics().output_rows > 4);
    VDB_CHECK(observed->Metrics().peak_build_rows == 12 && observed->Metrics().fallback_pairs == 0);
    VDB_CHECK(q.Temps().Metrics().allocated_temp_pages == 5);
    VDB_CHECK(!sort.Exhausted() && !q.Succeeded());

    sort.Close(); q.Close(); Clean(db);
    std::cout << "temp caps: partial partition cap=1; replay extent 2->3 then failure; released-pair sort extent=5 then failure\n";
}
void Failures() {
    for (bool right : {false, true}) for (bool init : {false, true}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db);
        const auto rows = Rows(20, 7);
        SourceState a, b;
        auto& failed = right ? b : a;
        if (init) failed.fail_init = true;
        else failed.fail_at = 3;
        GraceHashJoin join(q, {0, 0, 20000}, Child(q, Narrow(), rows, a), Child(q, Narrow(), rows, b));
        Throws([&] { join.Init(); }, "child");
        VDB_CHECK(!a.open && !b.open && !join.Exhausted());

        q.Close(); Clean(db);
    }
    for (bool fallback : {false, true}) {
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 8);
        QueryContext q(db);
        const auto left = Rows(fallback ? 80 : 3, 7, true), right = Rows(180, 7, true);
        SourceState a, b;
        GraceHashJoin join(q, {1, 1, 24576}, Child(q, Wide(), left, a), Child(q, Wide(), right, b));
        join.Init();
        Tuple row({}); VDB_CHECK(join.Next(row));
        if (fallback) VDB_CHECK(join.Metrics().fallback_pairs == 1);
        // Flush/evict temp pages so a later reader transfer must hit the real short file read.
        for (PageId id = 0; id < db.Disk().PageCount(FileKind::Temporary); ++id)
            db.Buffer().Invalidate({FileKind::Temporary, id});
        db.Disk().Flush(FileKind::Temporary);
        std::filesystem::resize_file(files.temp, 0);
        Throws([&] { while (join.Next(row)) {} }, "short page read");
        VDB_CHECK(!join.Exhausted() && !q.Succeeded());

        q.Close(); Clean(db);
    }
}
void SortComposition() {
    Files files;
    Database db(files.db, OpenMode::CreateNew, files.temp, 8);
    db.CreateTable("left", Wide()); db.CreateTable("right", Narrow());
    std::vector<Tuple> left, right;
    for (int64_t i = 0; i < 180; ++i) { left.push_back(W(i % 47 - 23, i, std::string(256, char('a' + i % 26)))); db.Insert("left", left.back()); }
    for (int64_t i = 0; i < 120; ++i) { right.push_back(N(i % 47 - 23, i)); db.Insert("right", right.back()); }
    QueryContext q(db);
    auto join = std::make_unique<GraceHashJoin>(q, GraceHashJoinPlan{1, 0, 65536},
        std::make_unique<SeqScan>(q, SeqScanPlan{"left", true}),
        std::make_unique<SeqScan>(q, SeqScanPlan{"right", true}));
    auto* observed = join.get();
    ExternalSort sort(q, {1, 32768}, std::move(join));
    sort.Init();
    VDB_CHECK(observed->Memory().max_bytes == 65536 && sort.Memory().max_bytes == 32768);
    VDB_CHECK(observed->Memory().partition_count == 12);
    const auto output = Drain(db, sort);
    for (size_t i = 1; i < output.size(); ++i)
        VDB_CHECK(output[i - 1].value(1).as_int64() <= output[i].value(1).as_int64());
    VDB_CHECK(Canonical(sort.OutputSchema(), output) == Reference(sort.OutputSchema(), left, right, 1, 0));
    VDB_CHECK(sort.Metrics().initial_runs > 1 && sort.Metrics().final_merge_streamed);
    VDB_CHECK(observed->Metrics().hash_pairs > 1 && observed->Metrics().fallback_pairs == 0);
    VDB_CHECK(db.Disk().counters().temporary_reads > 0 && db.Disk().counters().temporary_writes > 0);
    PairMetrics(*observed);
    std::cout << "ExternalSort(GHJ): GHJ M=65536 p=12 sort M=32768 outputs=" << output.size()
              << " hash_pairs=" << observed->Metrics().hash_pairs << " sort runs=" << sort.Metrics().initial_runs
              << " temp extent=" << q.Temps().Metrics().allocated_temp_pages << '\n';
    q.FinishQuery(sort); Clean(db);
}
void RandomizedOracle() {
    for (uint32_t seed : {9U, 909U, 3737844653U, 92831U}) {
        std::cout << "random seed=" << seed << '\n';
        std::mt19937 rng(seed);
        for (bool skew : {false, true}) for (bool wide : {false, true}) {
            std::vector<Tuple> left, right;
            for (size_t i = 0, n = 80 + rng() % 20; i < n; ++i) {
                const int64_t key = skew && i < 75 ? 7 : int64_t(rng() % 31) - 15;
                std::string text(rng() % 257, '\0');
                for (char& c : text) c = static_cast<char>(rng() % 256);
                left.push_back(wide ? W(key, int64_t(i % 17), text) : N(key, int64_t(i % 17)));
            }
            for (size_t i = 0, n = 90 + rng() % 30; i < n; ++i)
                right.push_back(N(skew && i < 85 ? 7 : int64_t(rng() % 41) - 18, int64_t(i % 13)));
            left.push_back(left.front()); right.push_back(right.front());
            const auto m = Run(wide ? Wide() : Narrow(), Narrow(), left, right, wide ? 1 : 0, 0,
                               skew ? (wide ? 22000 : 20000) : 65536);
            VDB_CHECK(skew ? m.fallback_pairs > 0 : (m.hash_pairs > 0 && m.fallback_pairs == 0));
        }
    }
}
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> tests{
        {"empty_disjoint", EmptyDisjoint}, {"extreme_duplicates", ExtremeDuplicates},
        {"build_choice", BuildChoice}, {"boundaries_skew", BoundariesAndSkew},
        {"fallback_right", FallbackRightAndStandalone}, {"build_count_mismatch", BuildCountMismatch},
        {"many_partitions_mixed", ManyPartitionsAndMixedPairs}, {"lifecycle_replay", LifecycleReplay},
        {"invalid_configuration", InvalidConfiguration}, {"maximum_schema", MaximumSchema},
        {"temp_limits", TempLimits}, {"failures", Failures}, {"sort_composition", SortComposition},
        {"randomized_oracle", RandomizedOracle}};
    try {
        for (const auto& [name, run] : tests) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            std::cout << "RUN " << name << std::endl;
            run(); ++cases_passed;
        }
        VDB_CHECK(cases_passed > 0);
        std::cout << "Grace Hash Join: " << cases_passed << " groups passed; active pins=0; maximum peak pins=" << maximum_pins << '\n';
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
