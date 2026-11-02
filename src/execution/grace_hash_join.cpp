#include "valencedb/execution/grace_hash_join.hpp"
#include "valencedb/execution/temp_scan.hpp"
#include "join_helpers.hpp"
#include <algorithm>
#include <cassert>
#include <new>

namespace valencedb {
namespace {
uint64_t Mix(uint64_t value) noexcept {
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}
size_t Blocks(size_t rows, size_t capacity) {
    return rows / capacity + (rows % capacity != 0); // No rounding-add overflow.
}
}  // namespace

uint64_t GraceHashJoin::PartitionHash(int64_t key) noexcept {
    return Mix(static_cast<uint64_t>(key) + UINT64_C(0x9e3779b97f4a7c15));
}
size_t GraceHashJoin::BuildHash::operator()(int64_t key) const noexcept {
    return static_cast<size_t>(Mix(static_cast<uint64_t>(key) + UINT64_C(0xd1b54a32d192ed03)));
}

GraceHashJoinMemory ComputeGraceJoinMemory(const QueryResources& resources,
    const Schema& left, const Schema& right, const Schema& output, std::optional<size_t> memory_bytes) {
    GraceHashJoinMemory m{};
    m.max_bytes = ResolveBudget(resources, memory_bytes).max_bytes;
    m.left_row_bytes = RowMemoryCharge(left);
    m.right_row_bytes = RowMemoryCharge(right);
    m.scratch_bytes = FixedScratchCharge(std::max({m.left_row_bytes,
        m.right_row_bytes, RowMemoryCharge(output)}));
    if (m.max_bytes <= m.scratch_bytes)
        throw DbError("Grace join memory cannot cover scratch");
    const size_t available = RemainingMemory(m.max_bytes, m.scratch_bytes);
    // Both run arrays coexist with one side's writers.
    constexpr size_t pair_bytes = 2 * kTempRunCharge;
    m.partition_count = std::min(size_t{64},
        available / (kPageSize + kTempHandleOverhead + pair_bytes));
    if (m.partition_count < 2) throw DbError("Grace join requires at least two partitions");
    m.descriptor_bytes = CheckedMultiply(pair_bytes, m.partition_count);
    const size_t reserve = CheckedAdd(m.scratch_bytes, m.descriptor_bytes);
    if (m.max_bytes <= reserve) throw DbError("Grace join cannot cover descriptors");
    const size_t build_bytes = RemainingMemory(m.max_bytes, reserve);
    m.left_build_capacity = build_bytes / HashBuildRowCharge(left);
    m.right_build_capacity = build_bytes / HashBuildRowCharge(right);
    m.fallback_memory = RemainingMemory(m.max_bytes, m.descriptor_bytes);
    m.left_block_capacity = build_bytes / m.left_row_bytes;
    m.right_block_capacity = build_bytes / m.right_row_bytes;
    const size_t block_max = std::vector<Tuple>().max_size();
    if (m.left_block_capacity > block_max) m.left_block_capacity = 0;
    if (m.right_block_capacity > block_max) m.right_block_capacity = 0;
    if (!m.left_block_capacity && !m.right_block_capacity)
        throw DbError("Grace join fallback cannot hold a legal outer block");
    if (m.left_build_capacity > std::unordered_multimap<int64_t, Tuple>().max_size() ||
        m.right_build_capacity > std::unordered_multimap<int64_t, Tuple>().max_size())
        throw DbError("Grace join build capacity is unrepresentable");
    return m;
}

GraceHashJoin::GraceHashJoin(QueryContext& context, GraceHashJoinPlan plan,
    std::unique_ptr<Executor> left, std::unique_ptr<Executor> right)
    : Executor(context), plan_(plan), left_(std::move(left)), right_(std::move(right)),
      schema_(detail::JoinSchema(context, {plan.left_key, plan.right_key}, left_, right_)),
      left_schema_(left_->OutputSchema()), right_schema_(right_->OutputSchema()) {
    memory_ = ComputeGraceJoinMemory(context.Resources(), left_schema_, right_schema_,
        schema_, plan.memory_bytes);
}

void GraceHashJoin::Partition(Executor& child, const Schema& schema, size_t key, Partitions& parts) {
    // This entire vector (including every private page) dies before the other side starts.
    std::vector<std::unique_ptr<TempWriter>> writers;
    parts.reserve(memory_.partition_count);
    writers.reserve(memory_.partition_count);
    for (size_t i = 0; i < memory_.partition_count; ++i) {
        writers.push_back(std::make_unique<TempWriter>(context_.Temps(), schema));
    }
    metrics_.peak_partition_writers = std::max(metrics_.peak_partition_writers, writers.size());
    child.Init();
    Tuple row({});
    while (child.Next(row)) {
        const size_t partition = PartitionHash(row.value(key).as_int64()) % memory_.partition_count;
        writers[partition]->Append(row);
    }
    child.Close();
    for (auto& writer : writers) parts.push_back(writer->Finish());
}

void GraceHashJoin::Init() {
    Start();
    try {
        metrics_ = {};
        Partition(*left_, left_schema_, plan_.left_key, left_parts_);
        Partition(*right_, right_schema_, plan_.right_key, right_parts_);
        phase_ = Phase::SelectPair;
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("Grace join memory allocation failed");
    } catch (const std::length_error&) {
        Close(); throw DbError("Grace join allocation size is unrepresentable");
    } catch (...) { Close(); throw; }
}

bool GraceHashJoin::Build(size_t capacity) {
    const bool left = build_side_ == PhysicalOuter::Left;
    const auto& run = left ? left_parts_[pair_] : right_parts_[pair_];
    table_ = std::make_unique<Table>();
    table_->max_load_factor(1.0f);
    // Reserve only the known rows, bounded by the explicit capacity, including on defensive tests.
    const size_t reserved_rows = std::min(run.RowCount(), capacity);
    table_->reserve(reserved_rows);
    metrics_.modeled_peak_build_bytes = std::max(metrics_.modeled_peak_build_bytes,
        CheckedAdd(CheckedAdd(memory_.scratch_bytes, memory_.descriptor_bytes),
            CheckedMultiply(reserved_rows, HashBuildRowCharge(run.OutputSchema()))));
    {
        TempReader reader(run);
        Tuple row({});
        while (reader.Next(row)) {
            if (table_->size() >= capacity) {
                metrics_.failed_build_rows = CheckedAdd(metrics_.failed_build_rows,
                                                        CheckedAdd(table_->size(), 1));
                reader.Close();
                table_.reset(); // Nodes AND buckets die before fallback is constructed.
                return false;
            }
            const int64_t key = row.value(left ? plan_.left_key : plan_.right_key).as_int64();
            table_->emplace(key, std::move(row));
            metrics_.peak_build_rows = std::max(metrics_.peak_build_rows, table_->size());
        }
    } // Build reader and its page are destroyed before opening probe.
    probe_ = std::make_unique<TempReader>(left ? right_parts_[pair_] : left_parts_[pair_]);
    match_ = match_end_ = table_->cend();
    ++metrics_.hash_pairs;
    if (left) ++metrics_.left_build_pairs;
    else ++metrics_.right_build_pairs;
    phase_ = Phase::Probe;
    return true;
}

void GraceHashJoin::BeginFallback() {
    assert(!table_ && !probe_ && probe_row_.size() == 0);
    const auto& l = left_parts_[pair_];
    const auto& r = right_parts_[pair_];
    // Prefer fewer complete inner scans; tie goes to logical Left. No global optimizer.
    PhysicalOuter outer = PhysicalOuter::Left;
    if (!memory_.left_block_capacity || (memory_.right_block_capacity &&
        Blocks(r.RowCount(), memory_.right_block_capacity) < Blocks(l.RowCount(), memory_.left_block_capacity)))
        outer = PhysicalOuter::Right;
    fallback_ = std::make_unique<BlockNestedLoopJoin>(context_,
        JoinPlan{plan_.left_key, plan_.right_key, outer},
        std::make_unique<TempScan>(context_, left_parts_[pair_]),
        std::make_unique<TempScan>(context_, right_parts_[pair_]), memory_.fallback_memory);
    fallback_->Init();
    if (outer == PhysicalOuter::Left) ++metrics_.left_fallback_pairs;
    else ++metrics_.right_fallback_pairs;
    ++metrics_.fallback_pairs;
    phase_ = Phase::Fallback;
}

bool GraceHashJoin::SelectPair() {
    while (pair_ < memory_.partition_count) {
        const auto& l = left_parts_[pair_];
        const auto& r = right_parts_[pair_];
        if (!l.RowCount() || !r.RowCount()) {
            ++metrics_.empty_pairs;
            ReleasePair();
            continue;
        }
        const size_t left_bytes = CheckedMultiply(l.RowCount(), HashBuildRowCharge(left_schema_));
        const size_t right_bytes = CheckedMultiply(r.RowCount(), HashBuildRowCharge(right_schema_));
        build_side_ = left_bytes <= right_bytes ? PhysicalOuter::Left : PhysicalOuter::Right;
        const bool left = build_side_ == PhysicalOuter::Left;
        const size_t capacity = left ? memory_.left_build_capacity : memory_.right_build_capacity;
        const size_t build_rows = left ? l.RowCount() : r.RowCount();
        if (build_rows > capacity) {
            BeginFallback();
        } else if (!Build(capacity)) {
            BeginFallback(); // A defensive runtime cap check abandoned the entire table.
        }
        return true;
    }
    phase_ = Phase::Done;
    return false;
}

void GraceHashJoin::ReleasePair() noexcept {
    fallback_.reset();
    probe_.reset();
    match_ = match_end_ = Table::const_iterator{};
    table_.reset();
    probe_row_ = Tuple({});
    ++pair_;
    phase_ = Phase::SelectPair;
}

bool GraceHashJoin::Next(Tuple& out) {
    if (!Ready()) return false;
    try {
        for (;;) {
            if (phase_ == Phase::SelectPair && !SelectPair()) return End();
            if (phase_ == Phase::Fallback) {
                if (fallback_->Next(out)) {
                    ++metrics_.output_rows;
                    return true;
                }
                metrics_.completed_fallback_inner_scans = CheckedAdd(
                    metrics_.completed_fallback_inner_scans, fallback_->Metrics().inner_scans);
                ReleasePair();
                continue;
            }
            assert(phase_ == Phase::Probe);
            if (match_ != match_end_) {
                out = detail::JoinedTuple(match_->second, probe_row_, build_side_);
                ++match_;
                ++metrics_.output_rows;
                return true;
            }
            probe_row_ = Tuple({});
            if (!probe_->Next(probe_row_)) {
                ReleasePair();
                continue;
            }
            const size_t key = build_side_ == PhysicalOuter::Left ? plan_.right_key : plan_.left_key;
            const auto range = table_->equal_range(probe_row_.value(key).as_int64());
            match_ = range.first;
            match_end_ = range.second;
        }
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("Grace join memory allocation failed");
    } catch (const std::length_error&) {
        Close(); throw DbError("Grace join allocation size is unrepresentable");
    } catch (...) { Close(); throw; }
}

void GraceHashJoin::Close() noexcept {
    fallback_.reset();
    probe_.reset();
    match_ = match_end_ = Table::const_iterator{};
    table_.reset();
    probe_row_ = Tuple({});
    Partitions().swap(left_parts_);
    Partitions().swap(right_parts_);
    left_->Close();
    right_->Close();
    pair_ = 0;
    phase_ = Phase::Closed;
    CloseState();
}

}  // namespace valencedb
