#include "valencedb/execution/grace_hash_join.hpp"
#include "valencedb/execution/temp_scan.hpp"
#include "join_helpers.hpp"
#include "valencedb/common/error.hpp"
#include <algorithm>
#include <cassert>

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
uint64_t PartitionHash(int64_t key) noexcept {
    return Mix(static_cast<uint64_t>(key) + UINT64_C(0x9e3779b97f4a7c15));
}
}  // namespace

// A different seed avoids reusing the partition hash for table buckets.
size_t GraceHashJoin::BuildHash::operator()(int64_t key) const noexcept {
    return static_cast<size_t>(Mix(static_cast<uint64_t>(key) + UINT64_C(0xd1b54a32d192ed03)));
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

void GraceHashJoin::DoInit() {
    metrics_ = {};
    Partition(*left_, left_schema_, plan_.left_key, left_parts_);
    Partition(*right_, right_schema_, plan_.right_key, right_parts_);
    phase_ = Phase::SelectPair;
}

void GraceHashJoin::Build() {
    const bool left = build_side_ == PhysicalOuter::Left;
    const auto& run = left ? left_parts_[pair_] : right_parts_[pair_];
    table_ = std::make_unique<Table>();
    table_->reserve(run.RowCount()); // SelectPair already established that this fits.
    {
        TempReader reader(run);
        Tuple row({});
        while (reader.Next(row)) {
            if (table_->size() == run.RowCount())
                throw DbError("Grace build row count disagrees with partition metadata");
            const int64_t key = row.value(left ? plan_.left_key : plan_.right_key).as_int64();
            table_->emplace(key, std::move(row));
            metrics_.peak_build_rows = std::max(metrics_.peak_build_rows, table_->size());
        }
    } // Build reader and its page are destroyed before opening probe.
    if (table_->size() != run.RowCount())
        throw DbError("Grace build row count disagrees with partition metadata");
    probe_ = std::make_unique<TempReader>(left ? right_parts_[pair_] : left_parts_[pair_]);
    match_ = match_end_ = table_->cend();
    ++metrics_.hash_pairs;
    if (left) ++metrics_.left_build_pairs;
    else ++metrics_.right_build_pairs;
    phase_ = Phase::Probe;
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
        } else {
            Build();
        }
        return true;
    }
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

bool GraceHashJoin::DoNext(Tuple& out) {
    for (;;) {
        if (phase_ == Phase::SelectPair && !SelectPair()) return false;
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
        if (Probe(out)) return true;
        ReleasePair();
    }
}

bool GraceHashJoin::Probe(Tuple& out) {
    assert(phase_ == Phase::Probe);
    for (;;) {
        if (match_ != match_end_) {
            out = detail::JoinedTuple(match_->second, probe_row_, build_side_);
            ++match_;
            ++metrics_.output_rows;
            return true;
        }
        if (!probe_->Next(probe_row_)) return false;
        const size_t key = build_side_ == PhysicalOuter::Left ? plan_.right_key : plan_.left_key;
        const auto range = table_->equal_range(probe_row_.value(key).as_int64());
        match_ = range.first;
        match_end_ = range.second;
    }
}

void GraceHashJoin::DoClose() noexcept {
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
    phase_ = Phase::SelectPair;
}

}  // namespace valencedb
