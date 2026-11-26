#include "valencedb/execution/block_nested_loop_join.hpp"
#include "join_helpers.hpp"
#include <algorithm>
#include <cassert>

namespace valencedb {


BlockNestedLoopJoin::BlockNestedLoopJoin(QueryContext& context, JoinPlan plan,
    std::unique_ptr<Executor> left, std::unique_ptr<Executor> right,
    std::optional<size_t> memory_bytes)
    : Executor(context), plan_(plan), outer_(std::move(left)), inner_(std::move(right)),
      schema_(detail::JoinSchema(context, plan, outer_, inner_)),
      outer_key_(plan.left_key), inner_key_(plan.right_key) {
    if (plan.outer == PhysicalOuter::Right) {
        outer_.swap(inner_);
        std::swap(outer_key_, inner_key_);
    }
    memory_ = ComputeBlockJoinMemory(context.Resources(), outer_->OutputSchema(),
        inner_->OutputSchema(), schema_, memory_bytes);
}

void BlockNestedLoopJoin::DoInit() {
    metrics_ = {};
    block_.reserve(memory_.block_capacity);
    metrics_.modeled_peak_bytes = CheckedAdd(memory_.scratch_bytes,
        CheckedMultiply(memory_.block_capacity, memory_.outer_row_bytes));
    outer_->Init();
}

bool BlockNestedLoopJoin::FillBlock() {
    assert(block_.empty() && !have_inner_);
    if (have_pending_) {
        block_.push_back(std::move(pending_));
        pending_ = Tuple({});
        have_pending_ = false;
    }
    while (!outer_exhausted_) {
        if (!outer_->Next(pending_)) {
            outer_exhausted_ = true;
            pending_ = Tuple({});
            break;
        }
        ++metrics_.outer_rows;
        if (block_.size() == memory_.block_capacity) {
            have_pending_ = true; // One fetched boundary row, charged to S, for the next block.
            ++metrics_.pending_boundary_tuples;
            break;
        }
        block_.push_back(std::move(pending_));
        pending_ = Tuple({});
    }
    assert(block_.size() <= memory_.block_capacity);
    if (block_.empty()) return false;
    metrics_.peak_block_rows = std::max(metrics_.peak_block_rows, block_.size());
    inner_->Init();
    ++metrics_.outer_blocks;
    ++metrics_.inner_scans;
    return true;
}

bool BlockNestedLoopJoin::DoNext(Tuple& out) {
    for (;;) {
        if (block_.empty() && !FillBlock()) return false;
        if (!have_inner_) {
            if (!inner_->Next(inner_row_)) {
                inner_->Close();
                inner_row_ = Tuple({});
                block_.clear();
                continue;
            }
            have_inner_ = true;
            position_ = 0;
        }
        while (position_ < block_.size()) {
            const auto& outer_row = block_[position_++];
            ++metrics_.candidate_comparisons;
            if (outer_row.value(outer_key_).as_int64() != inner_row_.value(inner_key_).as_int64())
                continue;
            out = detail::JoinedTuple(outer_row, inner_row_, plan_.outer);
            ++metrics_.output_rows;
            return true; // Resume the same inner row at the next block position.
        }
        inner_row_ = Tuple({});
        have_inner_ = false;
    }
}

void BlockNestedLoopJoin::DoClose() noexcept {
    inner_->Close();
    outer_->Close();
    std::vector<Tuple>().swap(block_);
    pending_ = Tuple({});
    inner_row_ = Tuple({});
    position_ = 0;
    have_pending_ = outer_exhausted_ = have_inner_ = false;
}

}  // namespace valencedb
