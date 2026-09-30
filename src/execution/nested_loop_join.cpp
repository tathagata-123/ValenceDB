#include "valencedb/execution/nested_loop_join.hpp"
#include "join_helpers.hpp"
#include <new>

namespace valencedb {

NestedLoopJoin::NestedLoopJoin(QueryContext& context, JoinPlan plan,
                               std::unique_ptr<Executor> left, std::unique_ptr<Executor> right)
    : Executor(context), plan_(plan), outer_(std::move(left)), inner_(std::move(right)),
      schema_(detail::JoinSchema(context, plan, outer_, inner_)),
      outer_key_(plan.left_key), inner_key_(plan.right_key) {
    if (plan.outer == PhysicalOuter::Right) {
        outer_.swap(inner_);
        std::swap(outer_key_, inner_key_);
    }
}

void NestedLoopJoin::Init() {
    Start();
    try {
        metrics_ = {};
        outer_->Init(); // Inner starts only after the first outer row is available.
    } catch (const std::bad_alloc&) {
        Close();
        throw DbError("nested loop join memory allocation failed");
    } catch (...) { Close(); throw; }
}

bool NestedLoopJoin::Next(Tuple& out) {
    if (!Ready()) return false;
    try {
        Tuple inner_row({});
        for (;;) {
            if (!have_outer_) {
                if (!outer_->Next(outer_row_)) return End();
                have_outer_ = true;
                ++metrics_.outer_rows;
                inner_->Init();
                ++metrics_.inner_scans;
            }
            while (inner_->Next(inner_row)) {
                ++metrics_.candidate_comparisons;
                if (outer_row_.value(outer_key_).as_int64() != inner_row.value(inner_key_).as_int64())
                    continue;
                out = detail::JoinedTuple(outer_row_, inner_row, plan_.outer);
                ++metrics_.output_rows;
                return true; // Keep this outer row and the inner's next position.
            }
            inner_->Close();
            outer_row_ = Tuple({});
            have_outer_ = false;
        }
    } catch (const std::bad_alloc&) {
        Close();
        throw DbError("nested loop join memory allocation failed");
    } catch (...) { Close(); throw; }
}

void NestedLoopJoin::Close() noexcept {
    inner_->Close();
    outer_->Close();
    outer_row_ = Tuple({});
    have_outer_ = false;
    CloseState();
}

}  // namespace valencedb
