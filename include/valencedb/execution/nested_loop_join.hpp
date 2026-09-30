#pragma once

#include "valencedb/execution/executor.hpp"
#include <memory>

namespace valencedb {

enum class PhysicalOuter { Left, Right };
struct JoinPlan {
    size_t left_key, right_key;
    PhysicalOuter outer = PhysicalOuter::Left;
};
struct NestedLoopJoinMetrics {
    size_t outer_rows = 0, inner_scans = 0, candidate_comparisons = 0, output_rows = 0;
};

// The physical inner must support the Close -> Init replay contract.
class NestedLoopJoin final : public Executor {
public:
    NestedLoopJoin(QueryContext& context, JoinPlan plan,
                   std::unique_ptr<Executor> left, std::unique_ptr<Executor> right);
    ~NestedLoopJoin() noexcept override { Close(); }
    void Init() override;
    bool Next(Tuple& out) override;
    void Close() noexcept override;
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
    const NestedLoopJoinMetrics& Metrics() const noexcept { return metrics_; }
private:
    JoinPlan plan_;
    std::unique_ptr<Executor> outer_, inner_;
    Schema schema_;
    size_t outer_key_, inner_key_;
    NestedLoopJoinMetrics metrics_;
    Tuple outer_row_{{}};
    bool have_outer_ = false;
};

}  // namespace valencedb
