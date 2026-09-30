#pragma once

#include "valencedb/execution/nested_loop_join.hpp"

namespace valencedb {

struct BlockNestedLoopJoinMemory {
    size_t max_bytes, outer_row_bytes, scratch_bytes, block_capacity;
};
BlockNestedLoopJoinMemory ComputeBlockJoinMemory(const QueryResources&, const Schema& outer,
    const Schema& inner, const Schema& output, std::optional<size_t> memory_bytes = std::nullopt);
struct BlockNestedLoopJoinMetrics {
    size_t outer_rows = 0, outer_blocks = 0, inner_scans = 0;
    size_t candidate_comparisons = 0, output_rows = 0, peak_block_rows = 0;
    size_t pending_boundary_tuples = 0, modeled_peak_bytes = 0;
};

class BlockNestedLoopJoin final : public Executor {
public:
    BlockNestedLoopJoin(QueryContext& context, JoinPlan plan,
                        std::unique_ptr<Executor> left, std::unique_ptr<Executor> right,
                        std::optional<size_t> memory_bytes = std::nullopt);
    ~BlockNestedLoopJoin() noexcept override { Close(); }
    void Init() override;
    bool Next(Tuple& out) override;
    void Close() noexcept override;
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
    const BlockNestedLoopJoinMemory& Memory() const noexcept { return memory_; }
    const BlockNestedLoopJoinMetrics& Metrics() const noexcept { return metrics_; }
private:
    JoinPlan plan_;
    std::unique_ptr<Executor> outer_, inner_;
    Schema schema_;
    size_t outer_key_, inner_key_;
    BlockNestedLoopJoinMemory memory_{};
    BlockNestedLoopJoinMetrics metrics_;
    std::vector<Tuple> block_;
    Tuple pending_{{}}, inner_row_{{}};
    size_t position_ = 0;
    bool have_pending_ = false, outer_exhausted_ = false, have_inner_ = false;
    bool FillBlock();
};

}  // namespace valencedb
