#pragma once

#include "valencedb/execution/block_nested_loop_join.hpp"
#include <unordered_map>

namespace valencedb {

struct GraceHashJoinPlan {
    size_t left_key, right_key;
    std::optional<size_t> memory_bytes = std::nullopt;
};
struct GraceHashJoinMemory {
    size_t max_bytes, left_row_bytes, right_row_bytes, scratch_bytes;
    size_t partition_count, descriptor_bytes, left_build_capacity, right_build_capacity;
    size_t fallback_memory, left_block_capacity, right_block_capacity;
};
GraceHashJoinMemory ComputeGraceJoinMemory(const QueryResources&, const Schema& left,
    const Schema& right, const Schema& output, std::optional<size_t> memory_bytes = std::nullopt);
struct GraceHashJoinMetrics {
    size_t hash_pairs = 0, fallback_pairs = 0, empty_pairs = 0;
    size_t left_build_pairs = 0, right_build_pairs = 0;
    size_t peak_build_rows = 0, modeled_peak_build_bytes = 0, output_rows = 0;
    size_t peak_partition_writers = 0;
    size_t left_fallback_pairs = 0, right_fallback_pairs = 0;
    size_t completed_fallback_inner_scans = 0; // Actual BNLJ scans, counted at pair EOF.
    size_t failed_build_rows = 0; // Extra prefix work on the defensive runtime-cap path.
};

class GraceHashJoin final : public Executor {
public:
    GraceHashJoin(QueryContext& context, GraceHashJoinPlan plan,
                  std::unique_ptr<Executor> left, std::unique_ptr<Executor> right);
    ~GraceHashJoin() noexcept override { Close(); }
    void Init() override;
    bool Next(Tuple& out) override;
    void Close() noexcept override;
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
    const GraceHashJoinMemory& Memory() const noexcept { return memory_; }
    const GraceHashJoinMetrics& Metrics() const noexcept { return metrics_; }
    static uint64_t PartitionHash(int64_t key) noexcept;
private:
    friend struct GraceHashJoinTestAccess; // Exercises the otherwise unreachable runtime cap.
    enum class Phase { Closed, SelectPair, Probe, Fallback, Done };
    struct BuildHash { size_t operator()(int64_t key) const noexcept; };
    using Table = std::unordered_multimap<int64_t, Tuple, BuildHash>;
    using Partitions = std::vector<TempRun>;

    GraceHashJoinPlan plan_;
    std::unique_ptr<Executor> left_, right_;
    const Schema schema_, left_schema_, right_schema_; // Outlive all borrowed temp schemas.
    GraceHashJoinMemory memory_{};
    GraceHashJoinMetrics metrics_;
    Phase phase_ = Phase::Closed;
    Partitions left_parts_, right_parts_;
    size_t pair_ = 0;
    PhysicalOuter build_side_ = PhysicalOuter::Left;
    std::unique_ptr<Table> table_;
    std::unique_ptr<TempReader> probe_;
    Tuple probe_row_{{}};
    Table::const_iterator match_{}, match_end_{};
    std::unique_ptr<BlockNestedLoopJoin> fallback_;

    void Partition(Executor& child, const Schema& schema, size_t key, Partitions& parts);
    bool SelectPair();
    bool Build(size_t capacity);
    void BeginFallback();
    void ReleasePair() noexcept;
};

}  // namespace valencedb
