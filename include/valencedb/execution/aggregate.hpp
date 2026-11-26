#pragma once

#include "valencedb/execution/external_sort.hpp"
#include <unordered_map>

namespace valencedb {

struct AggregateSpec {
    std::optional<size_t> group_column, measure_column;
    bool count = false, sum = false, min = false, max = false, avg = false;
};
// Raw combinable fields, never finalized averages. Only selected fields are updated.
struct AggregateState {
    int64_t count = 0, sum = 0, min = 0, max = 0;
    bool has_value = false;
    void Update(int64_t value, const AggregateSpec& spec);
    void Combine(const AggregateState& partial, const AggregateSpec& spec);
};
struct AggregateMemory {
    size_t max_bytes = 0, maximum_row_bytes = 0, scratch_bytes = 0, combine_bytes = 0;
    size_t hash_group_capacity = 0, sort_memory_bytes = 0;
};
Schema AggregateOutputSchema(const Schema&, const AggregateSpec&);
Schema AggregatePartialSchema();
AggregateMemory ComputeHashAggregateMemory(const QueryResources&, const Schema& input,
    const Schema& output, bool grouped, std::optional<size_t> bytes = std::nullopt);
AggregateMemory ComputeSortAggregateMemory(const QueryResources&, const Schema& input,
    const Schema& output, bool needs_sort, std::optional<size_t> bytes = std::nullopt);
struct AggregateMetrics {
    size_t input_rows = 0, peak_hash_groups = 0, modeled_peak_hash_bytes = 0;
    size_t overflow_flushes = 0, spill_batches = 0, spill_records = 0, output_groups = 0;
    bool used_internal_sort = false, input_order_reused = false;
    ExternalSortMetrics sort;
};

class HashAggregate final : public Executor {
public:
    HashAggregate(QueryContext&, AggregateSpec, std::unique_ptr<Executor> child,
                  std::optional<size_t> memory_bytes = std::nullopt);
    ~HashAggregate() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
    const AggregateMemory& Memory() const noexcept { return memory_; }
    const AggregateMetrics& Metrics() const noexcept { return metrics_; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    friend struct AggregateTestAccess;
    enum class Phase { Closed, EmitGlobal, EmitHash, CombinePartials, Done };
    using Table = std::unordered_map<int64_t, AggregateState>;
    AggregateSpec spec_;
    std::unique_ptr<Executor> child_;
    const Schema schema_, partial_schema_; // Outlive every borrowed temp descriptor.
    AggregateMemory memory_;
    AggregateMetrics metrics_;
    Phase phase_ = Phase::Closed;
    AggregateState global_;
    std::unique_ptr<Table> table_;
    Table::const_iterator position_{};
    std::unique_ptr<ExternalSort> sort_;
    std::optional<Tuple> lookahead_;
    std::optional<TempRun> BuildGroups();
    void FlushBatch(Table& groups, TempWriter& writer);
};

class SortAggregate final : public Executor {
public:
    SortAggregate(QueryContext&, AggregateSpec, std::unique_ptr<Executor> child,
                  std::optional<size_t> memory_bytes = std::nullopt);
    ~SortAggregate() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {spec_.group_column.has_value(), 0}; }
    const AggregateMemory& Memory() const noexcept { return memory_; }
    const AggregateMetrics& Metrics() const noexcept { return metrics_; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    friend struct AggregateTestAccess;
    AggregateSpec spec_;
    std::unique_ptr<Executor> input_; // Wrapped once at construction if sorting is needed.
    const Schema schema_;
    AggregateMemory memory_;
    AggregateMetrics metrics_;
    std::optional<Tuple> lookahead_;
    bool global_emitted_ = false;
};

}  // namespace valencedb
