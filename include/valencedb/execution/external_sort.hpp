#pragma once

#include "valencedb/execution/executor.hpp"
#include <memory>
#include <queue>

namespace valencedb {

// One ascending NONNULL INT64 output column; no direction or multikey configuration.
struct ExternalSortPlan {
    size_t column;
    std::optional<size_t> memory_bytes = std::nullopt;
};
struct ExternalSortMemory {
    size_t max_bytes, row_bytes, scratch_bytes;
    size_t descriptor_capacity, descriptor_bytes, run_capacity, merge_fan_in;
};
ExternalSortMemory ComputeSortMemory(const QueryResources&, const Schema&,
                                     std::optional<size_t> override_bytes = std::nullopt);
struct ExternalSortMetrics {
    size_t initial_runs = 0; // Includes a nonempty private run.
    size_t materialized_runs = 0;
    size_t intermediate_merge_passes = 0; // Completed materialized levels only.
    size_t final_merge_fan_in = 0;
    bool final_merge_streamed = false;
    size_t peak_retained_rows = 0; // Normal run rows or merge heads; excludes fixed scratch.
};

class ExternalSort final : public Executor {
public:
    ExternalSort(QueryContext& context, ExternalSortPlan plan, std::unique_ptr<Executor> child);
    ~ExternalSort() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {true, plan_.column}; }
    const ExternalSortMemory& Memory() const noexcept { return memory_; }
    const ExternalSortMetrics& Metrics() const noexcept { return metrics_; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    using Runs = std::vector<TempRun>;
    struct MergeInput {
        std::unique_ptr<TempReader> reader;
        Tuple head{{}}; // The only owned head for this input.
    };
    struct HeapEntry { int64_t key; size_t input; };
    struct GreaterKey {
        bool operator()(const HeapEntry& a, const HeapEntry& b) const { return a.key > b.key; }
    };
    using Heap = std::priority_queue<HeapEntry, std::vector<HeapEntry>, GreaterKey>;

    ExternalSortPlan plan_;
    std::unique_ptr<Executor> child_;
    const Schema schema_; // Outlives every run; never copied into descriptors.
    ExternalSortMemory memory_;
    ExternalSortMetrics metrics_;
    std::vector<Tuple> rows_;
    size_t position_ = 0;
    Runs current_runs_;
    std::vector<MergeInput> inputs_;
    Heap heap_;

    int64_t Key(const Tuple& row) const;
    void GenerateRuns();
    void SpillRun();
    void MergePasses();
    void OpenMerge(size_t first, size_t count);
    bool PullMerge(Tuple& out);
    void CloseMerge() noexcept;
};

}  // namespace valencedb
