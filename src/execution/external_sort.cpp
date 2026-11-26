#include "valencedb/execution/external_sort.hpp"
#include "valencedb/common/error.hpp"
#include <algorithm>
#include <cassert>

namespace valencedb {
namespace {
Schema SortSchema(QueryContext& context, const std::unique_ptr<Executor>& child, size_t column) {
    if (!child || &child->Context() != &context) throw DbError("sort child must belong to this query");
    const auto& schema = child->OutputSchema();
    ValidateInt64Key(schema, column);
    // Schema construction already enforces column, type, and maximum encoded width limits.
    return schema;
}
}

ExternalSort::ExternalSort(QueryContext& context, ExternalSortPlan plan,
                           std::unique_ptr<Executor> child)
    : Executor(context), plan_(plan), child_(std::move(child)),
      schema_(SortSchema(context, child_, plan.column)),
      memory_(ComputeSortMemory(context.Resources(), schema_, plan.memory_bytes)) {}

int64_t ExternalSort::Key(const Tuple& row) const { return row.value(plan_.column).as_int64(); }

void ExternalSort::DoInit() {
    metrics_ = {};
    position_ = 0;
    current_runs_.reserve(memory_.descriptor_capacity);
    rows_.reserve(memory_.run_capacity);
    metrics_.modeled_peak_bytes = CheckedAdd(
        CheckedAdd(memory_.scratch_bytes, memory_.descriptor_bytes),
        CheckedMultiply(memory_.run_capacity, memory_.row_bytes));
    child_->Init();
    GenerateRuns();
    child_->Close();
    if (current_runs_.empty()) {
        output_mode_ = OutputMode::PrivateRun;
        return;
    }
    // Run rows and their capacity must be gone before allocating reader staging pages.
    std::vector<Tuple>().swap(rows_);
    MergePasses();
    OpenMerge(0, current_runs_.size());
    metrics_.final_merge_fan_in = current_runs_.size();
    metrics_.final_merge_streamed = true;
    output_mode_ = OutputMode::Merge;
}

void ExternalSort::GenerateRuns() {
    Tuple pending({});
    bool has_pending = false;
    for (;;) {
        if (has_pending) {
            rows_.push_back(std::move(pending));
            has_pending = false;
        }
        Tuple incoming({});
        while (child_->Next(incoming)) {
            if (rows_.size() == memory_.run_capacity) {
                pending = std::move(incoming); // One boundary tuple, charged to fixed scratch.
                has_pending = true;
                ++metrics_.pending_boundary_tuples;
                break;
            }
            rows_.push_back(std::move(incoming));
            metrics_.peak_retained_rows = std::max(metrics_.peak_retained_rows, rows_.size());
        }
        std::sort(rows_.begin(), rows_.end(), [this](const Tuple& a, const Tuple& b) {
            return Key(a) < Key(b);
        });
        if (current_runs_.empty() && !has_pending) {
            metrics_.initial_runs = rows_.empty() ? 0 : 1;
            return; // Empty or one bounded private run, including exactly-full input.
        }
        if (has_pending && memory_.merge_fan_in < 2)
            throw DbError("external sort requires merge fan-in of at least two");
        SpillRun();
        rows_.clear(); // Reuse capacity only within run generation.
        if (!has_pending) return;
    }
}

void ExternalSort::SpillRun() {
    if (current_runs_.size() >= memory_.descriptor_capacity)
        throw DbError("sort initial run descriptor cap exceeded");
    assert(!rows_.empty() && rows_.size() <= memory_.run_capacity);
    assert(std::is_sorted(rows_.begin(), rows_.end(), [this](const Tuple& a, const Tuple& b) {
        return Key(a) < Key(b);
    }));
    TempWriter writer(context_.Temps(), schema_);
    for (const auto& row : rows_) writer.Append(row);
    current_runs_.push_back(writer.Finish());
    ++metrics_.initial_runs;
    ++metrics_.materialized_runs;
    metrics_.peak_run_descriptors = std::max(metrics_.peak_run_descriptors, current_runs_.size());
}

void ExternalSort::CloseMerge() noexcept {
    Heap().swap(heap_);
    std::vector<MergeInput>().swap(inputs_); // Destroy even closed readers' staging arrays.
}

void ExternalSort::OpenMerge(size_t first, size_t count) {
    CloseMerge();
    assert(count > 0 && count <= memory_.merge_fan_in);
    inputs_.reserve(count);
    std::vector<HeapEntry> entries;
    entries.reserve(count);
    heap_ = Heap(GreaterKey{}, std::move(entries));
    const size_t merge_bytes = CheckedMultiply(count, CheckedAdd(kPageSize + kTempHandleOverhead, memory_.row_bytes));
    metrics_.modeled_peak_bytes = std::max(metrics_.modeled_peak_bytes,
        CheckedAdd(CheckedAdd(memory_.scratch_bytes, memory_.descriptor_bytes), merge_bytes));
    for (size_t i = 0; i < count; ++i) {
        inputs_.push_back({std::make_unique<TempReader>(current_runs_[first + i]), Tuple({})});
        auto& input = inputs_.back();
        if (input.reader->Next(input.head)) heap_.push({Key(input.head), i});
        else input.reader.reset();
    }
    metrics_.peak_merge_heads = std::max(metrics_.peak_merge_heads, heap_.size());
    metrics_.peak_retained_rows = std::max(metrics_.peak_retained_rows, heap_.size());
}

bool ExternalSort::PullMerge(Tuple& out) {
    if (heap_.empty()) return false;
    const auto entry = heap_.top();
    heap_.pop();
    auto& input = inputs_[entry.input];
    out = std::move(input.head);
    if (input.reader->Next(input.head)) heap_.push({Key(input.head), entry.input});
    else input.reader.reset();
    assert(heap_.size() <= inputs_.size() && inputs_.size() <= memory_.merge_fan_in);
    return true;
}

void ExternalSort::MergePasses() {
    Runs next_runs; // Exists only during blocking setup, never during streamed output.
    next_runs.reserve(memory_.descriptor_capacity);
    while (current_runs_.size() > memory_.merge_fan_in) {
        for (size_t first = 0; first < current_runs_.size();) {
            const size_t count = std::min(memory_.merge_fan_in, current_runs_.size() - first);
            if (next_runs.size() >= memory_.descriptor_capacity)
                throw DbError("sort merge run descriptor cap exceeded");
            if (count == 1) {
                next_runs.push_back(current_runs_[first]); // Carry the odd run.
            } else {
                OpenMerge(first, count);
                {
                    TempWriter writer(context_.Temps(), schema_);
                    Tuple row({});
                    while (PullMerge(row)) writer.Append(row);
                    next_runs.push_back(writer.Finish());
                }
                CloseMerge();
                ++metrics_.materialized_runs;
            }
            metrics_.peak_run_descriptors = std::max(metrics_.peak_run_descriptors, next_runs.size());
            first += count;
        }
        current_runs_.clear();
        current_runs_.swap(next_runs);
        ++metrics_.intermediate_merge_passes;
    }
}

bool ExternalSort::DoNext(Tuple& out) {
    if (output_mode_ == OutputMode::PrivateRun) {
        if (position_ == rows_.size()) return false;
        out = std::move(rows_[position_++]);
        return true;
    }
    if (!PullMerge(out)) return false;
    return true;
}

void ExternalSort::DoClose() noexcept {
    child_->Close();
    CloseMerge();
    std::vector<Tuple>().swap(rows_);
    Runs().swap(current_runs_);
    position_ = 0;
}

}  // namespace valencedb
