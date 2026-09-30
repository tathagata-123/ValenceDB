#include "valencedb/execution/aggregate.hpp"
#include "valencedb/execution/temp_scan.hpp"
#include "valencedb/common/error.hpp"
#include <algorithm>
#include <cassert>
#include <limits>

namespace valencedb {
namespace {
int64_t AddInt64(int64_t a, int64_t b) {
    if ((b > 0 && a > std::numeric_limits<int64_t>::max() - b) ||
        (b < 0 && a < std::numeric_limits<int64_t>::min() - b))
        throw DbError("aggregate INT64 overflow");
    return a + b;
}
bool NeedsMeasure(const AggregateSpec& s) { return s.sum || s.min || s.max || s.avg; }
}  // namespace
Schema AggregateOutputSchema(const Schema& input, const AggregateSpec& s) {
    if (!s.count && !NeedsMeasure(s)) throw DbError("aggregate requires at least one output");
    if (s.group_column) ValidateInt64Key(input, *s.group_column);
    if (NeedsMeasure(s) && !s.measure_column) throw DbError("aggregate requires a measure column");
    if (s.measure_column) ValidateInt64Key(input, *s.measure_column);
    std::vector<Column> columns;
    const bool nullable = !s.group_column;
    if (s.group_column) columns.push_back({"group", TypeId::Int64});
    if (s.count) columns.push_back({"count", TypeId::Int64});
    if (s.sum) columns.push_back({"sum", TypeId::Int64, 0, nullable});
    if (s.min) columns.push_back({"min", TypeId::Int64, 0, nullable});
    if (s.max) columns.push_back({"max", TypeId::Int64, 0, nullable});
    if (s.avg) columns.push_back({"avg", TypeId::Double, 0, nullable});
    return IntermediateSchema(std::move(columns));
}
Schema AggregatePartialSchema() {
    return IntermediateSchema({{"group", TypeId::Int64}, {"count", TypeId::Int64},
        {"sum", TypeId::Int64}, {"min", TypeId::Int64}, {"max", TypeId::Int64},
        {"has_value", TypeId::Int64}});
}
namespace {
const Schema& AggregateInput(QueryContext& context, const std::unique_ptr<Executor>& child) {
    if (!child || &child->Context() != &context) throw DbError("aggregate child must belong to this query");
    return child->OutputSchema();
}
AggregateMemory MemoryFor(const QueryResources& resources, std::optional<size_t> bytes,
                          size_t maximum_row_bytes) {
    AggregateMemory m;
    m.max_bytes = ResolveBudget(resources, bytes).max_bytes;
    m.maximum_row_bytes = maximum_row_bytes;
    m.scratch_bytes = FixedScratchCharge(maximum_row_bytes);
    m.combine_bytes = CheckedAdd(CheckedMultiply(3, maximum_row_bytes), 1024);
    // Global and ordered streaming need only combine scratch, with no staging pages.
    RemainingMemory(m.max_bytes, m.combine_bytes);
    return m;
}
int64_t Measure(const Tuple& row, const AggregateSpec& s) {
    return NeedsMeasure(s) ? row.value(*s.measure_column).as_int64() : 0;
}
Tuple Finalize(int64_t key, const AggregateState& state, const AggregateSpec& s) {
    std::vector<Value> values;
    if (s.group_column) values.emplace_back(key);
    if (s.count) values.emplace_back(state.count);
    if (s.sum) values.push_back(state.has_value ? Value(state.sum) : Value::Null(TypeId::Int64));
    if (s.min) values.push_back(state.has_value ? Value(state.min) : Value::Null(TypeId::Int64));
    if (s.max) values.push_back(state.has_value ? Value(state.max) : Value::Null(TypeId::Int64));
    if (s.avg) values.push_back(state.has_value
        ? Value(static_cast<double>(state.sum) / static_cast<double>(state.count))
        : Value::Null(TypeId::Double));
    return Tuple(std::move(values));
}
AggregateState ReadPartial(const Tuple& row) {
    return {row.value(1).as_int64(), row.value(2).as_int64(), row.value(3).as_int64(),
            row.value(4).as_int64(), row.value(5).as_int64() != 0};
}
AggregateState ConsumeGlobal(Executor& child, const AggregateSpec& spec, AggregateMetrics& metrics) {
    AggregateState state;
    Tuple row({});
    while (child.Next(row)) {
        ++metrics.input_rows;
        state.Update(Measure(row, spec), spec);
    }
    child.Close();
    return state;
}
// Two concrete uses: raw ordered rows, or ordered partial states. One boundary row survives.
bool NextGroup(Executor& input, const AggregateSpec& spec, bool partials,
               std::optional<Tuple>& lookahead, AggregateMetrics& metrics, Tuple& out) {
    Tuple row({});
    if (lookahead) {
        row = std::move(*lookahead);
        lookahead.reset();
    } else {
        if (!input.Next(row)) return false;
        if (!partials) ++metrics.input_rows;
    }
    const size_t column = partials ? 0 : *spec.group_column;
    const int64_t key = row.value(column).as_int64();
    AggregateState state;
    for (;;) {
        if (partials) state.Combine(ReadPartial(row), spec);
        else state.Update(Measure(row, spec), spec);
        if (!input.Next(row)) break;
        if (!partials) ++metrics.input_rows;
        if (row.value(column).as_int64() != key) {
            lookahead = std::move(row);
            break;
        }
    }
    out = Finalize(key, state, spec);
    return true;
}
}  // namespace

void AggregateState::Update(int64_t value, const AggregateSpec& s) {
    if (s.count || s.avg) count = AddInt64(count, 1);
    if (s.sum || s.avg) sum = AddInt64(sum, value);
    if (s.min && (!has_value || value < min)) min = value;
    if (s.max && (!has_value || value > max)) max = value;
    if (NeedsMeasure(s)) has_value = true;
}
void AggregateState::Combine(const AggregateState& partial, const AggregateSpec& s) {
    if (s.count || s.avg) count = AddInt64(count, partial.count);
    if (s.sum || s.avg) sum = AddInt64(sum, partial.sum);
    if (s.min && partial.has_value && (!has_value || partial.min < min)) min = partial.min;
    if (s.max && partial.has_value && (!has_value || partial.max > max)) max = partial.max;
    if (NeedsMeasure(s)) has_value = has_value || partial.has_value;
}

AggregateMemory ComputeHashAggregateMemory(const QueryResources& resources, const Schema& input,
    const Schema& output, bool grouped, std::optional<size_t> bytes) {
    auto m = MemoryFor(resources, bytes, std::max({RowMemoryCharge(input), RowMemoryCharge(output),
        grouped ? RowMemoryCharge(AggregatePartialSchema()) : size_t{0}}));
    if (!grouped) return m;
    if (m.max_bytes <= m.scratch_bytes) throw DbError("hash aggregate cannot cover scratch");
    m.hash_group_capacity = RemainingMemory(m.max_bytes, m.scratch_bytes) / kAggregateGroupCharge;
    if (!m.hash_group_capacity || m.hash_group_capacity > std::unordered_map<int64_t, AggregateState>().max_size())
        throw DbError("hash aggregate group capacity is zero or unrepresentable");
    m.sort_memory_bytes = RemainingMemory(m.max_bytes, CheckedAdd(m.combine_bytes, kPageSize));
    return m;
}
AggregateMemory ComputeSortAggregateMemory(const QueryResources& resources, const Schema& input,
    const Schema& output, bool needs_sort, std::optional<size_t> bytes) {
    auto m = MemoryFor(resources, bytes, std::max(RowMemoryCharge(input), RowMemoryCharge(output)));
    if (needs_sort) m.sort_memory_bytes = RemainingMemory(m.max_bytes, m.combine_bytes);
    return m;
}

HashAggregate::HashAggregate(QueryContext& context, AggregateSpec spec,
    std::unique_ptr<Executor> child, std::optional<size_t> bytes)
    : Executor(context), spec_(spec), child_(std::move(child)),
      schema_(AggregateOutputSchema(AggregateInput(context, child_), spec)), partial_schema_(AggregatePartialSchema()),
      memory_(ComputeHashAggregateMemory(context.Resources(), child_->OutputSchema(), schema_,
          spec.group_column.has_value(), bytes)) {}

void HashAggregate::ConsumeHash() {
    table_ = std::make_unique<Table>();
    table_->max_load_factor(1.0f);
    table_->reserve(memory_.hash_group_capacity);
    metrics_.modeled_peak_hash_bytes = CheckedAdd(memory_.scratch_bytes,
        CheckedMultiply(memory_.hash_group_capacity, kAggregateGroupCharge));
    Tuple row({}); // Owns the triggering row throughout FlushBatch; never refetch it.
    while (child_->Next(row)) {
        ++metrics_.input_rows;
        const int64_t key = row.value(*spec_.group_column).as_int64();
        auto found = table_->find(key);
        if (found == table_->end()) {
            if (table_->size() == memory_.hash_group_capacity) {
                FlushBatch();
                ++metrics_.overflow_flushes;
            }
            found = table_->emplace(key, AggregateState{}).first;
            metrics_.peak_hash_groups = std::max(metrics_.peak_hash_groups, table_->size());
        }
        found->second.Update(Measure(row, spec_), spec_);
        assert(table_->size() <= memory_.hash_group_capacity);
    }
}

void HashAggregate::FlushBatch() {
    if (table_->empty()) return;
    if (!partials_) {
        partials_ = std::make_unique<TempRelation>(context_.Temps(), partial_schema_);
        writer_ = std::make_unique<TempWriter>(*partials_);
    }
    for (const auto& [key, state] : *table_) {
        writer_->Append(Tuple({Value(key), Value(state.count), Value(state.sum),
            Value(state.min), Value(state.max), Value(int64_t{state.has_value})}));
        ++metrics_.spill_records;
    }
    ++metrics_.spill_batches; // Every nonempty completed batch, including final flush.
    table_->clear(); // Reuse reserved buckets only within the hash phase.
}

void HashAggregate::SortPartials() {
    assert(!table_ && !writer_ && partials_->Sealed());
    phase_ = Phase::SortPartials;
    // TempScan's reader owns a page outside ExternalSort's own model: subtract P.
    sort_ = std::make_unique<ExternalSort>(context_, ExternalSortPlan{0, memory_.sort_memory_bytes},
        std::make_unique<TempScan>(context_, *partials_));
    metrics_.used_internal_sort = true;
    sort_->Init();
    metrics_.sort = sort_->Metrics();
    phase_ = Phase::CombinePartials;
}

void HashAggregate::Init() {
    Start();
    try {
        metrics_ = {};
        global_ = {};
        child_->Init();
        phase_ = Phase::ConsumeHash;
        if (!spec_.group_column) {
            global_ = ConsumeGlobal(*child_, spec_, metrics_);
            phase_ = Phase::EmitGlobal;
            return;
        }
        ConsumeHash(); // Its input/trigger tuple dies before starting the sort phase.
        if (!partials_) {
            child_->Close();
            position_ = table_->cbegin();
            phase_ = Phase::EmitHash;
            return;
        }
        phase_ = Phase::FlushFinal;
        FlushBatch();
        writer_->Seal();
        writer_.reset(); // Destroy the staging array, not just its handle registration.
        child_->Close();
        table_.reset(); // Destroy nodes AND reserved buckets before ExternalSort exists.
        SortPartials();
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("hash aggregate memory allocation failed");
    } catch (const std::length_error&) {
        Close(); throw DbError("hash aggregate allocation size is unrepresentable");
    } catch (...) { Close(); throw; }
}

bool HashAggregate::Next(Tuple& out) {
    if (!Ready()) return false;
    try {
        if (phase_ == Phase::EmitGlobal) {
            out = Finalize(0, global_, spec_);
            phase_ = Phase::Done;
        } else if (phase_ == Phase::EmitHash && position_ != table_->cend()) {
            out = Finalize(position_->first, position_->second, spec_);
            ++position_;
        } else if (phase_ == Phase::CombinePartials &&
                   NextGroup(*sort_, spec_, true, lookahead_, metrics_, out)) {
            // One combined output only; the boundary partial remains in lookahead_.
        } else {
            phase_ = Phase::Done;
            return End();
        }
        ++metrics_.output_groups;
        return true;
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("hash aggregate memory allocation failed");
    } catch (...) { Close(); throw; }
}

void HashAggregate::Close() noexcept {
    child_->Close();
    if (sort_) metrics_.sort = sort_->Metrics();
    sort_.reset(); // Destroys TempScan before its borrowed relation.
    writer_.reset();
    partials_.reset();
    position_ = Table::const_iterator{};
    table_.reset();
    lookahead_.reset();
    global_ = {};
    phase_ = Phase::Closed;
    CloseState();
}

SortAggregate::SortAggregate(QueryContext& context, AggregateSpec spec,
    std::unique_ptr<Executor> child, std::optional<size_t> bytes)
    : Executor(context), spec_(spec), child_(std::move(child)),
      schema_(AggregateOutputSchema(AggregateInput(context, child_), spec)),
      memory_(ComputeSortAggregateMemory(context.Resources(), child_->OutputSchema(), schema_,
          spec.group_column && child_->OutputOrdering() != Ordering{true, *spec.group_column}, bytes)) {
    if (spec.group_column && child_->OutputOrdering() != Ordering{true, *spec.group_column}) {
        sort_ = std::make_unique<ExternalSort>(context_,
            ExternalSortPlan{*spec.group_column, memory_.sort_memory_bytes}, std::move(child_));
    }
}

void SortAggregate::Init() {
    Start();
    try {
        metrics_ = {};
        global_emitted_ = false;
        metrics_.used_internal_sort = bool(sort_);
        metrics_.input_order_reused = spec_.group_column.has_value() && !sort_;
        Input().Init();
        if (sort_) metrics_.sort = sort_->Metrics();
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("sort aggregate memory allocation failed");
    } catch (...) { Close(); throw; }
}

bool SortAggregate::Next(Tuple& out) {
    if (!Ready()) return false;
    try {
        if (!spec_.group_column) {
            if (global_emitted_) return End();
            out = Finalize(0, ConsumeGlobal(Input(), spec_, metrics_), spec_);
            global_emitted_ = true;
        } else if (!NextGroup(Input(), spec_, false, lookahead_, metrics_, out)) {
            return End();
        }
        ++metrics_.output_groups;
        return true;
    } catch (const std::bad_alloc&) {
        Close(); throw DbError("sort aggregate memory allocation failed");
    } catch (...) { Close(); throw; }
}

void SortAggregate::Close() noexcept {
    Input().Close(); // Keep the sorter/child tree for legal whole-execution replay.
    if (sort_) metrics_.sort = sort_->Metrics();
    lookahead_.reset();
    global_emitted_ = false;
    CloseState();
}

}  // namespace valencedb
