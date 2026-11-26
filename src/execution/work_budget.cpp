#include "valencedb/execution/work_budget.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/page_codec.hpp"
#include <limits>
#include <algorithm>
#include "valencedb/execution/aggregate.hpp"
#include "valencedb/execution/block_nested_loop_join.hpp"
#include "valencedb/execution/grace_hash_join.hpp"

namespace valencedb {

void QueryResources::Validate() const {
    if (work_memory_bytes == 0 || max_temp_pages == 0 || max_temp_pages > kInvalidPageId)
        throw DbError("query memory and temporary page limits must be positive and representable");
}
size_t ResolveBudget(const QueryResources& resources, std::optional<size_t> local) {
    const size_t bytes = local.value_or(resources.work_memory_bytes);
    if (bytes == 0) throw DbError("operator memory must be positive");
    return bytes;
}
size_t CheckedAdd(size_t a, size_t b) {
    if (b > std::numeric_limits<size_t>::max() - a) throw DbError("size addition overflow");
    return a + b;
}
size_t CheckedMultiply(size_t a, size_t b) {
    if (a != 0 && b > std::numeric_limits<size_t>::max() / a)
        throw DbError("size multiplication overflow");
    return a * b;
}
size_t RemainingMemory(size_t allowance, size_t reserve) {
    if (allowance < reserve) throw DbError("operator memory cannot cover reserved scratch");
    return allowance - reserve;
}
size_t RowMemoryCharge(size_t columns, size_t width) {
    const size_t bytes = CheckedAdd(CheckedAdd(64, CheckedMultiply(32, columns)),
                                    CheckedMultiply(2, width));
    return CheckedAdd(bytes, 7) / 8 * 8;
}
size_t RowMemoryCharge(const Schema& schema) {
    return RowMemoryCharge(schema.size(), schema.max_encoded_bytes());
}
size_t FixedScratchCharge(size_t maximum_row_charge) {
    return CheckedAdd(2 * kPageSize + 1024, CheckedMultiply(3, maximum_row_charge));
}
size_t HashBuildRowCharge(const Schema& schema) { return CheckedAdd(RowMemoryCharge(schema), 96); }
void ValidateInt64Key(const Schema& schema, size_t column) {
    const auto& key = schema.column(column);
    if (key.type != TypeId::Int64 || key.nullable) throw DbError("key must be NONNULL INT64");
}
Schema IntermediateSchema(std::vector<Column> columns) {
    return Schema(SchemaKind::Intermediate, std::move(columns));
}

ExternalSortMemory ComputeSortMemory(const QueryResources& resources, const Schema& schema,
                              std::optional<size_t> override_bytes) {
    ExternalSortMemory m{};
    m.max_bytes = ResolveBudget(resources, override_bytes);
    m.row_bytes = RowMemoryCharge(schema);
    m.scratch_bytes = FixedScratchCharge(m.row_bytes);
    m.descriptor_capacity = std::min(size_t{4096}, m.max_bytes / (16 * kTempRunCharge));
    if (m.descriptor_capacity == 0) throw DbError("sort memory cannot hold run descriptors");
    m.descriptor_bytes = CheckedMultiply(CheckedMultiply(2, m.descriptor_capacity), kTempRunCharge);
    const size_t reserve = CheckedAdd(m.scratch_bytes, m.descriptor_bytes);
    if (m.max_bytes <= reserve) throw DbError("sort memory cannot cover scratch and descriptors");
    const size_t available = RemainingMemory(m.max_bytes, reserve);
    m.run_capacity = available / m.row_bytes;
    if (m.run_capacity == 0 || m.run_capacity > std::vector<Tuple>().max_size())
        throw DbError("sort run capacity is zero or unrepresentable");
    m.merge_fan_in = std::min(size_t{64}, available / CheckedAdd(kPageSize + kTempHandleOverhead, m.row_bytes));
    return m;
}

BlockNestedLoopJoinMemory ComputeBlockJoinMemory(const QueryResources& resources,
    const Schema& outer, const Schema& inner, const Schema& output, std::optional<size_t> memory_bytes) {
    BlockNestedLoopJoinMemory m{};
    m.max_bytes = ResolveBudget(resources, memory_bytes);
    m.outer_row_bytes = RowMemoryCharge(outer);
    const size_t maximum_row = std::max({m.outer_row_bytes,
        RowMemoryCharge(inner), RowMemoryCharge(output)});
    m.scratch_bytes = FixedScratchCharge(maximum_row);
    if (m.max_bytes <= m.scratch_bytes)
        throw DbError("block join memory cannot cover scratch");
    m.block_capacity = RemainingMemory(m.max_bytes, m.scratch_bytes)
                             / m.outer_row_bytes;
    if (m.block_capacity == 0 || m.block_capacity > std::vector<Tuple>().max_size())
        throw DbError("block join capacity is zero or unrepresentable");
    return m;
}

GraceHashJoinMemory ComputeGraceJoinMemory(const QueryResources& resources,
    const Schema& left, const Schema& right, const Schema& output, std::optional<size_t> memory_bytes) {
    GraceHashJoinMemory m{};
    m.max_bytes = ResolveBudget(resources, memory_bytes);
    m.left_row_bytes = RowMemoryCharge(left);
    m.right_row_bytes = RowMemoryCharge(right);
    m.scratch_bytes = FixedScratchCharge(std::max({m.left_row_bytes,
        m.right_row_bytes, RowMemoryCharge(output)}));
    if (m.max_bytes <= m.scratch_bytes)
        throw DbError("Grace join memory cannot cover scratch");
    const size_t available = RemainingMemory(m.max_bytes, m.scratch_bytes);
    // Both run arrays coexist with one side's writers.
    constexpr size_t pair_bytes = 2 * kTempRunCharge;
    m.partition_count = std::min(size_t{64},
        available / (kPageSize + kTempHandleOverhead + pair_bytes));
    if (m.partition_count < 2) throw DbError("Grace join requires at least two partitions");
    m.descriptor_bytes = CheckedMultiply(pair_bytes, m.partition_count);
    const size_t reserve = CheckedAdd(m.scratch_bytes, m.descriptor_bytes);
    if (m.max_bytes <= reserve) throw DbError("Grace join cannot cover descriptors");
    const size_t build_bytes = RemainingMemory(m.max_bytes, reserve);
    m.left_build_capacity = build_bytes / HashBuildRowCharge(left);
    m.right_build_capacity = build_bytes / HashBuildRowCharge(right);
    m.fallback_memory = RemainingMemory(m.max_bytes, m.descriptor_bytes);
    m.left_block_capacity = build_bytes / m.left_row_bytes;
    m.right_block_capacity = build_bytes / m.right_row_bytes;
    const size_t block_max = std::vector<Tuple>().max_size();
    if (m.left_block_capacity > block_max) m.left_block_capacity = 0;
    if (m.right_block_capacity > block_max) m.right_block_capacity = 0;
    if (!m.left_block_capacity && !m.right_block_capacity)
        throw DbError("Grace join fallback cannot hold a legal outer block");
    if (m.left_build_capacity > std::unordered_multimap<int64_t, Tuple>().max_size() ||
        m.right_build_capacity > std::unordered_multimap<int64_t, Tuple>().max_size())
        throw DbError("Grace join build capacity is unrepresentable");
    return m;
}

namespace {
AggregateMemory MemoryFor(const QueryResources& resources, std::optional<size_t> bytes,
                          size_t maximum_row_bytes) {
    AggregateMemory m;
    m.max_bytes = ResolveBudget(resources, bytes);
    m.maximum_row_bytes = maximum_row_bytes;
    m.scratch_bytes = FixedScratchCharge(maximum_row_bytes);
    m.combine_bytes = CheckedAdd(CheckedMultiply(3, maximum_row_bytes), 1024);
    // Global and ordered streaming need only combine scratch, with no staging pages.
    RemainingMemory(m.max_bytes, m.combine_bytes);
    return m;
}
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
    m.sort_memory_bytes = RemainingMemory(m.max_bytes, CheckedAdd(m.combine_bytes, kPageSize + kTempHandleOverhead));
    return m;
}

AggregateMemory ComputeSortAggregateMemory(const QueryResources& resources, const Schema& input,
    const Schema& output, bool needs_sort, std::optional<size_t> bytes) {
    auto m = MemoryFor(resources, bytes, std::max(RowMemoryCharge(input), RowMemoryCharge(output)));
    if (needs_sort) m.sort_memory_bytes = RemainingMemory(m.max_bytes, m.combine_bytes);
    return m;
}

}  // namespace valencedb
