#include "valencedb/execution/work_budget.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/page_codec.hpp"
#include <limits>

namespace valencedb {

void QueryResources::Validate() const {
    if (work_memory_bytes == 0 || max_temp_pages == 0 || max_temp_pages > kInvalidPageId)
        throw DbError("query memory and temporary page limits must be positive and representable");
}
OperatorMemoryBudget ResolveBudget(const QueryResources& resources, std::optional<size_t> local) {
    resources.Validate();
    const size_t bytes = local.value_or(resources.work_memory_bytes);
    if (bytes == 0) throw DbError("operator memory must be positive");
    return {bytes};
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

}  // namespace valencedb
