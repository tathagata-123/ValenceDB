#pragma once

#include "valencedb/types/schema.hpp"
#include <optional>

namespace valencedb {

struct QueryResources {
    size_t work_memory_bytes = 1024 * 1024;
    size_t max_temp_pages = 16384;
    void Validate() const;
};
// Capacity helpers check the selected memory allowance; QueryContext validates query resources.
size_t ResolveBudget(const QueryResources& resources,
                                   std::optional<size_t> override_bytes = std::nullopt);
size_t CheckedAdd(size_t a, size_t b);
size_t CheckedMultiply(size_t a, size_t b);
size_t RemainingMemory(size_t allowance, size_t reserve);
size_t RowMemoryCharge(size_t columns, size_t max_encoded_bytes);
size_t RowMemoryCharge(const Schema& schema);
size_t FixedScratchCharge(size_t maximum_row_charge);
size_t HashBuildRowCharge(const Schema& schema);
inline constexpr size_t kAggregateGroupCharge = 192;
void ValidateInt64Key(const Schema& schema, size_t column);
// Schema construction already checks the shared 16-column / 2048-byte limits.
Schema IntermediateSchema(std::vector<Column> columns);

}  // namespace valencedb
