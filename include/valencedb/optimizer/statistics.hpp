#pragma once

#include "valencedb/execution/plan.hpp"

namespace valencedb {

inline constexpr size_t kStatisticsSampleLimit = 1024;
struct ColumnStatistics {
    std::optional<int64_t> minimum, maximum;
    long double approximate_ndv = 0;
    std::vector<int64_t> sample; // Deterministic reservoir, never more than 1024 values.
};
struct TableStatistics {
    uint64_t analyzed_rows = 0;
    bool stale = false;
    std::vector<ColumnStatistics> columns; // VARCHAR entries stay empty.
};
// Advisory only: these functions never simplify or remove executable predicates.
long double EstimateNDV(const ColumnStatistics* stats, long double rows);
long double EstimateSelectivity(const ColumnStatistics* stats, long double rows,
                                const Int64Predicate& predicate);

}  // namespace valencedb
