#pragma once

#include "valencedb/execution/work_budget.hpp"
#include "valencedb/types/tuple.hpp"

namespace valencedb {

struct Ordering {
    bool ordered = false;
    size_t column = 0;
    bool operator==(const Ordering&) const = default;
};
struct SeqScanPlan { std::string table; bool use_scan_ring = false; };
// No bounds means full traversal; equal bounds mean equality; reversed bounds mean empty.
struct IndexScanPlan {
    std::string table;
    size_t column;
    std::optional<int64_t> low = std::nullopt;
    std::optional<int64_t> high = std::nullopt;
};
enum class CompareOp { Equal, Less, LessEqual, Greater, GreaterEqual };
struct Int64Predicate {
    size_t column;
    CompareOp op;
    int64_t constant;
    void Validate(const Schema& schema) const;
    bool Matches(const Tuple& tuple) const;
};
struct FilterPlan { Int64Predicate predicate; };
struct ProjectionPlan { std::vector<size_t> columns; };
Schema ProjectedSchema(const Schema& input, const ProjectionPlan& plan);
Ordering ProjectedOrdering(Ordering input, const ProjectionPlan& plan);
// Descriptors carry configuration only. Unary/binary child ownership belongs to executors.
// Later heavyweight descriptors can use the same optional numeric budget override.

}  // namespace valencedb
