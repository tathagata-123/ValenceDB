#pragma once

#include "valencedb/execution/nested_loop_join.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb::detail {

inline Schema JoinSchema(QueryContext& context, const JoinPlan& plan,
                         const std::unique_ptr<Executor>& left,
                         const std::unique_ptr<Executor>& right) {
    if (!left || !right || &left->Context() != &context || &right->Context() != &context)
        throw DbError("join children must belong to this query");
    if (plan.outer != PhysicalOuter::Left && plan.outer != PhysicalOuter::Right)
        throw DbError("invalid physical outer side");
    const auto& l = left->OutputSchema();
    const auto& r = right->OutputSchema();
    ValidateInt64Key(l, plan.left_key);
    ValidateInt64Key(r, plan.right_key);
    std::vector<Column> columns;
    for (size_t i = 0; i < l.size(); ++i) columns.push_back(l.column(i));
    for (size_t i = 0; i < r.size(); ++i) columns.push_back(r.column(i));
    return IntermediateSchema(std::move(columns));
}

inline Tuple JoinedTuple(const Tuple& outer, const Tuple& inner, PhysicalOuter side) {
    const Tuple& left = side == PhysicalOuter::Left ? outer : inner;
    const Tuple& right = side == PhysicalOuter::Left ? inner : outer;
    std::vector<Value> values;
    values.reserve(left.size() + right.size());
    for (size_t i = 0; i < left.size(); ++i) values.push_back(left.value(i));
    for (size_t i = 0; i < right.size(); ++i) values.push_back(right.value(i));
    return Tuple(std::move(values));
}

}  // namespace valencedb::detail
