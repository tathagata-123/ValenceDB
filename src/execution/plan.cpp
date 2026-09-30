#include "valencedb/execution/plan.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

void Int64Predicate::Validate(const Schema& schema) const {
    ValidateInt64Key(schema, column);
    switch (op) {
        case CompareOp::Equal: case CompareOp::Less: case CompareOp::LessEqual:
        case CompareOp::Greater: case CompareOp::GreaterEqual: return;
    }
    throw DbError("unsupported predicate comparison");
}
bool Int64Predicate::Matches(const Tuple& tuple) const {
    const int64_t value = tuple.value(column).as_int64();
    switch (op) {
        case CompareOp::Equal: return value == constant;
        case CompareOp::Less: return value < constant;
        case CompareOp::LessEqual: return value <= constant;
        case CompareOp::Greater: return value > constant;
        case CompareOp::GreaterEqual: return value >= constant;
    }
    throw DbError("unsupported predicate comparison");
}

Schema ProjectedSchema(const Schema& input, const ProjectionPlan& plan) {
    if (plan.columns.empty() || plan.columns.size() > 16) throw DbError("projection requires 1 to 16 columns");
    std::vector<Column> columns;
    for (size_t column : plan.columns) columns.push_back(input.column(column));
    return IntermediateSchema(std::move(columns));
}
Ordering ProjectedOrdering(Ordering input, const ProjectionPlan& plan) {
    if (input.ordered) {
        for (size_t i = 0; i < plan.columns.size(); ++i)
            if (plan.columns[i] == input.column) return {true, i};
    }
    return {};
}

}  // namespace valencedb
