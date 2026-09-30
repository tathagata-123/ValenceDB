#include "valencedb/execution/basic_executors.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {
namespace {
const Schema& ChildSchema(QueryContext& context, const std::unique_ptr<Executor>& child) {
    if (!child || &child->Context() != &context) throw DbError("child must belong to this query");
    return child->OutputSchema();
}
}

SeqScan::SeqScan(QueryContext& context, SeqScanPlan plan)
    : Executor(context), plan_(std::move(plan)), schema_(context.Db().Table(plan_.table).schema) {}
void SeqScan::Init() {
    Start();
    try {
        if (plan_.use_scan_ring) ring_.emplace(context_.Db().Buffer());
        scan_.emplace(context_.Db().Scan(plan_.table, ring_ ? &*ring_ : nullptr));
    } catch (...) { Close(); throw; }
}
bool SeqScan::Next(Tuple& out) {
    if (!Ready()) return false;
    auto row = scan_->Next();
    if (!row) return End();
    out = std::move(row->second);
    return true;
}
void SeqScan::Close() noexcept { scan_.reset(); ring_.reset(); CloseState(); }

IndexScan::IndexScan(QueryContext& context, IndexScanPlan plan)
    : Executor(context), plan_(std::move(plan)), schema_(context.Db().Table(plan_.table).schema) {
    ValidateInt64Key(schema_, plan_.column);
    if (context.Db().Index(plan_.table).KeyColumn() != plan_.column)
        throw DbError("requested column does not match table index");
}
void IndexScan::Init() {
    Start();
    try {
        cursor_.emplace(context_.Db().IndexRange(plan_.table, plan_.low.value_or(INT64_MIN),
                                                plan_.high.value_or(INT64_MAX)));
    } catch (...) { Close(); throw; }
}
bool IndexScan::Next(Tuple& out) {
    if (!Ready()) return false;
    const auto entry = cursor_->Next(); // Owned key/RID; the leaf guard has already been dropped.
    if (!entry) return End();
    auto row = context_.Db().Fetch(plan_.table, entry->rid);
    if (!row || row->value(plan_.column).as_int64() != entry->key)
        throw DbError("index entry does not match a live heap row");
    out = std::move(*row);
    return true;
}
void IndexScan::Close() noexcept { cursor_.reset(); CloseState(); }

Filter::Filter(QueryContext& context, FilterPlan plan, std::unique_ptr<Executor> child)
    : Executor(context), plan_(plan), child_(std::move(child)) {
    plan_.predicate.Validate(ChildSchema(context, child_));
}
void Filter::Init() {
    Start();
    try { child_->Init(); } catch (...) { Close(); throw; }
}
bool Filter::Next(Tuple& out) {
    if (!Ready()) return false;
    Tuple row({});
    while (child_->Next(row)) {
        if (!plan_.predicate.Matches(row)) continue;
        out = std::move(row);
        return true;
    }
    return End();
}
void Filter::Close() noexcept { child_->Close(); CloseState(); }

Projection::Projection(QueryContext& context, ProjectionPlan plan, std::unique_ptr<Executor> child)
    : Executor(context), plan_(std::move(plan)), child_(std::move(child)),
      schema_(ProjectedSchema(ChildSchema(context, child_), plan_)) {}
void Projection::Init() {
    Start();
    try { child_->Init(); } catch (...) { Close(); throw; }
}
bool Projection::Next(Tuple& out) {
    if (!Ready()) return false;
    Tuple row({});
    if (!child_->Next(row)) return End();
    std::vector<Value> values;
    values.reserve(plan_.columns.size());
    for (size_t column : plan_.columns) values.push_back(row.value(column));
    out = Tuple(std::move(values));
    return true;
}
void Projection::Close() noexcept { child_->Close(); CloseState(); }
Ordering Projection::OutputOrdering() const {
    return ProjectedOrdering(child_->OutputOrdering(), plan_);
}

}  // namespace valencedb
