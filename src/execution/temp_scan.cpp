#include "valencedb/execution/temp_scan.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

TempScan::TempScan(QueryContext& context, TempRelation& relation)
    : Executor(context), relation_(relation) {
    if (&relation.store_ != &context.Temps()) throw DbError("temp scan requires this query's relation");
    relation.CheckLive();
    if (!relation.Sealed()) throw DbError("temp scan requires a sealed relation");
}
void TempScan::Init() {
    Start();
    try { reader_ = std::make_unique<TempReader>(relation_); }
    catch (const std::bad_alloc&) {
        Close();
        throw DbError("temp scan memory allocation failed");
    } catch (...) { Close(); throw; }
}
bool TempScan::Next(Tuple& out) {
    if (!Ready()) return false;
    if (!reader_->Next(out)) return End();
    return true;
}
void TempScan::Close() noexcept { reader_.reset(); CloseState(); }

}  // namespace valencedb
