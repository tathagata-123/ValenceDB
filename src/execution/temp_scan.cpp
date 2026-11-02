#include "valencedb/execution/temp_scan.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

TempScan::TempScan(QueryContext& context, const TempRun& run)
    : Executor(context), run_(run) {
    if (run.store_ != &context.Temps()) throw DbError("temp scan requires this query's run");
}
void TempScan::Init() {
    Start();
    try { reader_ = std::make_unique<TempReader>(run_); }
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
