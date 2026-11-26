#include "valencedb/execution/temp_scan.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

TempScan::TempScan(QueryContext& context, const TempRun& run)
    : Executor(context), run_(run) {
    if (run.store_ != &context.Temps()) throw DbError("temp scan requires this query's run");
}
void TempScan::DoInit() {
    reader_ = std::make_unique<TempReader>(run_);
}
bool TempScan::DoNext(Tuple& out) {
    if (!reader_->Next(out)) return false;
    return true;
}
void TempScan::DoClose() noexcept { reader_.reset(); }

}  // namespace valencedb
