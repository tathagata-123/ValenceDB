#include "valencedb/execution/executor.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

QueryContext::QueryContext(Database& db, QueryResources resources)
    : db_(db), resources_(resources), temps_(db, resources.max_temp_pages) {
    resources_.Validate();
    db_.BeginQuery();
    // A reset failure deliberately leaves Database active: abandon it if cleanup cannot succeed.
    temps_.Reset();
    temps_.active_ = true;
}
QueryContext::~QueryContext() noexcept {
    try { Close(); } catch (...) { /* Explicit checked cleanup is the error-reporting path. */ }
}
void QueryContext::CheckActive() const {
    temps_.CheckActive();
}
void QueryContext::Close() {
    if (!temps_.active_) return;
    temps_.Reset();
    db_.query_active_ = false;
}
void QueryContext::FinishQuery(Executor& root) {
    CheckActive();
    if (&root.Context() != this || !root.Exhausted()) throw DbError("query completion requires root EOF");
    root.Close();
    Close();
    succeeded_ = true;
}
void Executor::Init() {
    context_.CheckActive();
    if (initialized_) throw DbError("executor Init requires Close before replay");
    initialized_ = true;
    eof_ = false;
    ++context_.temps_.open_resources_;
    try { DoInit(); }
    catch (...) { Close(); throw; }
}
bool Executor::Next(Tuple& out) {
    if (!initialized_) throw DbError("executor Next requires Init");
    if (eof_) return false;
    try {
        eof_ = !DoNext(out);
        return !eof_;
    } catch (...) { Close(); throw; }
}
void Executor::Close() noexcept {
    if (!initialized_) return;
    DoClose(); // Children and temporary handles close before the reset guard drops.
    --context_.temps_.open_resources_;
    initialized_ = false;
}

}  // namespace valencedb
