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
    if (!active_) throw DbError("query context is closed");
}
void QueryContext::Close() {
    if (!active_) return;
    if (open_executors_ != 0) throw DbError("close executors before query cleanup");
    temps_.Reset();
    active_ = false;
    db_.query_active_ = false;
}
void QueryContext::FinishQuery(Executor& root) {
    CheckActive();
    if (&root.Context() != this || !root.Exhausted()) throw DbError("query completion requires root EOF");
    root.Close();
    Close();
    succeeded_ = true;
}
void Executor::Start() {
    context_.CheckActive();
    if (initialized_) throw DbError("executor Init requires Close before replay");
    initialized_ = true;
    eof_ = false;
    ++context_.open_executors_;
}
bool Executor::Ready() const {
    context_.CheckActive();
    if (!initialized_) throw DbError("executor Next requires Init");
    return !eof_;
}
void Executor::CloseState() noexcept {
    if (initialized_) --context_.open_executors_;
    initialized_ = false;
}

}  // namespace valencedb
