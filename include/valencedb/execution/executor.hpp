#pragma once

#include "valencedb/execution/plan.hpp"
#include "valencedb/execution/temp_relation.hpp"

namespace valencedb {

class Executor;
// Database outlives context; context outlives every executor and temporary handle.
class QueryContext {
public:
    explicit QueryContext(Database& db, QueryResources resources = {});
    ~QueryContext() noexcept;
    QueryContext(const QueryContext&) = delete;
    QueryContext& operator=(const QueryContext&) = delete;
    void FinishQuery(Executor& root); // Requires normal EOF; closes root then checked cleanup.
    void Close();                   // Checked cancellation/cleanup, never records success.
    void CheckActive() const;
    bool Succeeded() const noexcept { return succeeded_; }
    Database& Db() noexcept { return db_; }
    TempStore& Temps() noexcept { return temps_; }
    const QueryResources& Resources() const noexcept { return resources_; }
private:
    friend class Executor;
    Database& db_;
    QueryResources resources_;
    TempStore temps_;
    size_t open_executors_ = 0;
    bool active_ = true, succeeded_ = false;
};

class Executor {
public:
    explicit Executor(QueryContext& context) : context_(context) { context.CheckActive(); }
    virtual ~Executor() noexcept = default;
    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;
    virtual void Init() = 0;
    virtual bool Next(Tuple& out) = 0;
    virtual void Close() noexcept = 0;
    virtual const Schema& OutputSchema() const = 0;
    virtual Ordering OutputOrdering() const = 0;
    bool Exhausted() const noexcept { return eof_; }
    QueryContext& Context() const noexcept { return context_; }
protected:
    void Start();
    bool Ready() const;
    bool End() noexcept { eof_ = true; return false; }
    void CloseState() noexcept;
    QueryContext& context_;
private:
    bool initialized_ = false, eof_ = false;
};

}  // namespace valencedb
