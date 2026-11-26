#pragma once

#include "valencedb/execution/executor.hpp"

namespace valencedb {

class SeqScan final : public Executor {
public:
    SeqScan(QueryContext& context, SeqScanPlan plan);
    ~SeqScan() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {}; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    SeqScanPlan plan_;
    Schema schema_;
    std::optional<ScanRing> ring_;
    std::optional<HeapScan> scan_;
};

class IndexScan final : public Executor {
public:
    IndexScan(QueryContext& context, IndexScanPlan plan);
    ~IndexScan() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override { return {true, plan_.column}; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    IndexScanPlan plan_;
    Schema schema_;
    std::optional<BPlusIterator> cursor_;
};

class Filter final : public Executor {
public:
    Filter(QueryContext& context, FilterPlan plan, std::unique_ptr<Executor> child);
    ~Filter() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return child_->OutputSchema(); }
    Ordering OutputOrdering() const override { return child_->OutputOrdering(); }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    FilterPlan plan_;
    std::unique_ptr<Executor> child_;
};

class Projection final : public Executor {
public:
    Projection(QueryContext& context, ProjectionPlan plan, std::unique_ptr<Executor> child);
    ~Projection() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return schema_; }
    Ordering OutputOrdering() const override;
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    ProjectionPlan plan_;
    std::unique_ptr<Executor> child_;
    Schema schema_;
};

}  // namespace valencedb
