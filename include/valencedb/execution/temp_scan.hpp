#pragma once

#include "valencedb/execution/executor.hpp"
#include <memory>

namespace valencedb {

// Copies a sealed run. Its store and immutable schema must outlive this executor.
class TempScan final : public Executor {
public:
    TempScan(QueryContext& context, const TempRun& run);
    ~TempScan() noexcept override { Close(); }
    const Schema& OutputSchema() const override { return run_.OutputSchema(); }
    Ordering OutputOrdering() const override { return {}; }
private:
    void DoInit() override;
    bool DoNext(Tuple& out) override;
    void DoClose() noexcept override;
    TempRun run_;
    std::unique_ptr<TempReader> reader_;
};

}  // namespace valencedb
