#pragma once

#include "valencedb/execution/executor.hpp"
#include <memory>

namespace valencedb {

// Copies a sealed run. Its store and immutable schema must outlive this executor.
class TempScan final : public Executor {
public:
    TempScan(QueryContext& context, const TempRun& run);
    ~TempScan() noexcept override { Close(); }
    void Init() override;
    bool Next(Tuple& out) override;
    void Close() noexcept override;
    const Schema& OutputSchema() const override { return run_.OutputSchema(); }
    Ordering OutputOrdering() const override { return {}; }
private:
    TempRun run_;
    std::unique_ptr<TempReader> reader_;
};

}  // namespace valencedb
