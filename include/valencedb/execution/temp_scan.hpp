#pragma once

#include "valencedb/execution/executor.hpp"
#include <memory>

namespace valencedb {

// Borrows a sealed relation, whose store/schema/descriptor must outlive this executor.
class TempScan final : public Executor {
public:
    TempScan(QueryContext& context, TempRelation& relation);
    ~TempScan() noexcept override { Close(); }
    void Init() override;
    bool Next(Tuple& out) override;
    void Close() noexcept override;
    const Schema& OutputSchema() const override { return relation_.OutputSchema(); }
    Ordering OutputOrdering() const override { return {}; }
private:
    TempRelation& relation_;
    std::unique_ptr<TempReader> reader_;
};

}  // namespace valencedb
