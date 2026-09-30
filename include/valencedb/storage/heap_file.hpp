#pragma once

#include "valencedb/storage/catalog.hpp"
#include "valencedb/types/tuple.hpp"

#include <optional>

namespace valencedb {

class HeapScan {
public:
    HeapScan(BufferPool& pool, TableInfo table, const uint64_t& epoch, ScanRing* ring = nullptr);
    std::optional<std::pair<RID, Tuple>> Next();
    void Reset();

private:
    BufferPool& pool_;
    TableInfo table_;
    const uint64_t& epoch_;
    uint64_t captured_epoch_;
    PageId page_;
    uint16_t slot_ = 0;
    uint32_t completed_pages_ = 0;
    ScanRing* ring_;
};

class HeapFile {
public:
    HeapFile(BufferPool& pool, TableInfo& table) : pool_(pool), table_(table) {}
    RID Insert(const Tuple& tuple);
    std::optional<Tuple> Fetch(RID rid) const;
    bool Erase(RID rid);

private:
    BufferPool& pool_;
    TableInfo& table_;
};

}  // namespace valencedb
