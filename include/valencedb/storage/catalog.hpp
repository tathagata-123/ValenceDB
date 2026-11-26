#pragma once

#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/types/schema.hpp"

#include <string>
#include <vector>

namespace valencedb {

struct TableInfo {
    std::string name;
    PageId meta_page;
    Schema schema;

    // Save updates only these heap and index fields.
    PageId first_page = kInvalidPageId;
    PageId last_page = kInvalidPageId;
    uint64_t live_rows = 0;
    uint32_t heap_pages = 0;
    uint16_t index_col = UINT16_MAX;
    PageId index_meta = kInvalidPageId;
};

class Catalog {
public:
    explicit Catalog(BufferPool& pool);
    static void Initialize(PageBytes& page, PageId id);
    TableInfo CreateTable(const std::string& name, const Schema& schema);
    TableInfo Find(const std::string& name) const;
    std::vector<std::string> TableNames() const;
    void Save(const TableInfo& info);

private:
    BufferPool& pool_;
    std::vector<TableInfo> tables_;
    static TableInfo DecodeMeta(const PageBytes& page, PageId id, const std::string& name);
    static void EncodeMeta(PageBytes& page, const TableInfo& info);
};

}  // namespace valencedb
