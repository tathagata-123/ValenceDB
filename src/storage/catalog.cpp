#include "valencedb/storage/catalog.hpp"
#include "valencedb/common/error.hpp"

#include <cstring>

namespace valencedb {
namespace {
constexpr size_t kEntry = 36;
constexpr size_t kCatalogHeader = 32;
constexpr size_t kColumn = 32;
constexpr size_t kSchemaStart = 64;
constexpr uint16_t kNoIndexColumn = UINT16_MAX;

std::string ReadName(const PageBytes& page, size_t offset, size_t width) {
    size_t length = 0;
    while (length < width && page[offset + length] != std::byte{0}) ++length;
    if (length == 0 || length == width) throw DbError("invalid catalog name");
    return std::string(reinterpret_cast<const char*>(page.data() + offset), length);
}

void CheckName(const std::string& name, size_t maximum) {
    if (name.empty() || name.size() > maximum || name.find('\0') != std::string::npos)
        throw DbError("invalid catalog name length or embedded NUL");
}
}

void Catalog::Initialize(PageBytes& page, PageId id) {
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::Catalog, id);
    WriteU16(page, 16, 0);
}

Catalog::Catalog(BufferPool& pool) : pool_(pool) {
    auto directory = pool_.Fetch({FileKind::Database, 1});
    ValidatePageHeader(directory.Data(), PageType::Catalog, 1);
    const uint16_t count = ReadU16(directory.Data(), 16);
    if (count > 16) throw DbError("catalog table count exceeds 16");
    for (uint16_t i = 0; i < count; ++i) {
        const size_t at = kCatalogHeader + size_t(i) * kEntry;
        const std::string name = ReadName(directory.Data(), at, 32);
        const PageId id = ReadU32(directory.Data(), at + 32);
        if (id == kInvalidPageId || id < 2) throw DbError("invalid table metadata page");
        for (const auto& table : tables_)
            if (table.name == name || table.meta_page == id)
                throw DbError("duplicate catalog entry");
        directory.Drop();
        auto meta = pool_.Fetch({FileKind::Database, id});
        tables_.push_back(DecodeMeta(meta.Data(), id, name));
        meta.Drop();
        directory = pool_.Fetch({FileKind::Database, 1});
        ValidatePageHeader(directory.Data(), PageType::Catalog, 1);
    }
}

TableInfo Catalog::DecodeMeta(const PageBytes& page, PageId id, const std::string& name) {
    ValidatePageHeader(page, PageType::TableMeta, id);
    const uint16_t count = ReadU16(page, 38);
    if (count == 0 || count > 8) throw DbError("invalid table column count");
    std::vector<Column> columns;
    for (uint16_t i = 0; i < count; ++i) {
        const size_t at = kSchemaStart + size_t(i) * kColumn;
        const std::string colname = ReadName(page, at, 24);
        const uint8_t type = std::to_integer<uint8_t>(page[at + 24]);
        const uint8_t flags = std::to_integer<uint8_t>(page[at + 25]);
        const uint16_t limit = ReadU16(page, at + 26);
        if (type > 1 || flags != 0)
            throw DbError("invalid column descriptor");
        columns.push_back({colname, type == 0 ? TypeId::Int64 : TypeId::Varchar, limit, false});
    }
    TableInfo info{name, id, Schema(SchemaKind::Base, std::move(columns))};
    info.first_page = ReadU32(page, 16);
    info.last_page = ReadU32(page, 20);
    info.live_rows = ReadU64(page, 24);
    info.heap_pages = ReadU32(page, 32);
    info.index_col = ReadU16(page, 36);
    info.index_meta = ReadU32(page, 40);
    if ((info.index_col == kNoIndexColumn) != (info.index_meta == kInvalidPageId) ||
        (info.index_meta != kInvalidPageId &&
         (info.index_meta < 2 || info.index_meta == id || info.index_col >= count ||
          info.schema.column(info.index_col).type != TypeId::Int64)))
        throw DbError("invalid table index metadata");
    if ((info.heap_pages == 0 && info.live_rows != 0) ||
        (info.heap_pages == 0) != (info.first_page == kInvalidPageId &&
                                   info.last_page == kInvalidPageId) ||
        (info.heap_pages != 0 && (info.first_page == kInvalidPageId ||
                                  info.last_page == kInvalidPageId)))
        throw DbError("inconsistent table heap metadata");
    return info;
}

void Catalog::EncodeMeta(PageBytes& page, const TableInfo& info) {
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::TableMeta, info.meta_page);
    WriteU32(page, 16, info.first_page);
    WriteU32(page, 20, info.last_page);
    WriteU64(page, 24, info.live_rows);
    WriteU32(page, 32, info.heap_pages);
    WriteU16(page, 36, info.index_col);
    WriteU16(page, 38, static_cast<uint16_t>(info.schema.size()));
    WriteU32(page, 40, info.index_meta);
    for (size_t i = 0; i < info.schema.size(); ++i) {
        const auto& col = info.schema.column(i);
        const size_t at = kSchemaStart + i * kColumn;
        std::memcpy(page.data() + at, col.name.data(), col.name.size());
        page[at + 24] = std::byte(col.type == TypeId::Int64 ? 0 : 1);
        WriteU16(page, at + 26, col.varchar_limit);
    }
}

TableInfo Catalog::CreateTable(const std::string& name, const Schema& schema) {
    CheckName(name, 31);
    if (schema.kind() != SchemaKind::Base) throw DbError("table requires base schema");
    for (size_t i = 0; i < schema.size(); ++i) CheckName(schema.column(i).name, 23);
    if (tables_.size() >= 16) throw DbError("catalog is full");
    for (const auto& table : tables_) if (table.name == name) throw DbError("duplicate table name");
    auto meta = pool_.NewPage(FileKind::Database);
    TableInfo info{name, meta.Page().page_id, schema};
    EncodeMeta(meta.MutableData(), info);
    meta.MarkDirty();
    meta.Drop();
    auto directory = pool_.Fetch({FileKind::Database, 1});
    ValidatePageHeader(directory.Data(), PageType::Catalog, 1);
    const size_t at = kCatalogHeader + tables_.size() * kEntry;
    std::memcpy(directory.MutableData().data() + at, name.data(), name.size());
    WriteU32(directory.MutableData(), at + 32, info.meta_page);
    WriteU16(directory.MutableData(), 16, static_cast<uint16_t>(tables_.size() + 1));
    directory.MarkDirty();
    tables_.push_back(info);
    return info;
}

TableInfo Catalog::Find(const std::string& name) const {
    for (const auto& table : tables_) if (table.name == name) return table;
    throw DbError("table not found");
}

std::vector<std::string> Catalog::TableNames() const {
    std::vector<std::string> names;
    for (const auto& table : tables_) names.push_back(table.name);
    return names;
}

void Catalog::Save(const TableInfo& info) {
    for (auto& table : tables_) {
        if (table.name != info.name || table.meta_page != info.meta_page) continue;
        auto guard = pool_.Fetch({FileKind::Database, info.meta_page});
        ValidatePageHeader(guard.Data(), PageType::TableMeta, info.meta_page);
        EncodeMeta(guard.MutableData(), info);
        guard.MarkDirty();
        table = info;
        return;
    }
    throw DbError("table metadata is not in catalog");
}

}  // namespace valencedb
