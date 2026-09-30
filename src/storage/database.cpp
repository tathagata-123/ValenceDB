#include "valencedb/storage/database.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

Database::Database(std::filesystem::path database_path, OpenMode mode,
                   std::filesystem::path temporary_path, size_t buffer_frames)
    : disk_(std::move(database_path), mode, temporary_path,
            std::filesystem::exists(temporary_path) ? OpenMode::OpenExisting : OpenMode::CreateNew),
      pool_(disk_, buffer_frames) {
    if (disk_.PageCount(FileKind::Temporary) != 0) disk_.ResetTemporary();
    if (mode == OpenMode::CreateNew) {
        auto super = pool_.NewPage(FileKind::Database);
        if (super.Page().page_id != 0) throw DbError("new superblock is not page 0");
        WritePageHeader(super.MutableData(), PageType::Superblock, 0);
        WriteU32(super.MutableData(), 16, kPageSize);
        WriteU32(super.MutableData(), 20, 1);
        super.MarkDirty();
        super.Drop();
        auto directory = pool_.NewPage(FileKind::Database);
        if (directory.Page().page_id != 1) throw DbError("new catalog is not page 1");
        Catalog::Initialize(directory.MutableData(), 1);
        directory.MarkDirty();
    } else {
        if (disk_.PageCount(FileKind::Database) < 2)
            throw DbError("database has no superblock/catalog");
        auto super = pool_.Fetch({FileKind::Database, 0});
        ValidatePageHeader(super.Data(), PageType::Superblock, 0);
        if (ReadU32(super.Data(), 16) != kPageSize || ReadU32(super.Data(), 20) != 1)
            throw DbError("invalid superblock configuration");
    }
    catalog_ = std::make_unique<Catalog>(pool_);
    for (const auto& name : catalog_->TableNames()) {
        const auto table = catalog_->Find(name);
        if (table.index_meta == kInvalidPageId) continue;
        auto index = std::unique_ptr<BPlusTree>(new BPlusTree(BPlusTree::Open(pool_, table.index_meta)));
        if (index->KeyColumn() != table.index_col) throw DbError("index column metadata mismatch");
        if (!indexes_.emplace(table.index_meta, std::move(index)).second)
            throw DbError("index metadata shared by tables");
    }
}

void Database::CheckOpen() const { if (closed_) throw DbError("database is closed"); }

void Database::CheckMutable() const {
    CheckOpen();
    if (query_active_) throw DbError("database mutation is forbidden during an active query");
}

void Database::BeginQuery() {
    CheckMutable();
    if (pool_.FrameCount() < 8) throw DbError("query execution requires at least 8 buffer frames");
    query_active_ = true;
}

TableInfo Database::CreateTable(const std::string& name, const Schema& schema) {
    CheckMutable();
    auto table = catalog_->CreateTable(name, schema);
    ++epoch_;
    return table;
}

TableInfo Database::Table(const std::string& name) const {
    CheckOpen();
    return catalog_->Find(name);
}

std::vector<std::string> Database::TableNames() const {
    CheckOpen();
    return catalog_->TableNames();
}

RID Database::Insert(const std::string& name, const Tuple& tuple) {
    CheckMutable();
    auto table = catalog_->Find(name);
    // Validate the entire tuple before either heap or index mutation.
    (void)EncodeTuple(table.schema, tuple);
    const int64_t key = table.index_meta == kInvalidPageId ? 0 : tuple.value(table.index_col).as_int64();
    HeapFile heap(pool_, table);
    const RID rid = heap.Insert(tuple);
    ++epoch_;
    if (table.index_meta != kInvalidPageId && !TableIndex(table).Insert(key, rid))
        throw DbError("new heap RID already exists in index");
    catalog_->Save(table);
    StaleStatistics(name);
    return rid;
}

std::optional<Tuple> Database::Fetch(const std::string& name, RID rid) {
    CheckOpen();
    auto table = catalog_->Find(name);
    return HeapFile(pool_, table).Fetch(rid);
}

bool Database::Delete(const std::string& name, RID rid) {
    CheckMutable();
    auto table = catalog_->Find(name);
    HeapFile heap(pool_, table);
    // Fetch validates owner_meta_page before deriving a key or changing the index.
    const auto old = heap.Fetch(rid);
    if (!old) return false;
    if (table.live_rows == 0) throw DbError("table row count underflow");
    if (table.index_meta != kInvalidPageId &&
        !TableIndex(table).Erase(old->value(table.index_col).as_int64(), rid))
        throw DbError("live heap row missing from index");
    if (!heap.Erase(rid)) throw DbError("live heap row disappeared during delete");
    ++epoch_;
    catalog_->Save(table);
    StaleStatistics(name);
    return true;
}

HeapScan Database::Scan(const std::string& name, ScanRing* ring) {
    CheckOpen();
    return HeapScan(pool_, catalog_->Find(name), epoch_, ring);
}

BPlusTree& Database::TableIndex(const TableInfo& table) const {
    const auto found = indexes_.find(table.index_meta);
    if (found == indexes_.end()) throw DbError("table has no open index");
    return *found->second;
}

const BPlusTree& Database::Index(const std::string& name) const {
    CheckOpen();
    return TableIndex(catalog_->Find(name));
}

BPlusIterator Database::IndexEqualRange(const std::string& name, int64_t key) const {
    return Index(name).EqualRange(key);
}

BPlusIterator Database::IndexRange(const std::string& name, int64_t low, int64_t high) const {
    return Index(name).Range(low, high);
}

void Database::CreateIndex(const std::string& name, size_t column) {
    CheckMutable();
    auto table = catalog_->Find(name);
    if (table.index_meta != kInvalidPageId) throw DbError("table already has an index");
    if (column >= table.schema.size() || table.schema.column(column).type != TypeId::Int64)
        throw DbError("index requires a valid INT64 column");
    auto index = std::unique_ptr<BPlusTree>(new BPlusTree(
        BPlusTree::Create(pool_, static_cast<uint16_t>(column))));
    auto scan = Scan(name);
    uint64_t built = 0;
    while (auto row = scan.Next()) {
        if (!index->Insert(row->second.value(column).as_int64(), row->first))
            throw DbError("duplicate heap RID during index build");
        ++built;
    }
    if (built != table.live_rows) throw DbError("index build row count mismatch");
    // Publish only after the complete live heap scan and all tree inserts succeeded.
    table.index_col = static_cast<uint16_t>(column);
    table.index_meta = index->MetaPage();
    indexes_.emplace(table.index_meta, std::move(index));
    catalog_->Save(table);
    StaleStatistics(name);
    ++epoch_;
}

void Database::Flush() {
    CheckOpen();
    pool_.FlushAll();
}

void Database::Close() {
    if (closed_) return;
    if (query_active_) throw DbError("cannot close database during an active query");
    if (pool_.metrics().active_pins != 0) throw DbError("cannot close with pinned pages");
    pool_.FlushAll();
    disk_.Close();
    closed_ = true;
    ++epoch_;
}

}  // namespace valencedb
