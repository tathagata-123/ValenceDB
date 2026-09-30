#pragma once

#include "valencedb/storage/database.hpp"
#include "valencedb/execution/work_budget.hpp"

namespace valencedb {

struct TempMetrics {
    size_t allocated_temp_pages = 0;
    size_t current_temp_extent_pages = 0;
    size_t peak_temp_extent_pages = 0;
    size_t temp_relations_created = 0;
};
class TempStore {
public:
    TempStore(const TempStore&) = delete;
    TempStore& operator=(const TempStore&) = delete;
    const TempMetrics& Metrics() const noexcept { return metrics_; }
private:
    friend class QueryContext;
    friend class TempRelation;
    friend class TempWriter;
    friend class TempReader;
    TempStore(Database& db, size_t limit) : db_(db), limit_(limit) {}
    void CheckActive() const;
    PageId Allocate();
    void Reset();
    Database& db_;
    size_t limit_;
    size_t open_handles_ = 0;
    TempMetrics metrics_;
    bool active_ = true;
};

// Store and immutable schema must outlive this nonmoving descriptor and all its handles.
// Close/destroy handles before Release or descriptor destruction. No schema is copied.
class TempRelation {
public:
    TempRelation(TempStore& store, const Schema& schema);
    TempRelation(TempStore&, const Schema&&) = delete;
    TempRelation(const TempRelation&) = delete;
    TempRelation& operator=(const TempRelation&) = delete;
    void Release();
    PageId FirstPage() const noexcept { return first_; }
    PageId LastPage() const noexcept { return last_; }
    size_t RowCount() const noexcept { return rows_; }
    size_t PageCount() const noexcept { return pages_; }
    size_t ModeledRowBytes() const noexcept { return modeled_bytes_; }
    const Schema& OutputSchema() const noexcept { return schema_; }
    bool Sealed() const noexcept { return sealed_; }
private:
    friend class TempScan;
    friend class TempWriter;
    friend class TempReader;
    void CheckLive() const;
    TempStore& store_;
    const Schema& schema_;
    PageId first_ = kInvalidPageId, last_ = kInvalidPageId;
    size_t rows_ = 0, pages_ = 0, modeled_bytes_ = 0;
    size_t open_handles_ = 0;
    bool sealed_ = false, released_ = false, writer_started_ = false;
};

class TempWriter {
public:
    explicit TempWriter(TempRelation& relation);
    ~TempWriter() noexcept { Close(); }
    TempWriter(const TempWriter&) = delete;
    TempWriter& operator=(const TempWriter&) = delete;
    void Append(const Tuple& tuple);
    void Seal();
    void Close() noexcept;
private:
    void Publish();
    TempRelation* relation_;
    PageBytes staging_{};
};

class TempReader {
public:
    explicit TempReader(TempRelation& relation);
    ~TempReader() noexcept { Close(); }
    TempReader(const TempReader&) = delete;
    TempReader& operator=(const TempReader&) = delete;
    bool Next(Tuple& out);
    void Rewind();
    void Close() noexcept;
private:
    TempRelation* relation_;
    PageBytes staging_{};
    PageId page_ = kInvalidPageId;
    uint16_t slot_ = 0;
    size_t pages_read_ = 0, rows_read_ = 0;
    bool loaded_ = false;
};

}  // namespace valencedb
