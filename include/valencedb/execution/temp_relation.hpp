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

// A sealed, non-owning value. The store and immutable schema must outlive all
// copies, readers, and scans. Only Finish can construct a run; copying it owns no
// pages and does not keep the query open. Query cleanup invalidates every copy.
class TempRun {
public:
    PageId FirstPage() const noexcept { return first_; }
    PageId LastPage() const noexcept { return last_; }
    size_t RowCount() const noexcept { return rows_; }
    size_t PageCount() const noexcept { return pages_; }
    size_t ModeledRowBytes() const noexcept { return modeled_bytes_; }
    const Schema& OutputSchema() const noexcept { return *schema_; }
private:
    friend class TempScan;
    friend class TempWriter;
    friend class TempReader;
    TempRun(TempStore& store, const Schema& schema, PageId first, PageId last,
            size_t rows, size_t pages, size_t modeled_bytes)
        : store_(&store), schema_(&schema), first_(first), last_(last),
          rows_(rows), pages_(pages), modeled_bytes_(modeled_bytes) {}
    TempStore* store_;
    const Schema* schema_;
    PageId first_, last_;
    size_t rows_, pages_, modeled_bytes_;
};

class TempWriter {
public:
    TempWriter(TempStore& store, const Schema& schema);
    TempWriter(TempStore&, const Schema&&) = delete;
    ~TempWriter() noexcept { Close(); }
    TempWriter(const TempWriter&) = delete;
    TempWriter& operator=(const TempWriter&) = delete;
    void Append(const Tuple& tuple);
    TempRun Finish();
    void Close() noexcept;
private:
    void Publish();
    TempStore* store_; // Null after completion, failure, or abandonment.
    const Schema& schema_;
    PageId first_ = kInvalidPageId, last_ = kInvalidPageId;
    size_t rows_ = 0, pages_ = 0, modeled_bytes_ = 0;
    PageBytes staging_{};
};

class TempReader {
public:
    explicit TempReader(const TempRun& run);
    ~TempReader() noexcept { Close(); }
    TempReader(const TempReader&) = delete;
    TempReader& operator=(const TempReader&) = delete;
    bool Next(Tuple& out);
    void Rewind();
    void Close() noexcept;
private:
    TempRun run_;
    bool open_ = true;
    PageBytes staging_{};
    PageId page_ = kInvalidPageId;
    uint16_t slot_ = 0;
    size_t pages_read_ = 0, rows_read_ = 0;
    bool loaded_ = false;
};

// Conservative descriptor and handle allowances shared by the capacity models.
inline constexpr size_t kTempRunCharge = 64;
inline constexpr size_t kTempHandleOverhead = 128;
static_assert(sizeof(TempRun) <= kTempRunCharge);
static_assert(sizeof(TempWriter) + sizeof(void*) + 16 <= kPageSize + kTempHandleOverhead);
// Reader allocation, pointer, and a sort heap entry; owned tuple data is charged
// separately by RowMemoryCharge. The 16 bytes allow for allocation bookkeeping.
static_assert(sizeof(TempReader) + sizeof(void*) + 2 * sizeof(size_t) + 16 <=
              kPageSize + kTempHandleOverhead);

}  // namespace valencedb
