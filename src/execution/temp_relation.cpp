#include "valencedb/execution/temp_relation.hpp"
#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

void TempStore::CheckActive() const {
    if (!active_) throw DbError("temporary store is closed");
}
PageId TempStore::Allocate() {
    CheckActive();
    // NewPage can allocate before failing to acquire a frame. Count that extent too.
    if (db_.Disk().PageCount(FileKind::Temporary) >= limit_) throw DbError("temporary page cap exceeded");
    try {
        auto guard = db_.Buffer().NewPage(FileKind::Temporary);
        metrics_.allocated_temp_pages = db_.Disk().PageCount(FileKind::Temporary);
        metrics_.current_temp_extent_pages = metrics_.allocated_temp_pages;
        metrics_.peak_temp_extent_pages = metrics_.allocated_temp_pages;
        return guard.Page().page_id;
    } catch (...) {
        metrics_.allocated_temp_pages = db_.Disk().PageCount(FileKind::Temporary);
        metrics_.current_temp_extent_pages = metrics_.allocated_temp_pages;
        metrics_.peak_temp_extent_pages = metrics_.allocated_temp_pages;
        throw;
    }
}
void TempStore::Reset() {
    if (open_handles_ != 0) throw DbError("close temporary readers and writers before query cleanup");
    db_.Buffer().DiscardTemporaryPages();
    db_.Disk().ResetTemporary();
    metrics_.current_temp_extent_pages = 0;
    active_ = false;
}
TempWriter::TempWriter(TempStore& store, const Schema& schema)
    : store_(&store), schema_(schema) {
    store.CheckActive();
    ++store.open_handles_;
    ++store.metrics_.temp_relations_created; // Counts construction attempts, as before.
}
void TempWriter::Publish() {
    auto guard = store_->db_.Buffer().Fetch({FileKind::Temporary, last_});
    guard.MutableData() = staging_;
    guard.MarkDirty();
}
void TempWriter::Append(const Tuple& tuple) {
    if (!store_) throw DbError("temporary writer is closed");
    try {
        store_->CheckActive();
        const auto bytes = EncodeTuple(schema_, tuple);
        const size_t rows = CheckedAdd(rows_, 1);
        const size_t modeled = CheckedAdd(modeled_bytes_, RowMemoryCharge(schema_));
        if (last_ == kInvalidPageId || !SlottedPage::CanFit(staging_, bytes.size())) {
            const PageId next = store_->Allocate();
            if (last_ != kInvalidPageId) {
                SlottedPage::SetNext(staging_, next);
                Publish();
            } else {
                first_ = next;
            }
            SlottedPage::Initialize(staging_, next, kInvalidPageId);
            last_ = next;
            ++pages_;
        }
        if (!SlottedPage::Insert(staging_, bytes)) throw DbError("temporary tuple does not fit page");
        rows_ = rows;
        modeled_bytes_ = modeled;
    } catch (...) {
        Close(); // A failed append can never be turned into a successful partial run.
        throw;
    }
}
TempRun TempWriter::Finish() {
    if (!store_) throw DbError("temporary writer is closed");
    try {
        store_->CheckActive();
        if (last_ != kInvalidPageId) {
            SlottedPage::SetNext(staging_, kInvalidPageId);
            Publish();
        }
        TempRun run(*store_, schema_, first_, last_, rows_, pages_, modeled_bytes_);
        Close();
        return run;
    } catch (...) {
        Close();
        throw;
    }
}
void TempWriter::Close() noexcept {
    if (!store_) return;
    --store_->open_handles_;
    store_ = nullptr;
}
TempReader::TempReader(const TempRun& run) : run_(run) {
    Rewind(); // Validate before registering: a failed constructor owns no handle.
    ++run_.store_->open_handles_;
}
void TempReader::Rewind() {
    if (!open_) throw DbError("temporary reader is closed");
    run_.store_->CheckActive();
    page_ = run_.first_;
    slot_ = 0;
    pages_read_ = rows_read_ = 0;
    loaded_ = false;
}
bool TempReader::Next(Tuple& out) {
    if (!open_) throw DbError("temporary reader is closed");
    const auto& run = run_;
    run.store_->CheckActive();
    while (page_ != kInvalidPageId) {
        if (!loaded_) {
            if (pages_read_ >= run.pages_) throw DbError("temporary page chain too long");
            {
                auto guard = run.store_->db_.Buffer().Fetch({FileKind::Temporary, page_});
                staging_ = guard.Data();
            }
            SlottedPage::Validate(staging_, page_, kInvalidPageId);
            ++pages_read_;
            loaded_ = true;
        }
        while (slot_ < SlottedPage::SlotCount(staging_)) {
            auto bytes = SlottedPage::Fetch(staging_, slot_++);
            if (!bytes) continue;
            if (rows_read_ >= run.rows_) throw DbError("temporary row count exceeded");
            out = DecodeTuple(*run.schema_, *bytes);
            ++rows_read_;
            return true;
        }
        const PageId next = SlottedPage::Next(staging_);
        if ((next != kInvalidPageId && next <= page_) ||
            (next == kInvalidPageId && page_ != run.last_))
            throw DbError("invalid temporary page link");
        page_ = next;
        slot_ = 0;
        loaded_ = false;
    }
    if (pages_read_ != run.pages_ || rows_read_ != run.rows_)
        throw DbError("temporary run counts disagree with its pages");
    return false;
}
void TempReader::Close() noexcept {
    if (!open_) return;
    --run_.store_->open_handles_;
    open_ = false;
}

}  // namespace valencedb
