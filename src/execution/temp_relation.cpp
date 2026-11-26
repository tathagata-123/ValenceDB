#include "valencedb/execution/temp_relation.hpp"
#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

void TempStore::CheckActive() const {
    if (!active_) throw DbError("temporary store is closed");
}
PageId TempStore::Allocate() {
    if (db_.Disk().PageCount(FileKind::Temporary) >= limit_) throw DbError("temporary page cap exceeded");
    auto guard = db_.Buffer().NewPage(FileKind::Temporary);
    ++metrics_.allocated_temp_pages;
    return guard.Page().page_id;
}
void TempStore::Reset() {
    if (open_resources_ != 0) throw DbError("close executors and temporary handles before query cleanup");
    db_.Buffer().DiscardTemporaryPages();
    db_.Disk().ResetTemporary();
    active_ = false;
}
TempWriter::TempWriter(TempStore& store, const Schema& schema)
    : store_(&store), schema_(schema) {
    store.CheckActive();
    ++store.open_resources_;
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
        const auto bytes = EncodeTuple(schema_, tuple);
        const size_t rows = CheckedAdd(rows_, 1);
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
    } catch (...) {
        Close(); // A failed append can never be turned into a successful partial run.
        throw;
    }
}
TempRun TempWriter::Finish() {
    if (!store_) throw DbError("temporary writer is closed");
    try {
        if (last_ != kInvalidPageId) {
            SlottedPage::SetNext(staging_, kInvalidPageId);
            Publish();
        }
        TempRun run(*store_, schema_, first_, last_, rows_, pages_);
        Close();
        return run;
    } catch (...) {
        Close();
        throw;
    }
}
void TempWriter::Close() noexcept {
    if (!store_) return;
    --store_->open_resources_;
    store_ = nullptr;
}
TempReader::TempReader(const TempRun& run) : run_(run) {
    run_.store_->CheckActive();
    Rewind();
    ++run_.store_->open_resources_;
}
void TempReader::Rewind() {
    if (!open_) throw DbError("temporary reader is closed");
    page_ = run_.first_;
    slot_ = 0;
    loaded_ = false;
}
bool TempReader::Next(Tuple& out) {
    if (!open_) throw DbError("temporary reader is closed");
    const auto& run = run_;
    while (page_ != kInvalidPageId) {
        if (!loaded_) {
            {
                auto guard = run.store_->db_.Buffer().Fetch({FileKind::Temporary, page_});
                staging_ = guard.Data();
            }
            SlottedPage::Validate(staging_, page_, kInvalidPageId);
            loaded_ = true;
        }
        while (slot_ < SlottedPage::SlotCount(staging_)) {
            auto bytes = SlottedPage::Fetch(staging_, slot_++);
            if (!bytes) continue;
            out = DecodeTuple(*run.schema_, *bytes);
            return true;
        }
        const PageId next = SlottedPage::Next(staging_);
        // Append-only page IDs strictly increase, preventing cycles without a visited set.
        if (next != kInvalidPageId && next <= page_)
            throw DbError("invalid temporary page link");
        page_ = next;
        slot_ = 0;
        loaded_ = false;
    }
    return false;
}
void TempReader::Close() noexcept {
    if (!open_) return;
    --run_.store_->open_resources_;
    open_ = false;
}

}  // namespace valencedb
