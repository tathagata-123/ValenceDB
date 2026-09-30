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
TempRelation::TempRelation(TempStore& store, const Schema& schema)
    : store_(store), schema_(schema) {
    store_.CheckActive();
    ++store_.metrics_.temp_relations_created;
}
void TempRelation::CheckLive() const {
    store_.CheckActive();
    if (released_) throw DbError("temporary relation is released");
}
void TempRelation::Release() {
    if (open_handles_ != 0) throw DbError("close relation handles before release");
    released_ = true;
    first_ = last_ = kInvalidPageId;
    rows_ = pages_ = modeled_bytes_ = 0;
}
TempWriter::TempWriter(TempRelation& relation) : relation_(&relation) {
    relation.CheckLive();
    if (relation.writer_started_) throw DbError("temporary relation permits one writer");
    relation.writer_started_ = true;
    ++relation.open_handles_;
    ++relation.store_.open_handles_;
}
void TempWriter::Publish() {
    auto& relation = *relation_;
    auto guard = relation.store_.db_.Buffer().Fetch({FileKind::Temporary, relation.last_});
    guard.MutableData() = staging_;
    guard.MarkDirty();
}
void TempWriter::Append(const Tuple& tuple) {
    if (!relation_) throw DbError("temporary writer is closed");
    auto& relation = *relation_;
    relation.CheckLive();
    const auto bytes = EncodeTuple(relation.schema_, tuple);
    const size_t rows = CheckedAdd(relation.rows_, 1);
    const size_t modeled = CheckedAdd(relation.modeled_bytes_, RowMemoryCharge(relation.schema_));
    if (relation.last_ == kInvalidPageId || !SlottedPage::CanFit(staging_, bytes.size())) {
        const PageId next = relation.store_.Allocate();
        if (relation.last_ != kInvalidPageId) {
            SlottedPage::SetNext(staging_, next);
            Publish();
        } else {
            relation.first_ = next;
        }
        SlottedPage::Initialize(staging_, next, kInvalidPageId);
        relation.last_ = next;
        ++relation.pages_;
    }
    if (!SlottedPage::Insert(staging_, bytes)) throw DbError("temporary tuple does not fit page");
    relation.rows_ = rows;
    relation.modeled_bytes_ = modeled;
}
void TempWriter::Seal() {
    if (!relation_) throw DbError("temporary writer is closed");
    relation_->CheckLive();
    if (relation_->last_ != kInvalidPageId) {
        SlottedPage::SetNext(staging_, kInvalidPageId);
        Publish();
    }
    relation_->sealed_ = true;
    Close();
}
void TempWriter::Close() noexcept {
    if (!relation_) return;
    --relation_->open_handles_;
    --relation_->store_.open_handles_;
    relation_ = nullptr;
}
TempReader::TempReader(TempRelation& relation) : relation_(&relation) {
    relation.CheckLive();
    if (!relation.sealed_) throw DbError("temporary reader requires a sealed relation");
    ++relation.open_handles_;
    ++relation.store_.open_handles_;
    Rewind();
}
void TempReader::Rewind() {
    if (!relation_) throw DbError("temporary reader is closed");
    relation_->CheckLive();
    page_ = relation_->first_;
    slot_ = 0;
    pages_read_ = rows_read_ = 0;
    loaded_ = false;
}
bool TempReader::Next(Tuple& out) {
    if (!relation_) throw DbError("temporary reader is closed");
    auto& relation = *relation_;
    relation.CheckLive();
    while (page_ != kInvalidPageId) {
        if (!loaded_) {
            if (pages_read_ >= relation.pages_) throw DbError("temporary page chain too long");
            {
                auto guard = relation.store_.db_.Buffer().Fetch({FileKind::Temporary, page_});
                staging_ = guard.Data();
            }
            SlottedPage::Validate(staging_, page_, kInvalidPageId);
            ++pages_read_;
            loaded_ = true;
        }
        while (slot_ < SlottedPage::SlotCount(staging_)) {
            auto bytes = SlottedPage::Fetch(staging_, slot_++);
            if (!bytes) continue;
            if (rows_read_ >= relation.rows_) throw DbError("temporary row count exceeded");
            out = DecodeTuple(relation.schema_, *bytes);
            ++rows_read_;
            return true;
        }
        const PageId next = SlottedPage::Next(staging_);
        if ((next != kInvalidPageId && next <= page_) ||
            (next == kInvalidPageId && page_ != relation.last_))
            throw DbError("invalid temporary page link");
        page_ = next;
        slot_ = 0;
        loaded_ = false;
    }
    if (pages_read_ != relation.pages_ || rows_read_ != relation.rows_)
        throw DbError("temporary relation counts disagree with its pages");
    return false;
}
void TempReader::Close() noexcept {
    if (!relation_) return;
    --relation_->open_handles_;
    --relation_->store_.open_handles_;
    relation_ = nullptr;
}

}  // namespace valencedb
