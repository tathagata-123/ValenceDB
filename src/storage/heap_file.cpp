#include "valencedb/storage/heap_file.hpp"
#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

#include <limits>

namespace valencedb {
namespace {
PageGuard FetchHeap(BufferPool& pool, PageId id, PageId owner, AccessMode mode,
                    ScanRing* ring = nullptr) {
    if (id == kInvalidPageId) throw DbError("invalid heap page ID");
    auto guard = pool.Fetch({FileKind::Database, id}, mode, ring);
    SlottedPage::Validate(guard.Data(), id, owner);
    return guard;
}
}

RID HeapFile::Insert(const Tuple& tuple) {
    const auto bytes = EncodeTuple(table_.schema, tuple);
    if (table_.live_rows == std::numeric_limits<uint64_t>::max())
        throw DbError("table row count overflow");
    if (table_.first_page == kInvalidPageId) {
        auto page = pool_.NewPage(FileKind::Database);
        const PageId id = page.Page().page_id;
        SlottedPage::Initialize(page.MutableData(), id, table_.meta_page);
        const auto slot = SlottedPage::Insert(page.MutableData(), bytes);
        if (!slot) throw DbError("tuple does not fit empty heap page");
        page.MarkDirty();
        table_.first_page = table_.last_page = id;
        table_.live_rows = 1;
        table_.heap_pages = 1;
        return {id, *slot};
    }
    auto tail = FetchHeap(pool_, table_.last_page, table_.meta_page, AccessMode::Normal);
    if (SlottedPage::Next(tail.Data()) != kInvalidPageId)
        throw DbError("table tail has a next link");
    const auto slot = SlottedPage::Insert(tail.MutableData(), bytes);
    if (slot) {
        tail.MarkDirty();
        ++table_.live_rows;
        return {table_.last_page, *slot};
    }
    if (table_.heap_pages == std::numeric_limits<uint32_t>::max())
        throw DbError("table page count overflow");
    tail.Drop();
    auto next = pool_.NewPage(FileKind::Database);
    const PageId id = next.Page().page_id;
    SlottedPage::Initialize(next.MutableData(), id, table_.meta_page);
    const auto new_slot = SlottedPage::Insert(next.MutableData(), bytes);
    if (!new_slot) throw DbError("tuple does not fit empty heap page");
    next.MarkDirty();
    next.Drop();
    tail = FetchHeap(pool_, table_.last_page, table_.meta_page, AccessMode::Normal);
    SlottedPage::SetNext(tail.MutableData(), id);
    tail.MarkDirty();
    table_.last_page = id;
    ++table_.heap_pages;
    ++table_.live_rows;
    return {id, *new_slot};
}

std::optional<Tuple> HeapFile::Fetch(RID rid) const {
    if (rid.page_id == kInvalidPageId) return std::nullopt;
    auto page = FetchHeap(pool_, rid.page_id, table_.meta_page, AccessMode::Normal);
    auto bytes = SlottedPage::Fetch(page.Data(), rid.slot_id);
    page.Drop();
    if (!bytes) return std::nullopt;
    return DecodeTuple(table_.schema, *bytes);
}

bool HeapFile::Erase(RID rid) {
    if (rid.page_id == kInvalidPageId) return false;
    auto page = FetchHeap(pool_, rid.page_id, table_.meta_page, AccessMode::Normal);
    if (!SlottedPage::Fetch(page.Data(), rid.slot_id)) return false;
    if (table_.live_rows == 0) throw DbError("table row count underflow");
    SlottedPage::Erase(page.MutableData(), rid.slot_id);
    page.MarkDirty();
    --table_.live_rows;
    return true;
}

HeapScan::HeapScan(BufferPool& pool, TableInfo table, const uint64_t& epoch, ScanRing* ring)
    : pool_(pool), table_(std::move(table)), epoch_(epoch), captured_epoch_(epoch),
      page_(table_.first_page), ring_(ring) {}

void HeapScan::Reset() {
    if (epoch_ != captured_epoch_) throw DbError("heap scan invalidated by mutation");
    page_ = table_.first_page;
    slot_ = 0;
    completed_pages_ = 0;
}

std::optional<std::pair<RID, Tuple>> HeapScan::Next() {
    if (epoch_ != captured_epoch_) throw DbError("heap scan invalidated by mutation");
    while (page_ != kInvalidPageId) {
        auto guard = FetchHeap(pool_, page_, table_.meta_page, AccessMode::Scan, ring_);
        while (slot_ < SlottedPage::SlotCount(guard.Data())) {
            const uint16_t slot = slot_++;
            auto bytes = SlottedPage::Fetch(guard.Data(), slot);
            if (!bytes) continue;
            RID rid{page_, slot};
            guard.Drop();
            return std::make_pair(rid, DecodeTuple(table_.schema, *bytes));
        }
        const PageId next = SlottedPage::Next(guard.Data());
        guard.Drop();
        ++completed_pages_;
        if (completed_pages_ > table_.heap_pages ||
            (next == kInvalidPageId &&
             (page_ != table_.last_page || completed_pages_ != table_.heap_pages)))
            throw DbError("inconsistent heap page chain");
        page_ = next;
        slot_ = 0;
    }
    return std::nullopt;
}

}  // namespace valencedb
