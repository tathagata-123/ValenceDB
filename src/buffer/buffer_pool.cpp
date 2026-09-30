#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/common/error.hpp"

#include <cassert>
#include <limits>

namespace valencedb {

BufferPool::BufferPool(DiskManager& disk, size_t frame_count)
    : disk_(disk), frames_(frame_count) {
    if (frame_count == 0 || frame_count > std::numeric_limits<size_t>::max() / 6)
        throw DbError("buffer pool frame count is invalid");
    page_table_.reserve(frame_count);
}

size_t BufferPool::FindInvalid() const noexcept {
    for (size_t i = 0; i < frames_.size(); ++i)
        if (!frames_[i].valid) return i;
    return frames_.size();
}

size_t BufferPool::SweepVictim() {
    // Each unpinned frame can be decremented at most five times before selection.
    for (size_t inspected = 0; inspected < 6 * frames_.size(); ++inspected) {
        const size_t index = sweep_.Next(frames_.size());
        Frame& frame = frames_[index];
        ++metrics_.sweep_inspections;
        if (!frame.valid) return index;
        if (frame.pin_count != 0) continue;
        if (frame.usage_count != 0) {
            --frame.usage_count;
            ++metrics_.sweep_decrements;
            continue;
        }
        return index;
    }
    throw DbError("buffer pool exhausted: all frames pinned");
}

size_t BufferPool::ChooseFrame(ScanRing* ring, bool& ring_reuse) {
    ring_reuse = false;
    const size_t empty = FindInvalid();
    if (empty != frames_.size()) {
        if (ring) ++metrics_.ring_fallbacks;
        return empty;
    }
    if (ring) {
        const RingHandle handle = ring->Current();
        if (handle.occupied && handle.frame_index < frames_.size()) {
            const Frame& frame = frames_[handle.frame_index];
            if (frame.valid && frame.generation == handle.generation &&
                frame.pin_count == 0 && frame.usage_count <= 1) {
                ring_reuse = true;
                return handle.frame_index;
            }
        }
        ++metrics_.ring_fallbacks;
    }
    return SweepVictim();
}

void BufferPool::Install(size_t index, PageRef page, const PageBytes& bytes) {
    Frame& frame = frames_[index];
    assert(frame.pin_count == 0);
    assert(page_table_.find(page) == page_table_.end());
    if (frame.valid && frame.dirty) {
        disk_.Write(frame.page, frame.data);
        ++metrics_.dirty_eviction_writes;
    }

    // Allocate the new map node before changing the old frame. If allocation throws,
    // the old mapping and bytes still describe the resident page.
    [[maybe_unused]] const auto inserted = page_table_.emplace(page, index);
    assert(inserted.second);

    if (frame.valid) {
        assert(page_table_.at(frame.page) == index);
        page_table_.erase(frame.page);
        ++metrics_.evictions;
        if (frame.dirty) ++metrics_.dirty_evictions;
    }
    frame.data = bytes;
    frame.page = page;
    frame.valid = true;
    frame.dirty = false;
    frame.usage_count = 1;
    ++frame.generation;
    Pin(index);
}

void BufferPool::Pin(size_t index) noexcept {
    Frame& frame = frames_[index];
    assert(frame.valid && frame.pin_count < std::numeric_limits<uint32_t>::max());
    ++frame.pin_count;
    ++metrics_.active_pins;
    if (metrics_.active_pins > metrics_.peak_pins)
        metrics_.peak_pins = metrics_.active_pins;
}

void BufferPool::Unpin(size_t index) noexcept {
    Frame& frame = frames_[index];
    assert(frame.valid && frame.pin_count > 0 && metrics_.active_pins > 0);
    --frame.pin_count;
    --metrics_.active_pins;
}

void BufferPool::CheckGuard([[maybe_unused]] size_t index) const {
    assert(index < frames_.size());
    assert(frames_[index].valid && frames_[index].pin_count > 0);
}

PageGuard BufferPool::Fetch(PageRef page, AccessMode mode, ScanRing* ring) {
    if (ring && (mode != AccessMode::Scan || ring->pool_ != this))
        throw DbError("scan ring does not belong to this scan and buffer pool");
    ++metrics_.buffer_fetches;
    auto found = page_table_.find(page);
    if (found != page_table_.end()) {
        ++metrics_.buffer_hits;
        Frame& frame = frames_[found->second];
        assert(frame.valid && frame.page == page && frame.usage_count <= 5);
        if (mode == AccessMode::Normal) {
            if (frame.usage_count < 5) ++frame.usage_count;
        } else if (frame.usage_count == 0) {
            frame.usage_count = 1;
        }
        Pin(found->second);
        return PageGuard(*this, found->second);
    }

    ++metrics_.buffer_misses;
    bool ring_reuse = false;
    const size_t index = ChooseFrame(ring, ring_reuse);
    PageBytes incoming{};
    disk_.Read(page, incoming);  // Staged: failure leaves the old mapping and bytes intact.
    Install(index, page, incoming);
    if (ring) {
        if (ring_reuse) ++metrics_.ring_reuses;
        ring->Record(index, frames_[index].generation);
    }
    return PageGuard(*this, index);
}

PageGuard BufferPool::NewPage(FileKind kind) {
    const PageRef page{kind, disk_.Allocate(kind)};
    ++metrics_.new_pages;
    bool unused = false;
    const size_t index = ChooseFrame(nullptr, unused);
    const PageBytes zero{};  // DiskManager allocated and wrote the same zero page.
    Install(index, page, zero);
    return PageGuard(*this, index);
}

void BufferPool::Flush(PageRef page) {
    auto found = page_table_.find(page);
    if (found == page_table_.end()) return;
    Frame& frame = frames_[found->second];
    if (!frame.dirty) return;
    disk_.Write(page, frame.data);
    ++metrics_.explicit_flush_writes;
    disk_.Flush(page.file);
    frame.dirty = false;
}

void BufferPool::FlushAll() {
    for (const Frame& frame : frames_)
        if (frame.valid && frame.page.file == FileKind::Database)
            Flush(frame.page);
    disk_.Flush(FileKind::Database);
}

void BufferPool::InvalidateFrame(size_t index) noexcept {
    Frame& frame = frames_[index];
    assert(frame.valid && frame.pin_count == 0);
    page_table_.erase(frame.page);
    frame.valid = false;
    frame.page = {FileKind::Database, kInvalidPageId};
    frame.dirty = false;
    frame.usage_count = 0;
    ++frame.generation;
}

void BufferPool::Invalidate(PageRef page) {
    auto found = page_table_.find(page);
    if (found == page_table_.end()) return;
    Frame& frame = frames_[found->second];
    if (frame.pin_count != 0) throw DbError("cannot invalidate a pinned page");
    if (frame.dirty) Flush(page);
    InvalidateFrame(found->second);
}

void BufferPool::DiscardTemporaryPages() {
    for (const Frame& frame : frames_)
        if (frame.valid && frame.page.file == FileKind::Temporary && frame.pin_count != 0)
            throw DbError("cannot discard pinned temporary pages");
    for (size_t index = 0; index < frames_.size(); ++index)
        if (frames_[index].valid && frames_[index].page.file == FileKind::Temporary)
            InvalidateFrame(index);
}

std::vector<FrameSnapshot> BufferPool::InspectFrames() const {
    std::vector<FrameSnapshot> snapshot;
    snapshot.reserve(frames_.size());
    for (const Frame& frame : frames_)
        snapshot.push_back({frame.valid ? std::optional<PageRef>(frame.page) : std::nullopt,
                            frame.pin_count, frame.dirty, frame.usage_count, frame.generation});
    return snapshot;
}

}  // namespace valencedb
