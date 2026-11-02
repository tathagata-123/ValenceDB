#pragma once

#include "valencedb/buffer/page_guard.hpp"
#include "valencedb/buffer/scan_ring.hpp"
#include "valencedb/storage/disk_manager.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace valencedb {

enum class AccessMode { Normal, Scan };

struct BufferMetrics {
    uint64_t buffer_fetches = 0;
    uint64_t buffer_hits = 0;
    uint64_t buffer_misses = 0;
    uint64_t new_pages = 0;
    uint64_t evictions = 0;
    uint64_t dirty_evictions = 0;
    // Successful DiskManager::Write calls by cause, even if later cleanup/install fails.
    uint64_t explicit_flush_writes = 0;
    uint64_t dirty_eviction_writes = 0;
    uint64_t sweep_inspections = 0;
    uint64_t sweep_decrements = 0;
    uint64_t ring_reuses = 0;
    uint64_t ring_fallbacks = 0;
    uint64_t active_pins = 0;
    uint64_t peak_pins = 0;
};

struct FrameSnapshot {
    std::optional<PageRef> page;
    uint32_t pin_count = 0;
    bool dirty = false;
    uint8_t usage_count = 0;
    uint64_t generation = 0;
};

class BufferPool {
public:
    BufferPool(DiskManager& disk, size_t frame_count);
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    PageGuard Fetch(PageRef page, AccessMode mode = AccessMode::Normal,
                    ScanRing* ring = nullptr);
    PageGuard NewPage(FileKind kind);
    void Flush(PageRef page);  // Uncached pages are a no-op.
    void FlushAll();           // Persistent pages only; does not unpin or evict.
    void Invalidate(PageRef page);
    void DiscardTemporaryPages();

    size_t FrameCount() const noexcept { return frames_.size(); }
    size_t SweepHand() const noexcept { return clock_hand_; }
    const BufferMetrics& metrics() const noexcept { return metrics_; }
    std::vector<FrameSnapshot> InspectFrames() const;

private:
    friend class PageGuard;
    struct Frame {
        bool valid = false;
        PageRef page{FileKind::Database, kInvalidPageId};
        uint32_t pin_count = 0;
        bool dirty = false;
        uint8_t usage_count = 0;
        uint64_t generation = 0;
        PageBytes data{};
    };

    DiskManager& disk_;
    std::vector<Frame> frames_;
    std::unordered_map<PageRef, size_t, PageRefHash> page_table_;
    size_t clock_hand_ = 0;  // Next frame to inspect.
    BufferMetrics metrics_;

    size_t FindInvalid() const noexcept;
    size_t SweepVictim();
    size_t ChooseFrame(ScanRing* ring, bool& ring_reuse);
    void Install(size_t index, PageRef page, const PageBytes& bytes);
    void Pin(size_t index) noexcept;
    void Unpin(size_t index) noexcept;
    void CheckGuard(size_t index) const;
    void InvalidateFrame(size_t index) noexcept;
};

}  // namespace valencedb
