#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace valencedb {

class BufferPool;

struct RingHandle {
    size_t frame_index = 0;
    uint64_t generation = 0;
    bool occupied = false;
};

// Holds frame identities only; it never owns page bytes or pins.
class ScanRing {
public:
    explicit ScanRing(BufferPool& pool, size_t size_override = 0);
    size_t size() const noexcept { return slots_.size(); }
    size_t cursor() const noexcept { return cursor_; }
    RingHandle Current() const noexcept { return slots_[cursor_]; }

private:
    friend class BufferPool;
    BufferPool* pool_;
    std::vector<RingHandle> slots_;
    size_t cursor_ = 0;
    void Record(size_t frame_index, uint64_t generation) noexcept;
};

}  // namespace valencedb
