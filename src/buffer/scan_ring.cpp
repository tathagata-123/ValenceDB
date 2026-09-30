#include "valencedb/buffer/scan_ring.hpp"
#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/common/error.hpp"

#include <algorithm>

namespace valencedb {

ScanRing::ScanRing(BufferPool& pool, size_t size_override) : pool_(&pool) {
    const size_t default_size = std::min<size_t>(8, std::max<size_t>(1, pool.FrameCount() / 8));
    const size_t count = size_override == 0 ? default_size : size_override;
    if (count > pool.FrameCount()) throw DbError("scan ring exceeds buffer pool size");
    slots_.resize(count);
}

void ScanRing::Record(size_t frame_index, uint64_t generation) noexcept {
    slots_[cursor_] = RingHandle{frame_index, generation, true};
    cursor_ = (cursor_ + 1 == slots_.size()) ? 0 : cursor_ + 1;
}

}  // namespace valencedb
