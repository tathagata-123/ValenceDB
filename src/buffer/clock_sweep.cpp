#include "valencedb/buffer/clock_sweep.hpp"

namespace valencedb {

size_t ClockSweep::Next(size_t frame_count) noexcept {
    const size_t inspected = hand_;
    hand_ = (hand_ + 1 == frame_count) ? 0 : hand_ + 1;
    return inspected;
}

}  // namespace valencedb
