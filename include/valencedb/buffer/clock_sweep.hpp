#pragma once

#include <cstddef>

namespace valencedb {

class BufferPool;

// The hand is the next frame to inspect. BufferPool owns frame state.
class ClockSweep {
public:
    size_t hand() const noexcept { return hand_; }

private:
    friend class BufferPool;
    size_t hand_ = 0;
    size_t Next(size_t frame_count) noexcept;
};

}  // namespace valencedb
