#pragma once

#include "valencedb/storage/page_codec.hpp"

#include <cstddef>

namespace valencedb {

class BufferPool;

// The pool must outlive every guard. Data references are valid only while this guard owns its pin.
class PageGuard {
public:
    PageGuard() noexcept = default;
    PageGuard(const PageGuard&) = delete;
    PageGuard& operator=(const PageGuard&) = delete;
    PageGuard(PageGuard&& other) noexcept;
    PageGuard& operator=(PageGuard&& other) noexcept;
    ~PageGuard() noexcept;

    PageRef Page() const;
    const PageBytes& Data() const;
    PageBytes& MutableData();
    void MarkDirty();
    void Drop() noexcept;
    explicit operator bool() const noexcept { return pool_ != nullptr; }

private:
    friend class BufferPool;
    PageGuard(BufferPool& pool, size_t frame_index) noexcept;
    BufferPool* pool_ = nullptr;
    size_t frame_index_ = 0;
};

}  // namespace valencedb
