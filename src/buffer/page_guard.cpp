#include "valencedb/buffer/page_guard.hpp"
#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/common/error.hpp"

#include <utility>

namespace valencedb {

PageGuard::PageGuard(BufferPool& pool, size_t frame_index) noexcept
    : pool_(&pool), frame_index_(frame_index) {}

PageGuard::PageGuard(PageGuard&& other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)), frame_index_(other.frame_index_) {}

PageGuard& PageGuard::operator=(PageGuard&& other) noexcept {
    if (this != &other) {
        Drop();
        pool_ = std::exchange(other.pool_, nullptr);
        frame_index_ = other.frame_index_;
    }
    return *this;
}

PageGuard::~PageGuard() noexcept { Drop(); }

PageRef PageGuard::Page() const {
    if (!pool_) throw DbError("page guard is empty");
    pool_->CheckGuard(frame_index_);
    return pool_->frames_[frame_index_].page;
}

const PageBytes& PageGuard::Data() const {
    if (!pool_) throw DbError("page guard is empty");
    pool_->CheckGuard(frame_index_);
    return pool_->frames_[frame_index_].data;
}

PageBytes& PageGuard::MutableData() {
    if (!pool_) throw DbError("page guard is empty");
    pool_->CheckGuard(frame_index_);
    return pool_->frames_[frame_index_].data;
}

void PageGuard::MarkDirty() {
    if (!pool_) throw DbError("page guard is empty");
    pool_->CheckGuard(frame_index_);
    pool_->frames_[frame_index_].dirty = true;
}

void PageGuard::Drop() noexcept {
    if (pool_) {
        pool_->Unpin(frame_index_);
        pool_ = nullptr;
    }
}

}  // namespace valencedb
