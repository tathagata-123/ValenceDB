#pragma once

#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/index/bplus_iterator.hpp"

#include <vector>

namespace valencedb {

// One live handle per tree. The pool must outlive this nonmoving handle and its cursors.
class BPlusTree {
public:
    static BPlusTree Create(BufferPool& pool, uint16_t key_column = UINT16_MAX,
                            BPlusCapacity capacity = {});
    static BPlusTree Open(BufferPool& pool, PageId meta_page, BPlusCapacity capacity = {});
    BPlusTree(const BPlusTree&) = delete;
    BPlusTree& operator=(const BPlusTree&) = delete;

    bool Insert(int64_t key, RID rid);
    bool Contains(int64_t key, RID rid) const;
    BPlusIterator LowerBound(int64_t key) const;
    BPlusIterator EqualRange(int64_t key) const;
    BPlusIterator Range(int64_t low, int64_t high) const;  // Inclusive; low > high is empty.

    PageId MetaPage() const noexcept { return meta_page_; }
    PageId RootPage() const noexcept { return meta_.root; }
    uint16_t Height() const noexcept { return meta_.height; }
    uint16_t KeyColumn() const noexcept { return meta_.key_column; }
    BPlusCapacity Capacity() const noexcept { return capacity_; }
    uint64_t MutationEpoch() const noexcept { return epoch_; }

private:
    friend class BPlusIterator;
    struct Ancestor {
        PageId page;
        uint16_t child_index;
    };
    BPlusTree(BufferPool& pool, PageId meta_page, BPlusMeta meta, BPlusCapacity capacity)
        : pool_(pool), meta_page_(meta_page), meta_(meta), capacity_(capacity) {}
    PageId Descend(BPlusKey key, std::vector<Ancestor>* path = nullptr) const;
    BPlusIterator Start(int64_t key, std::optional<int64_t> upper) const;
    void WriteLeaf(PageId id, std::span<const BPlusKey> entries, PageId next);
    void WriteInternal(PageId id, std::span<const PageId> children,
                       std::span<const BPlusKey> separators);
    void SetRoot(PageId root, uint16_t height);
    PageId AllocatePage();
    struct LeafNode {
        std::vector<BPlusKey> entries;
        PageId next;
    };
    struct InternalNode {
        std::vector<PageId> children;
        std::vector<BPlusKey> separators;
    };
    LeafNode ReadLeaf(PageId id) const;
    InternalNode ReadInternal(PageId id) const;
    struct SplitResult {
        BPlusKey separator;
        PageId right;
    };
    SplitResult SplitLeaf(PageId id, const LeafNode& node);
    SplitResult SplitInternal(PageId id, const InternalNode& node);

    BufferPool& pool_;
    PageId meta_page_;
    BPlusMeta meta_;
    BPlusCapacity capacity_;
    uint64_t epoch_ = 0;
};

}  // namespace valencedb
