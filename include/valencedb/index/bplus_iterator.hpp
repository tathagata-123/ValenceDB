#pragma once

#include "valencedb/index/bplus_page.hpp"

#include <optional>

namespace valencedb {

class BPlusTree;

// The tree and its BufferPool must outlive the cursor. No page guard is retained.
class BPlusIterator {
public:
    std::optional<BPlusKey> Next();

private:
    friend class BPlusTree;
    BPlusIterator(const BPlusTree& tree, PageId leaf, uint16_t offset,
                  std::optional<int64_t> upper = std::nullopt);
    const BPlusTree& tree_;
    PageId leaf_;
    uint16_t offset_;
    uint64_t expected_epoch_;
    std::optional<int64_t> upper_;
};

}  // namespace valencedb
