#include "valencedb/index/bplus_tree.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {

BPlusIterator::BPlusIterator(const BPlusTree& tree, PageId leaf, uint16_t offset,
                             std::optional<int64_t> upper)
    : tree_(tree), leaf_(leaf), offset_(offset), expected_epoch_(tree.epoch_), upper_(upper) {}

std::optional<BPlusKey> BPlusIterator::Next() {
    if (expected_epoch_ != tree_.epoch_) throw DbError("B+ cursor invalidated by mutation");
    while (leaf_ != kInvalidPageId) {
        auto guard = tree_.pool_.Fetch({FileKind::Database, leaf_});
        BPlusLeafPage::Validate(guard.Data(), leaf_, tree_.capacity_.leaf_max);
        const auto count = BPlusLeafPage::Count(guard.Data());
        if (offset_ > count) throw DbError("invalid B+ cursor offset");
        if (offset_ == count) {
            leaf_ = BPlusLeafPage::Next(guard.Data());
            offset_ = 0;
            continue;  // guard dies before fetching the next leaf.
        }
        const BPlusKey entry = BPlusLeafPage::Entry(guard.Data(), offset_);
        if (upper_ && entry.key > *upper_) {
            leaf_ = kInvalidPageId;
            return std::nullopt;
        }
        ++offset_;
        if (offset_ == count) {
            leaf_ = BPlusLeafPage::Next(guard.Data());
            offset_ = 0;
        }
        return entry;  // Owned copy; guard is released before control returns to caller.
    }
    return std::nullopt;
}

}  // namespace valencedb
