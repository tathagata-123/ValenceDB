#pragma once

#include "test_harness.h"
#include "valencedb/index/bplus_tree.hpp"

#include <set>
#include <tuple>

namespace bplus_test {
using namespace valencedb;

// Independent of BPlusKey::operator< and of every B+ page/search helper.
struct OracleLess {
    bool operator()(const BPlusKey& a, const BPlusKey& b) const {
        return std::tie(a.key, a.rid.page_id, a.rid.slot_id) <
               std::tie(b.key, b.rid.page_id, b.rid.slot_id);
    }
};
using Oracle = std::set<BPlusKey, OracleLess>;

inline BPlusKey RawKey(const PageBytes& page, size_t at) {
    BPlusKey key{ReadI64(page, at), {ReadU32(page, at + 8), ReadU16(page, at + 12)}};
    VDB_CHECK(ReadU16(page, at + 14) == 0);
    VDB_CHECK(key.rid.page_id != kInvalidPageId);
    return key;
}

struct TreeShape {
    std::set<PageId> reachable;
    std::vector<PageId> leaves;
    std::vector<BPlusKey> entries;
    size_t nodes = 0;
    uint16_t height = 0;
};

// Reads actual persistent pages through BufferPool, never production tree traversal.
class Validator {
public:
    Validator(BufferPool& pool, uint32_t allocated, const BPlusTree& tree)
        : pool_(pool), allocated_(allocated), tree_(tree), capacity_(tree.Capacity()) {}

    TreeShape Run() {
        VDB_CHECK(tree_.MetaPage() < allocated_);
        auto meta = pool_.Fetch({FileKind::Database, tree_.MetaPage()});
        ValidatePageHeader(meta.Data(), PageType::IndexMeta, tree_.MetaPage());
        VDB_CHECK(ReadU32(meta.Data(), 12) == 0);
        VDB_CHECK(ReadU32(meta.Data(), 16) == tree_.RootPage());
        VDB_CHECK(ReadU16(meta.Data(), 20) == tree_.Height());
        VDB_CHECK(ReadU16(meta.Data(), 22) == tree_.KeyColumn());
        VDB_CHECK(tree_.Height() >= 1 && tree_.Height() <= 32);
        for (size_t at = 24; at < kPageSize; ++at) VDB_CHECK(meta.Data()[at] == std::byte{0});
        meta.Drop();
        seen_.insert(tree_.MetaPage());
        Walk(tree_.RootPage(), 1, true);
        shape_.reachable = seen_;
        VDB_CHECK(shape_.height == tree_.Height());

        // Independently walk the linked list and compare both page order and contents.
        std::set<PageId> chain_seen;
        std::vector<PageId> chain;
        std::vector<BPlusKey> entries;
        PageId id = shape_.leaves.front();
        while (id != kInvalidPageId) {
            VDB_CHECK(id < allocated_ && chain_seen.insert(id).second);
            auto guard = pool_.Fetch({FileKind::Database, id});
            ValidatePageHeader(guard.Data(), PageType::BPlusLeaf, id);
            const auto count = ReadU16(guard.Data(), 16);
            VDB_CHECK(count <= capacity_.leaf_max && count <= 254);
            chain.push_back(id);
            for (size_t i = 0; i < count; ++i) entries.push_back(RawKey(guard.Data(), 32 + 16 * i));
            id = ReadU32(guard.Data(), 20);
        }
        VDB_CHECK(chain == shape_.leaves);
        VDB_CHECK(entries == shape_.entries);
        for (size_t i = 1; i < entries.size(); ++i) VDB_CHECK(OracleLess{}(entries[i - 1], entries[i]));
        VDB_CHECK(pool_.metrics().active_pins == 0);
        return shape_;
    }

private:
    struct Bounds { std::optional<BPlusKey> minimum, maximum; };

    Bounds Walk(PageId id, uint16_t depth, bool root) {
        VDB_CHECK(id < allocated_ && seen_.insert(id).second);
        VDB_CHECK(depth <= tree_.Height());
        ++shape_.nodes;
        auto guard = pool_.Fetch({FileKind::Database, id});
        const auto& page = guard.Data();
        VDB_CHECK(ReadU32(page, 12) == 0);
        VDB_CHECK(ReadU16(page, 18) == 0 && ReadU64(page, 24) == 0);
        const uint16_t count = ReadU16(page, 16);
        if (depth == tree_.Height()) {
            ValidatePageHeader(page, PageType::BPlusLeaf, id);
            VDB_CHECK(count <= capacity_.leaf_max && count <= 254);
            VDB_CHECK(root || count >= (capacity_.leaf_max + 1) / 2);
            VDB_CHECK(root || count != 0);
            VDB_CHECK(ReadU32(page, 20) == kInvalidPageId || ReadU32(page, 20) < allocated_);
            shape_.leaves.push_back(id);
            shape_.height = depth;
            Bounds bounds;
            for (size_t i = 0; i < count; ++i) {
                const auto entry = RawKey(page, 32 + 16 * i);
                if (bounds.maximum) VDB_CHECK(OracleLess{}(*bounds.maximum, entry));
                if (i == 0) bounds.minimum = entry;
                bounds.maximum = entry;
                shape_.entries.push_back(entry);
            }
            return bounds;
        }

        ValidatePageHeader(page, PageType::BPlusInternal, id);
        VDB_CHECK(count >= 2 && count <= capacity_.internal_max && count <= 204);
        VDB_CHECK(root || count >= (capacity_.internal_max + 1) / 2);
        std::vector<PageId> children{ReadU32(page, 20)};
        std::vector<BPlusKey> separators;
        for (size_t i = 0; i < size_t(count - 1); ++i) {
            separators.push_back(RawKey(page, 32 + 20 * i));
            children.push_back(ReadU32(page, 48 + 20 * i));
            if (i != 0) VDB_CHECK(OracleLess{}(separators[i - 1], separators[i]));
        }
        guard.Drop();  // Recursive inspection does not itself inflate peak pins.
        Bounds total;
        Bounds previous;
        for (size_t i = 0; i < children.size(); ++i) {
            const auto child = Walk(children[i], depth + 1, false);
            VDB_CHECK(child.minimum && child.maximum);
            if (i == 0) total.minimum = child.minimum;
            else {
                VDB_CHECK(OracleLess{}(*previous.maximum, separators[i - 1]));
                VDB_CHECK(!OracleLess{}(*child.minimum, separators[i - 1]));
            }
            total.maximum = child.maximum;
            previous = child;
        }
        return total;
    }

    BufferPool& pool_;
    uint32_t allocated_;
    const BPlusTree& tree_;
    BPlusCapacity capacity_;
    std::set<PageId> seen_;
    TreeShape shape_;
};
}  // namespace bplus_test
