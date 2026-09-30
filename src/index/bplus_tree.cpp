#include "valencedb/index/bplus_tree.hpp"
#include "valencedb/common/error.hpp"

#include <array>
#include <algorithm>
#include <cassert>

namespace valencedb {
namespace {
void CheckRID(RID rid) {
    if (rid.page_id == kInvalidPageId) throw DbError("invalid B+ tree RID");
}

uint16_t LeafLowerBound(const PageBytes& page, BPlusKey key) {
    uint16_t low = 0;
    uint16_t high = BPlusLeafPage::Count(page);
    while (low < high) {
        const uint16_t middle = low + (high - low) / 2;
        if (BPlusLeafPage::Entry(page, middle) < key) low = middle + 1;
        else high = middle;
    }
    return low;
}
}  // namespace

BPlusTree BPlusTree::Create(BufferPool& pool, uint16_t key_column, BPlusCapacity capacity) {
    capacity.Validate();
    if (key_column != UINT16_MAX && key_column >= 8) throw DbError("invalid B+ key column");
    auto meta_guard = pool.NewPage(FileKind::Database);
    const PageId meta_page = meta_guard.Page().page_id;
    meta_guard.Drop();
    auto leaf = pool.NewPage(FileKind::Database);
    const PageId root = leaf.Page().page_id;
    BPlusLeafPage::Initialize(leaf.MutableData(), root);
    leaf.MarkDirty();
    leaf.Drop();
    const BPlusMeta meta{root, 1, key_column};
    meta_guard = pool.Fetch({FileKind::Database, meta_page});
    BPlusMetaPage::Write(meta_guard.MutableData(), meta_page, meta);
    meta_guard.MarkDirty();
    return BPlusTree(pool, meta_page, meta, capacity);
}

BPlusTree BPlusTree::Open(BufferPool& pool, PageId meta_page, BPlusCapacity capacity) {
    capacity.Validate();
    auto guard = pool.Fetch({FileKind::Database, meta_page});
    const auto meta = BPlusMetaPage::Read(guard.Data(), meta_page);
    guard.Drop();
    guard = pool.Fetch({FileKind::Database, meta.root});
    if (meta.height == 1) {
        BPlusLeafPage::Validate(guard.Data(), meta.root, capacity.leaf_max);
        if (BPlusLeafPage::Next(guard.Data()) != kInvalidPageId)
            throw DbError("root leaf has a next leaf");
    } else {
        BPlusInternalPage::Validate(guard.Data(), meta.root, capacity.internal_max);
    }
    return BPlusTree(pool, meta_page, meta, capacity);
}

PageId BPlusTree::Descend(BPlusKey key, std::vector<Ancestor>* path) const {
    PageId page = meta_.root;
    for (uint16_t level = 1; level < meta_.height; ++level) {
        auto guard = pool_.Fetch({FileKind::Database, page});
        BPlusInternalPage::Validate(guard.Data(), page, capacity_.internal_max);
        const uint16_t count = BPlusInternalPage::ChildCount(guard.Data());
        // upper_bound: equality with a lower fence routes to its right child.
        uint16_t low = 0;
        uint16_t high = count - 1;
        while (low < high) {
            const uint16_t middle = low + (high - low) / 2;
            if (key < BPlusInternalPage::Separator(guard.Data(), middle)) high = middle;
            else low = middle + 1;
        }
        if (path) path->push_back({page, low, count});
        page = BPlusInternalPage::Child(guard.Data(), low);
        // guard dies here; the ancestor path contains only numbers.
    }
    return page;
}

bool BPlusTree::Contains(int64_t key, RID rid) const {
    CheckRID(rid);
    const BPlusKey entry{key, rid};
    const PageId leaf = Descend(entry);
    auto guard = pool_.Fetch({FileKind::Database, leaf});
    BPlusLeafPage::Validate(guard.Data(), leaf, capacity_.leaf_max);
    const auto at = LeafLowerBound(guard.Data(), entry);
    return at < BPlusLeafPage::Count(guard.Data()) && BPlusLeafPage::Entry(guard.Data(), at) == entry;
}

BPlusIterator BPlusTree::Start(int64_t key, std::optional<int64_t> upper) const {
    const BPlusKey entry{key, {0, 0}};
    PageId leaf = Descend(entry);
    auto guard = pool_.Fetch({FileKind::Database, leaf});
    BPlusLeafPage::Validate(guard.Data(), leaf, capacity_.leaf_max);
    uint16_t at = LeafLowerBound(guard.Data(), entry);
    while (at == BPlusLeafPage::Count(guard.Data())) {
        leaf = BPlusLeafPage::Next(guard.Data());
        guard.Drop();
        at = 0;
        if (leaf == kInvalidPageId) break;
        guard = pool_.Fetch({FileKind::Database, leaf});
        BPlusLeafPage::Validate(guard.Data(), leaf, capacity_.leaf_max);
    }
    return BPlusIterator(*this, leaf, at, upper);
}

BPlusIterator BPlusTree::LowerBound(int64_t key) const { return Start(key, std::nullopt); }
BPlusIterator BPlusTree::EqualRange(int64_t key) const { return Range(key, key); }
BPlusIterator BPlusTree::Range(int64_t low, int64_t high) const {
    if (low > high) return BPlusIterator(*this, kInvalidPageId, 0, high);
    return Start(low, high);
}

void BPlusTree::WriteLeaf(PageId id, std::span<const BPlusKey> entries, PageId next) {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusLeafPage::Initialize(guard.MutableData(), id);
    BPlusLeafPage::Write(guard.MutableData(), entries, capacity_.leaf_max);
    BPlusLeafPage::SetNext(guard.MutableData(), next);
    guard.MarkDirty();
}

void BPlusTree::WriteInternal(PageId id, std::span<const PageId> children,
                             std::span<const BPlusKey> separators) {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusInternalPage::Initialize(guard.MutableData(), id);
    BPlusInternalPage::Write(guard.MutableData(), children, separators, capacity_.internal_max);
    guard.MarkDirty();
}

void BPlusTree::SetRoot(PageId root, uint16_t height) {
    auto guard = pool_.Fetch({FileKind::Database, meta_page_});
    (void)BPlusMetaPage::Read(guard.Data(), meta_page_);
    const BPlusMeta updated{root, height, meta_.key_column};
    BPlusMetaPage::Write(guard.MutableData(), meta_page_, updated);
    guard.MarkDirty();
    meta_ = updated;
}

bool BPlusTree::Insert(int64_t key, RID rid) {
    CheckRID(rid);
    const BPlusKey entry{key, rid};
    std::vector<Ancestor> path;
    const PageId leaf = Descend(entry, &path);
    auto guard = pool_.Fetch({FileKind::Database, leaf});
    BPlusLeafPage::Validate(guard.Data(), leaf, capacity_.leaf_max);
    const uint16_t count = BPlusLeafPage::Count(guard.Data());
    const uint16_t at = LeafLowerBound(guard.Data(), entry);
    if (at < count && BPlusLeafPage::Entry(guard.Data(), at) == entry) return false;

    // Only this leaf's bounded overflow representation lives in memory.
    std::vector<BPlusKey> entries;
    entries.reserve(count + 1);
    for (uint16_t i = 0; i < count; ++i) entries.push_back(BPlusLeafPage::Entry(guard.Data(), i));
    entries.insert(entries.begin() + at, entry);
    if (count < capacity_.leaf_max) {
        BPlusLeafPage::Write(guard.MutableData(), entries, capacity_.leaf_max);
        guard.MarkDirty();
        guard.Drop();
        ++epoch_;
        return true;
    }
    const PageId old_next = BPlusLeafPage::Next(guard.Data());
    guard.Drop();

    // Predict the whole cascade before changing any existing page.
    size_t needed = 1;  // right leaf
    bool grows_root = true;
    for (size_t level = path.size(); level > 0; --level) {
        if (path[level - 1].child_count < capacity_.internal_max) {
            grows_root = false;
            break;
        }
        ++needed;  // right internal node
    }
    if (grows_root) {
        if (meta_.height == kBPlusMaxHeight) throw DbError("B+ tree height limit");
        ++needed;
    }
    std::vector<PageId> reserved;
    reserved.reserve(needed);
    for (size_t i = 0; i < needed; ++i) {
        auto allocation = pool_.NewPage(FileKind::Database);
        reserved.push_back(allocation.Page().page_id);
        // No reservation guard survives this iteration. Unused pages remain unreachable.
    }

    // Leaf split: original page is left; floor(total/2) gives 127 + 128 in production.
    size_t used = 0;
    PageId right = reserved[used++];
    const size_t left_count = entries.size() / 2;
    BPlusKey separator = entries[left_count];
    const std::span<const BPlusKey> all_entries(entries);
    WriteLeaf(right, all_entries.subspan(left_count), old_next);
    WriteLeaf(leaf, all_entries.first(left_count), right);
    PageId left = leaf;

    // Insert separator/right child at the exact descent index, then propagate if full.
    for (size_t level = path.size(); level > 0; --level) {
        const Ancestor parent = path[level - 1];
        auto parent_guard = pool_.Fetch({FileKind::Database, parent.page});
        BPlusInternalPage::Validate(parent_guard.Data(), parent.page, capacity_.internal_max);
        assert(BPlusInternalPage::ChildCount(parent_guard.Data()) == parent.child_count);
        assert(BPlusInternalPage::Child(parent_guard.Data(), parent.child_index) == left);
        std::vector<PageId> children;
        std::vector<BPlusKey> separators;
        children.reserve(parent.child_count + 1);
        separators.reserve(parent.child_count);
        for (size_t i = 0; i < parent.child_count; ++i) {
            children.push_back(BPlusInternalPage::Child(parent_guard.Data(), i));
            if (i != 0) separators.push_back(BPlusInternalPage::Separator(parent_guard.Data(), i - 1));
        }
        parent_guard.Drop();
        children.insert(children.begin() + parent.child_index + 1, right);
        separators.insert(separators.begin() + parent.child_index, separator);
        if (children.size() <= capacity_.internal_max) {
            WriteInternal(parent.page, children, separators);
            assert(used == reserved.size());
            ++epoch_;
            return true;
        }

        // Internal split: promote the boundary and omit it from BOTH split pages.
        const size_t left_children = children.size() / 2;
        separator = separators[left_children - 1];
        right = reserved[used++];
        const std::span<const PageId> all_children(children);
        const std::span<const BPlusKey> all_separators(separators);
        WriteInternal(right, all_children.subspan(left_children), all_separators.subspan(left_children));
        WriteInternal(parent.page, all_children.first(left_children), all_separators.first(left_children - 1));
        left = parent.page;
    }

    // Propagation reached above the old root. Publish the new root through IndexMeta.
    assert(grows_root && left == meta_.root);
    const PageId new_root = reserved[used++];
    WriteInternal(new_root, std::array<PageId, 2>{left, right}, std::array<BPlusKey, 1>{separator});
    SetRoot(new_root, static_cast<uint16_t>(meta_.height + 1));
    assert(used == reserved.size());
    ++epoch_;
    return true;
}

BPlusTree::LeafNode BPlusTree::ReadLeaf(PageId id) const {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusLeafPage::Validate(guard.Data(), id, capacity_.leaf_max);
    LeafNode node{{}, BPlusLeafPage::Next(guard.Data())};
    for (size_t i = 0; i < BPlusLeafPage::Count(guard.Data()); ++i)
        node.entries.push_back(BPlusLeafPage::Entry(guard.Data(), i));
    return node;
}

BPlusTree::InternalNode BPlusTree::ReadInternal(PageId id) const {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusInternalPage::Validate(guard.Data(), id, capacity_.internal_max);
    InternalNode node;
    for (size_t i = 0; i < BPlusInternalPage::ChildCount(guard.Data()); ++i) {
        node.children.push_back(BPlusInternalPage::Child(guard.Data(), i));
        if (i != 0) node.separators.push_back(BPlusInternalPage::Separator(guard.Data(), i - 1));
    }
    return node;
}

void BPlusTree::InternalNode::RemoveRightChild(size_t left) {
    assert(left + 1 < children.size());
    children.erase(children.begin() + left + 1);
    separators.erase(separators.begin() + left);
    assert(children.size() == separators.size() + 1);
}

PageId BPlusTree::RepairLeaf(PageId id, LeafNode& current, InternalNode& parent, size_t index) {
    // Deterministic policy: lend left, lend right, merge left, merge right.
    if (index > 0) {
        const PageId left_id = parent.children[index - 1];
        auto left = ReadLeaf(left_id);
        if (left.entries.size() > capacity_.LeafMin()) {
            const BPlusKey moved = left.entries.back();
            left.entries.pop_back();
            current.entries.insert(current.entries.begin(), moved);
            parent.separators[index - 1] = moved;
            WriteLeaf(left_id, left.entries, left.next);
            WriteLeaf(id, current.entries, current.next);
            return kInvalidPageId;
        }
    }
    if (index + 1 < parent.children.size()) {
        const PageId right_id = parent.children[index + 1];
        auto right = ReadLeaf(right_id);
        if (right.entries.size() > capacity_.LeafMin()) {
            current.entries.push_back(right.entries.front());
            right.entries.erase(right.entries.begin());
            parent.separators[index] = right.entries.front();
            WriteLeaf(right_id, right.entries, right.next);
            WriteLeaf(id, current.entries, current.next);
            return kInvalidPageId;
        }
    }
    // Always retain the left page and skip the retired right page in the leaf chain.
    if (index > 0) {
        const PageId left_id = parent.children[index - 1];
        auto left = ReadLeaf(left_id);
        assert(left.next == id);
        left.entries.insert(left.entries.end(), current.entries.begin(), current.entries.end());
        WriteLeaf(left_id, left.entries, current.next);
        parent.RemoveRightChild(index - 1);
        return id;
    }
    const PageId right_id = parent.children[1];
    auto right = ReadLeaf(right_id);
    assert(current.next == right_id);
    current.entries.insert(current.entries.end(), right.entries.begin(), right.entries.end());
    WriteLeaf(id, current.entries, right.next);
    parent.RemoveRightChild(0);
    return right_id;
}

PageId BPlusTree::RepairInternal(PageId id, InternalNode& current,
                                InternalNode& parent, size_t index) {
    if (index > 0) {
        const PageId left_id = parent.children[index - 1];
        auto left = ReadInternal(left_id);
        if (left.children.size() > capacity_.InternalMin()) {
            // Rotate left's last fence UP and the old parent fence DOWN.
            current.children.insert(current.children.begin(), left.children.back());
            current.separators.insert(current.separators.begin(), parent.separators[index - 1]);
            parent.separators[index - 1] = left.separators.back();
            left.children.pop_back();
            left.separators.pop_back();
            WriteInternal(left_id, left.children, left.separators);
            WriteInternal(id, current.children, current.separators);
            return kInvalidPageId;
        }
    }
    if (index + 1 < parent.children.size()) {
        const PageId right_id = parent.children[index + 1];
        auto right = ReadInternal(right_id);
        if (right.children.size() > capacity_.InternalMin()) {
            // Rotate the old parent fence DOWN and right's first fence UP.
            current.children.push_back(right.children.front());
            current.separators.push_back(parent.separators[index]);
            parent.separators[index] = right.separators.front();
            right.children.erase(right.children.begin());
            right.separators.erase(right.separators.begin());
            WriteInternal(right_id, right.children, right.separators);
            WriteInternal(id, current.children, current.separators);
            return kInvalidPageId;
        }
    }
    if (index > 0) {
        const PageId left_id = parent.children[index - 1];
        auto left = ReadInternal(left_id);
        left.children.insert(left.children.end(), current.children.begin(), current.children.end());
        left.separators.push_back(parent.separators[index - 1]);
        left.separators.insert(left.separators.end(), current.separators.begin(), current.separators.end());
        WriteInternal(left_id, left.children, left.separators);
        parent.RemoveRightChild(index - 1);
        return id;
    }
    const PageId right_id = parent.children[1];
    auto right = ReadInternal(right_id);
    current.children.insert(current.children.end(), right.children.begin(), right.children.end());
    current.separators.push_back(parent.separators[0]);
    current.separators.insert(current.separators.end(), right.separators.begin(), right.separators.end());
    WriteInternal(id, current.children, current.separators);
    parent.RemoveRightChild(0);
    return right_id;
}

bool BPlusTree::Erase(int64_t key, RID rid) {
    CheckRID(rid);
    const BPlusKey entry{key, rid};
    std::vector<Ancestor> path;
    PageId current_id = Descend(entry, &path);
    auto leaf = ReadLeaf(current_id);
    const auto at = std::lower_bound(leaf.entries.begin(), leaf.entries.end(), entry);
    if (at == leaf.entries.end() || !(*at == entry)) return false;
    leaf.entries.erase(at);
    if (path.empty() || leaf.entries.size() >= capacity_.LeafMin()) {
        // Stable fences need no update when a right minimum disappears.
        WriteLeaf(current_id, leaf.entries, leaf.next);
        ++epoch_;
        return true;
    }

    std::vector<PageId> retired;
    InternalNode current;
    bool repairing_leaf = true;
    while (!path.empty()) {
        const Ancestor ancestor = path.back();
        path.pop_back();
        auto parent = ReadInternal(ancestor.page);
        assert(parent.children[ancestor.child_index] == current_id);
        const PageId removed = repairing_leaf
            ? RepairLeaf(current_id, leaf, parent, ancestor.child_index)
            : RepairInternal(current_id, current, parent, ancestor.child_index);
        if (removed != kInvalidPageId) retired.push_back(removed);
        if (ancestor.page == meta_.root && parent.children.size() == 1) {
            // Never serialize a one-child internal root. The child is already valid.
            SetRoot(parent.children.front(), static_cast<uint16_t>(meta_.height - 1));
            retired.push_back(ancestor.page);
            break;
        }
        if (ancestor.page == meta_.root || parent.children.size() >= capacity_.InternalMin()) {
            WriteInternal(ancestor.page, parent.children, parent.separators);
            break;
        }
        // Carry the underfull parent in bounded owned scratch, not on disk or in a guard.
        current_id = ancestor.page;
        current = std::move(parent);
        repairing_leaf = false;
    }
    // All structural links/metadata now omit these pages. Allocation remains append-only.
    for (PageId id : retired) pool_.Invalidate({FileKind::Database, id});
    ++epoch_;
    return true;
}

}  // namespace valencedb
