#include "valencedb/index/bplus_tree.hpp"
#include "valencedb/common/error.hpp"

#include <array>
#include <algorithm>

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
        if (path) path->push_back({page, low});
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
    const uint16_t at = LeafLowerBound(guard.Data(), entry);
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
    ValidatePageHeader(guard.Data(), PageType::IndexMeta, meta_page_);
    const BPlusMeta updated{root, height, meta_.key_column};
    BPlusMetaPage::Write(guard.MutableData(), meta_page_, updated);
    guard.MarkDirty();
    meta_ = updated;
}

PageId BPlusTree::AllocatePage() {
    auto guard = pool_.NewPage(FileKind::Database);
    return guard.Page().page_id; // Release the allocation pin before any other fetch.
}

BPlusTree::SplitResult BPlusTree::SplitLeaf(PageId id, const LeafNode& node) {
    // Copy the first right key UP; it also remains in the right leaf.
    const size_t middle = node.entries.size() / 2;
    const PageId right = AllocatePage();
    const std::span<const BPlusKey> entries(node.entries);
    WriteLeaf(right, entries.subspan(middle), node.next);
    WriteLeaf(id, entries.first(middle), right);
    return {entries[middle], right};
}

BPlusTree::SplitResult BPlusTree::SplitInternal(PageId id, const InternalNode& node) {
    // Move the boundary separator UP; omit it from both internal pages.
    const size_t middle = node.children.size() / 2;
    const PageId right = AllocatePage();
    const std::span<const PageId> children(node.children);
    const std::span<const BPlusKey> separators(node.separators);
    WriteInternal(right, children.subspan(middle), separators.subspan(middle));
    WriteInternal(id, children.first(middle), separators.first(middle - 1));
    return {separators[middle - 1], right};
}

bool BPlusTree::Insert(int64_t key, RID rid) {
    CheckRID(rid);
    const BPlusKey entry{key, rid};
    std::vector<Ancestor> path;
    const PageId leaf_id = Descend(entry, &path);
    auto leaf = ReadLeaf(leaf_id);
    const auto at = std::lower_bound(leaf.entries.begin(), leaf.entries.end(), entry);
    if (at != leaf.entries.end() && *at == entry) return false;
    leaf.entries.insert(at, entry);
    if (leaf.entries.size() <= capacity_.leaf_max) {
        WriteLeaf(leaf_id, leaf.entries, leaf.next);
        ++epoch_;
        return true;
    }

    auto split = SplitLeaf(leaf_id, leaf);
    // Each saved child index identifies where the separator/right child belongs.
    while (!path.empty()) {
        const auto ancestor = path.back();
        path.pop_back();
        auto parent = ReadInternal(ancestor.page);
        parent.children.insert(parent.children.begin() + ancestor.child_index + 1, split.right);
        parent.separators.insert(parent.separators.begin() + ancestor.child_index, split.separator);
        if (parent.children.size() <= capacity_.internal_max) {
            WriteInternal(ancestor.page, parent.children, parent.separators);
            ++epoch_;
            return true;
        }
        split = SplitInternal(ancestor.page, parent);
    }

    // The old root is the left half of the final split.
    if (meta_.height == kBPlusMaxHeight) throw DbError("B+ tree height limit");
    const PageId root = AllocatePage();
    WriteInternal(root, std::array<PageId, 2>{meta_.root, split.right},
                  std::array<BPlusKey, 1>{split.separator});
    SetRoot(root, static_cast<uint16_t>(meta_.height + 1));
    ++epoch_;
    return true;
}

BPlusTree::LeafNode BPlusTree::ReadLeaf(PageId id) const {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusLeafPage::Validate(guard.Data(), id, capacity_.leaf_max);
    LeafNode node{{}, BPlusLeafPage::Next(guard.Data())};
    const auto count = BPlusLeafPage::Count(guard.Data());
    node.entries.reserve(count + 1);
    for (size_t i = 0; i < count; ++i)
        node.entries.push_back(BPlusLeafPage::Entry(guard.Data(), i));
    return node;
}

BPlusTree::InternalNode BPlusTree::ReadInternal(PageId id) const {
    auto guard = pool_.Fetch({FileKind::Database, id});
    BPlusInternalPage::Validate(guard.Data(), id, capacity_.internal_max);
    InternalNode node;
    const auto count = BPlusInternalPage::ChildCount(guard.Data());
    // Leave room for one inserted child without growing the vectors.
    node.children.reserve(count + 1);
    node.separators.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        node.children.push_back(BPlusInternalPage::Child(guard.Data(), i));
        if (i != 0) node.separators.push_back(BPlusInternalPage::Separator(guard.Data(), i - 1));
    }
    return node;
}

}  // namespace valencedb
