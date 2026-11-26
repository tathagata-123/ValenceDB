#include "valencedb/index/bplus_page.hpp"
#include "valencedb/common/error.hpp"

namespace valencedb {
namespace {
constexpr size_t kHeader = 32;
constexpr size_t kLeafEntry = 16;
constexpr size_t kInternalEntry = 20;
static_assert((kPageSize - kHeader) / kLeafEntry == kBPlusLeafMax);
static_assert((kPageSize - kHeader) / kInternalEntry + 1 == kBPlusInternalMax);

void CheckKey(BPlusKey entry) {
    if (entry.rid.page_id == kInvalidPageId) throw DbError("invalid B+ tree RID");
}

BPlusKey ReadKey(const PageBytes& page, size_t at) {
    BPlusKey entry{ReadI64(page, at), ReadRID(page, at + 8)};
    CheckKey(entry);
    return entry;
}

void WriteKey(PageBytes& page, size_t at, BPlusKey entry) {
    CheckKey(entry);
    WriteI64(page, at, entry.key);
    WriteRID(page, at + 8, entry.rid);
}

void CheckReserved(const PageBytes& page) {
    if (ReadU16(page, 18) != 0 || ReadU64(page, 24) != 0)
        throw DbError("nonzero B+ tree reserved header bytes");
}

void CheckMeta(PageId id, BPlusMeta meta) {
    if (id == kInvalidPageId || meta.root == kInvalidPageId || meta.root == id ||
        meta.height == 0 || meta.height > kBPlusMaxHeight ||
        (meta.key_column != UINT16_MAX && meta.key_column >= 8))
        throw DbError("invalid B+ tree metadata");
}
}  // namespace

bool BPlusKey::operator<(const BPlusKey& other) const noexcept {
    if (key != other.key) return key < other.key;
    if (rid.page_id != other.rid.page_id) return rid.page_id < other.rid.page_id;
    return rid.slot_id < other.rid.slot_id;
}

void BPlusCapacity::Validate() const {
    if (leaf_max < 2 || leaf_max > kBPlusLeafMax ||
        internal_max < 3 || internal_max > kBPlusInternalMax)
        throw DbError("invalid B+ tree capacities");
}

void BPlusLeafPage::Initialize(PageBytes& page, PageId id) {
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::BPlusLeaf, id);
    WriteU32(page, 20, kInvalidPageId);
}

uint16_t BPlusLeafPage::Count(const PageBytes& page) {
    const auto count = ReadU16(page, 16);
    if (count > kBPlusLeafMax) throw DbError("B+ leaf count exceeds physical capacity");
    return count;
}

PageId BPlusLeafPage::Next(const PageBytes& page) { return ReadU32(page, 20); }

void BPlusLeafPage::SetNext(PageBytes& page, PageId next) {
    if (next == ReadU32(page, 8)) throw DbError("B+ leaf cannot link to itself");
    WriteU32(page, 20, next);
}

BPlusKey BPlusLeafPage::Entry(const PageBytes& page, size_t index) {
    if (index >= Count(page)) throw DbError("B+ leaf entry out of bounds");
    return ReadKey(page, kHeader + index * kLeafEntry);
}

void BPlusLeafPage::WriteEntry(PageBytes& page, size_t index, BPlusKey entry) {
    if (index >= Count(page)) throw DbError("B+ leaf entry out of bounds");
    WriteKey(page, kHeader + index * kLeafEntry, entry);
}

void BPlusLeafPage::Write(PageBytes& page, std::span<const BPlusKey> entries, uint16_t maximum) {
    if (maximum < 2 || maximum > kBPlusLeafMax || entries.size() > maximum)
        throw DbError("cannot persist overflowing B+ leaf");
    WriteU16(page, 16, static_cast<uint16_t>(entries.size()));
    for (size_t i = 0; i < entries.size(); ++i) WriteEntry(page, i, entries[i]);
}

void BPlusLeafPage::Validate(const PageBytes& page, PageId id, uint16_t maximum) {
    ValidatePageHeader(page, PageType::BPlusLeaf, id);
    CheckReserved(page);
    const auto count = Count(page);
    if (maximum < 2 || maximum > kBPlusLeafMax || count > maximum || Next(page) == id)
        throw DbError("invalid B+ leaf count or link");
}

void BPlusInternalPage::Initialize(PageBytes& page, PageId id) {
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::BPlusInternal, id);
    WriteU32(page, 20, kInvalidPageId);
}

uint16_t BPlusInternalPage::ChildCount(const PageBytes& page) {
    const auto count = ReadU16(page, 16);
    if (count < 2 || count > kBPlusInternalMax) throw DbError("invalid B+ internal child count");
    return count;
}

PageId BPlusInternalPage::Child(const PageBytes& page, size_t index) {
    if (index >= ChildCount(page)) throw DbError("B+ internal child out of bounds");
    const PageId id = ReadU32(page, index == 0 ? 20 : kHeader + (index - 1) * kInternalEntry + 16);
    if (id == kInvalidPageId || id == ReadU32(page, 8)) throw DbError("invalid B+ child ID");
    return id;
}

BPlusKey BPlusInternalPage::Separator(const PageBytes& page, size_t index) {
    if (index >= size_t(ChildCount(page) - 1)) throw DbError("B+ separator out of bounds");
    return ReadKey(page, kHeader + index * kInternalEntry);
}

void BPlusInternalPage::Write(PageBytes& page, std::span<const PageId> children,
                              std::span<const BPlusKey> separators, uint16_t maximum) {
    if (maximum < 3 || maximum > kBPlusInternalMax || children.size() < 2 ||
        children.size() > maximum || separators.size() != children.size() - 1)
        throw DbError("cannot persist invalid or overflowing B+ internal node");
    for (PageId child : children)
        if (child == kInvalidPageId || child == ReadU32(page, 8)) throw DbError("invalid B+ child ID");
    WriteU16(page, 16, static_cast<uint16_t>(children.size()));
    WriteU32(page, 20, children[0]);
    for (size_t i = 0; i < separators.size(); ++i) {
        WriteKey(page, kHeader + i * kInternalEntry, separators[i]);
        WriteU32(page, kHeader + i * kInternalEntry + 16, children[i + 1]);
    }
}

void BPlusInternalPage::Validate(const PageBytes& page, PageId id, uint16_t maximum) {
    ValidatePageHeader(page, PageType::BPlusInternal, id);
    CheckReserved(page);
    const auto count = ChildCount(page);
    if (maximum < 3 || maximum > kBPlusInternalMax || count > maximum)
        throw DbError("B+ internal count exceeds configured capacity");
}

BPlusMeta BPlusMetaPage::Read(const PageBytes& page, PageId id) {
    ValidatePageHeader(page, PageType::IndexMeta, id);
    BPlusMeta meta{ReadU32(page, 16), ReadU16(page, 20), ReadU16(page, 22)};
    CheckMeta(id, meta);
    return meta;
}

void BPlusMetaPage::Write(PageBytes& page, PageId id, BPlusMeta meta) {
    CheckMeta(id, meta);
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::IndexMeta, id);
    WriteU32(page, 16, meta.root);
    WriteU16(page, 20, meta.height);
    WriteU16(page, 22, meta.key_column);
}
}  // namespace valencedb
