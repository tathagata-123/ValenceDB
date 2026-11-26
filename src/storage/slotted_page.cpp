#include "valencedb/storage/slotted_page.hpp"
#include "valencedb/common/error.hpp"

#include <cstring>

namespace valencedb {
namespace {
size_t SlotOffset(uint16_t slot) { return SlottedPage::kHeaderSize + size_t(slot) * 4; }

void CheckBounds(const PageBytes& page) {
    const size_t count = ReadU16(page, 16);
    const size_t lower = ReadU16(page, 18);
    const size_t upper = ReadU16(page, 20);
    if (count > (kPageSize - SlottedPage::kHeaderSize) / SlottedPage::kSlotSize ||
        lower != SlottedPage::kHeaderSize + count * SlottedPage::kSlotSize ||
        lower > upper || upper > kPageSize || ReadU16(page, 22) > count)
        throw DbError("invalid heap page bounds");
}

void CheckPayload(const PageBytes& page, size_t offset, size_t length) {
    if (offset < ReadU16(page, 20) || offset > kPageSize || length > kPageSize - offset)
        throw DbError("invalid heap slot bounds");
}
}

void SlottedPage::Initialize(PageBytes& page, PageId id, PageId owner) {
    page.fill(std::byte{0});
    WritePageHeader(page, PageType::Heap, id);
    WriteU16(page, 16, 0);
    WriteU16(page, 18, kHeaderSize);
    WriteU16(page, 20, kPageSize);
    WriteU16(page, 22, 0);
    WriteU32(page, 24, kInvalidPageId);
    WriteU32(page, 28, owner);
}

void SlottedPage::Validate(const PageBytes& page, PageId id, PageId owner) {
    ValidatePageHeader(page, PageType::Heap, id);
    if (ReadU32(page, 28) != owner) throw DbError("heap page owner mismatch");
    CheckBounds(page);
}

bool SlottedPage::CanFit(const PageBytes& page, size_t tuple_size) {
    CheckBounds(page);
    const size_t lower = ReadU16(page, 18);
    const size_t upper = ReadU16(page, 20);
    return tuple_size != 0 && tuple_size <= kPageSize &&
           lower + kSlotSize <= upper && tuple_size <= upper - lower - kSlotSize;
}

std::optional<uint16_t> SlottedPage::Insert(PageBytes& page, std::span<const std::byte> tuple) {
    if (tuple.empty() || tuple.size() > kPageSize) return std::nullopt;
    if (!CanFit(page, tuple.size())) {
        size_t live_bytes = 0;
        for (uint16_t i = 0; i < SlotCount(page); ++i)
            live_bytes += ReadU16(page, SlotOffset(i) + 2);
        if (size_t(ReadU16(page, 18)) + kSlotSize + live_bytes + tuple.size() > kPageSize)
            return std::nullopt;
        Compact(page);
    }
    const uint16_t slot = SlotCount(page);
    const uint16_t upper = static_cast<uint16_t>(ReadU16(page, 20) - tuple.size());
    std::memcpy(page.data() + upper, tuple.data(), tuple.size());
    WriteU16(page, SlotOffset(slot), upper);
    WriteU16(page, SlotOffset(slot) + 2, static_cast<uint16_t>(tuple.size()));
    WriteU16(page, 16, slot + 1);
    WriteU16(page, 18, static_cast<uint16_t>(SlotOffset(slot + 1)));
    WriteU16(page, 20, upper);
    WriteU16(page, 22, LiveCount(page) + 1);
    return slot;
}

std::optional<std::vector<std::byte>> SlottedPage::Fetch(const PageBytes& page, uint16_t slot) {
    CheckBounds(page);
    if (slot >= SlotCount(page)) return std::nullopt;
    const size_t offset = ReadU16(page, SlotOffset(slot));
    const size_t length = ReadU16(page, SlotOffset(slot) + 2);
    if (length == 0) return std::nullopt;
    CheckPayload(page, offset, length);
    return std::vector<std::byte>(page.begin() + offset, page.begin() + offset + length);
}

bool SlottedPage::Erase(PageBytes& page, uint16_t slot) {
    CheckBounds(page);
    if (slot >= SlotCount(page) || ReadU16(page, SlotOffset(slot) + 2) == 0) return false;
    if (LiveCount(page) == 0) throw DbError("heap live count underflow");
    WriteU16(page, SlotOffset(slot) + 2, 0);
    WriteU16(page, 22, LiveCount(page) - 1);
    return true;
}

void SlottedPage::Compact(PageBytes& page) {
    CheckBounds(page);
    PageBytes scratch{};
    uint16_t upper = kPageSize;
    const uint16_t lower = ReadU16(page, 18);
    for (uint16_t i = 0; i < SlotCount(page); ++i) {
        const size_t slot = SlotOffset(i);
        const uint16_t length = ReadU16(page, slot + 2);
        if (length == 0) continue;
        const uint16_t offset = ReadU16(page, slot);
        CheckPayload(page, offset, length);
        if (length > upper - lower) throw DbError("heap payloads exceed page capacity");
        upper -= length;
        std::memcpy(scratch.data() + upper, page.data() + offset, length);
        WriteU16(page, slot, upper);
    }
    std::memcpy(page.data() + upper, scratch.data() + upper, kPageSize - upper);
    WriteU16(page, 20, upper);
}

PageId SlottedPage::Next(const PageBytes& page) { return ReadU32(page, 24); }
void SlottedPage::SetNext(PageBytes& page, PageId next) { WriteU32(page, 24, next); }
uint16_t SlottedPage::SlotCount(const PageBytes& page) { return ReadU16(page, 16); }
uint16_t SlottedPage::LiveCount(const PageBytes& page) { return ReadU16(page, 22); }

}  // namespace valencedb
