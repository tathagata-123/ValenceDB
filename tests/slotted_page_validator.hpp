#pragma once

#include "valencedb/common/error.hpp"
#include "valencedb/storage/slotted_page.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace valencedb::test {

// Exhaustive audit for tests, separate from the constant-time access checks.
inline void ValidateSlottedPage(const PageBytes& page, PageId id, PageId owner) {
    ValidatePageHeader(page, PageType::Heap, id);
    if (ReadU32(page, 28) != owner) throw DbError("heap page owner mismatch");
    const size_t count = ReadU16(page, 16);
    const size_t lower = ReadU16(page, 18);
    const size_t upper = ReadU16(page, 20);
    const size_t live = ReadU16(page, 22);
    if (count > (kPageSize - SlottedPage::kHeaderSize) / SlottedPage::kSlotSize ||
        lower != SlottedPage::kHeaderSize + count * SlottedPage::kSlotSize ||
        lower > upper || upper > kPageSize || live > count)
        throw DbError("invalid heap page bounds");
    std::vector<std::pair<size_t, size_t>> ranges;
    for (size_t i = 0; i < count; ++i) {
        const size_t at = SlottedPage::kHeaderSize + i * SlottedPage::kSlotSize;
        const size_t offset = ReadU16(page, at);
        const size_t length = ReadU16(page, at + 2);
        if (length == 0) {
            if (offset > kPageSize) throw DbError("invalid tombstone offset");
            continue;
        }
        if (offset < upper || offset > kPageSize || length > kPageSize - offset)
            throw DbError("invalid heap slot bounds");
        ranges.emplace_back(offset, offset + length);
    }
    if (ranges.size() != live) throw DbError("heap live count mismatch");
    std::sort(ranges.begin(), ranges.end());
    for (size_t i = 1; i < ranges.size(); ++i)
        if (ranges[i].first < ranges[i - 1].second)
            throw DbError("overlapping heap tuple payloads");
}

}  // namespace valencedb::test
