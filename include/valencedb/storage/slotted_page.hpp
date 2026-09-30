#pragma once

#include "valencedb/storage/page_codec.hpp"

#include <optional>
#include <span>
#include <vector>

namespace valencedb {

class SlottedPage {
public:
    static constexpr uint16_t kHeaderSize = 32;
    static constexpr uint16_t kSlotSize = 4;

    static void Initialize(PageBytes& page, PageId id, PageId owner);
    static void Validate(const PageBytes& page, PageId id, PageId owner);
    static bool CanFit(const PageBytes& page, size_t tuple_size);
    static std::optional<uint16_t> Insert(PageBytes& page, std::span<const std::byte> tuple);
    static std::optional<std::vector<std::byte>> Fetch(const PageBytes& page, uint16_t slot);
    static bool Erase(PageBytes& page, uint16_t slot);
    static void Compact(PageBytes& page);
    static PageId Next(const PageBytes& page);
    static void SetNext(PageBytes& page, PageId next);
    static uint16_t SlotCount(const PageBytes& page);
    static uint16_t LiveCount(const PageBytes& page);
};

}  // namespace valencedb
