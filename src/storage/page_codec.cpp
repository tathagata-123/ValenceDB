#include "valencedb/storage/page_codec.hpp"
#include "valencedb/common/error.hpp"

#include <bit>
#include <climits>
#include <limits>

namespace valencedb {
namespace {

void CheckBounds(size_t length, size_t offset, size_t width) {
    if (offset > length || width > length - offset) {
        throw DbError("byte field exceeds buffer bounds");
    }
}

bool KnownPageType(uint16_t value) {
    return value <= static_cast<uint16_t>(PageType::Temporary);
}

}  // namespace

static_assert(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559,
              "ValenceDB requires IEEE-754 binary64 double");
static_assert(CHAR_BIT == 8);

size_t PageRefHash::operator()(PageRef page) const noexcept {
    return (static_cast<size_t>(page.page_id) << 1) ^
           static_cast<size_t>(page.file);
}

bool RID::operator<(const RID& other) const noexcept {
    return page_id < other.page_id ||
           (page_id == other.page_id && slot_id < other.slot_id);
}

uint16_t ReadU16(std::span<const std::byte> bytes, size_t offset) {
    CheckBounds(bytes.size(), offset, 2);
    return uint16_t(std::to_integer<uint8_t>(bytes[offset])) |
           (uint16_t(std::to_integer<uint8_t>(bytes[offset + 1])) << 8);
}

uint32_t ReadU32(std::span<const std::byte> bytes, size_t offset) {
    CheckBounds(bytes.size(), offset, 4);
    uint32_t result = 0;
    for (size_t i = 0; i < 4; ++i) {
        result |= uint32_t(std::to_integer<uint8_t>(bytes[offset + i])) << (8 * i);
    }
    return result;
}

uint64_t ReadU64(std::span<const std::byte> bytes, size_t offset) {
    CheckBounds(bytes.size(), offset, 8);
    uint64_t result = 0;
    for (size_t i = 0; i < 8; ++i) {
        result |= uint64_t(std::to_integer<uint8_t>(bytes[offset + i])) << (8 * i);
    }
    return result;
}

int64_t ReadI64(std::span<const std::byte> bytes, size_t offset) {
    return std::bit_cast<int64_t>(ReadU64(bytes, offset));
}

double ReadDouble(std::span<const std::byte> bytes, size_t offset) {
    return std::bit_cast<double>(ReadU64(bytes, offset));
}

void WriteU16(std::span<std::byte> bytes, size_t offset, uint16_t value) {
    CheckBounds(bytes.size(), offset, 2);
    for (size_t i = 0; i < 2; ++i) {
        bytes[offset + i] = std::byte((value >> (8 * i)) & 0xff);
    }
}

void WriteU32(std::span<std::byte> bytes, size_t offset, uint32_t value) {
    CheckBounds(bytes.size(), offset, 4);
    for (size_t i = 0; i < 4; ++i) {
        bytes[offset + i] = std::byte((value >> (8 * i)) & 0xff);
    }
}

void WriteU64(std::span<std::byte> bytes, size_t offset, uint64_t value) {
    CheckBounds(bytes.size(), offset, 8);
    for (size_t i = 0; i < 8; ++i) {
        bytes[offset + i] = std::byte((value >> (8 * i)) & 0xff);
    }
}

void WriteI64(std::span<std::byte> bytes, size_t offset, int64_t value) {
    WriteU64(bytes, offset, std::bit_cast<uint64_t>(value));
}

void WriteDouble(std::span<std::byte> bytes, size_t offset, double value) {
    WriteU64(bytes, offset, std::bit_cast<uint64_t>(value));
}

void WriteRID(std::span<std::byte> bytes, size_t offset, RID rid) {
    CheckBounds(bytes.size(), offset, kSerializedRIDSize);
    WriteU32(bytes, offset, rid.page_id);
    WriteU16(bytes, offset + 4, rid.slot_id);
    WriteU16(bytes, offset + 6, 0);
}

RID ReadRID(std::span<const std::byte> bytes, size_t offset) {
    CheckBounds(bytes.size(), offset, kSerializedRIDSize);
    return {ReadU32(bytes, offset), ReadU16(bytes, offset + 4)};
}

void WritePageHeader(std::span<std::byte> bytes, PageType type, PageId page_id) {
    CheckBounds(bytes.size(), 0, kCommonPageHeaderSize);
    if (!KnownPageType(static_cast<uint16_t>(type)) || type == PageType::Unknown ||
        page_id == kInvalidPageId) throw DbError("invalid page header identity");
    WriteU32(bytes, 0, kPageMagic);
    WriteU16(bytes, 4, static_cast<uint16_t>(type));
    WriteU16(bytes, 6, kPageFormatVersion);
    WriteU32(bytes, 8, page_id);
    WriteU32(bytes, 12, 0);
}

void ValidatePageHeader(std::span<const std::byte> bytes, PageType expected_type,
                        PageId expected_page_id) {
    CheckBounds(bytes.size(), 0, kCommonPageHeaderSize);
    if (ReadU32(bytes, 0) != kPageMagic) throw DbError("page magic mismatch");
    const auto type = ReadU16(bytes, 4);
    if (!KnownPageType(type) || type == 0) throw DbError("invalid page type");
    if (ReadU16(bytes, 6) != kPageFormatVersion) throw DbError("page format mismatch");
    if (ReadU32(bytes, 8) == kInvalidPageId) throw DbError("invalid page ID in header");
    if (expected_type != PageType::Unknown && type != static_cast<uint16_t>(expected_type))
        throw DbError("page type mismatch");
    if (expected_page_id != kInvalidPageId && ReadU32(bytes, 8) != expected_page_id)
        throw DbError("page ID mismatch");
}

}  // namespace valencedb
