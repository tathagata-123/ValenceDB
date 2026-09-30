#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>

namespace valencedb {

using PageId = uint32_t;
constexpr PageId kInvalidPageId = std::numeric_limits<PageId>::max();
constexpr size_t kPageSize = 4096;
using PageBytes = std::array<std::byte, kPageSize>;

enum class FileKind : uint8_t { Database, Temporary };

struct PageRef {
    FileKind file;
    PageId page_id;
    bool operator==(const PageRef&) const = default;
};

struct PageRefHash {
    size_t operator()(PageRef page) const noexcept;
};

struct RID {
    PageId page_id;
    uint16_t slot_id;
    bool operator==(const RID&) const = default;
    bool operator<(const RID& other) const noexcept;
};

enum class PageType : uint16_t {
    Unknown = 0, Superblock = 1, Catalog = 2, TableMeta = 3,
    Heap = 4, IndexMeta = 5, BPlusLeaf = 6, BPlusInternal = 7,
    Temporary = 8
};

constexpr uint32_t kPageMagic = 0x50424456;  // "VDBP" on disk
constexpr uint16_t kPageFormatVersion = 1;
constexpr size_t kCommonPageHeaderSize = 16;
constexpr size_t kSerializedRIDSize = 8;

uint16_t ReadU16(std::span<const std::byte> bytes, size_t offset);
uint32_t ReadU32(std::span<const std::byte> bytes, size_t offset);
uint64_t ReadU64(std::span<const std::byte> bytes, size_t offset);
int64_t ReadI64(std::span<const std::byte> bytes, size_t offset);
double ReadDouble(std::span<const std::byte> bytes, size_t offset);
void WriteU16(std::span<std::byte> bytes, size_t offset, uint16_t value);
void WriteU32(std::span<std::byte> bytes, size_t offset, uint32_t value);
void WriteU64(std::span<std::byte> bytes, size_t offset, uint64_t value);
void WriteI64(std::span<std::byte> bytes, size_t offset, int64_t value);
void WriteDouble(std::span<std::byte> bytes, size_t offset, double value);

void WriteRID(std::span<std::byte> bytes, size_t offset, RID rid);
RID ReadRID(std::span<const std::byte> bytes, size_t offset);
void WritePageHeader(std::span<std::byte> bytes, PageType type, PageId page_id);
void ValidatePageHeader(std::span<const std::byte> bytes, PageType expected_type,
                        PageId expected_page_id);

}  // namespace valencedb
