#pragma once

#include "valencedb/storage/page_codec.hpp"

namespace valencedb {

struct BPlusKey {
    int64_t key;
    RID rid;
    bool operator==(const BPlusKey&) const = default;
    bool operator<(const BPlusKey& other) const noexcept;
};

constexpr uint16_t kBPlusLeafMax = 254;
constexpr uint16_t kBPlusInternalMax = 204;
constexpr uint16_t kBPlusMaxHeight = 32;  // At least two children per internal node.

// Logical test limits; the physical layout never changes. Supply again on Open.
struct BPlusCapacity {
    uint16_t leaf_max = kBPlusLeafMax;
    uint16_t internal_max = kBPlusInternalMax;
    void Validate() const;
    uint16_t LeafMin() const { return (leaf_max + 1) / 2; }
    uint16_t InternalMin() const { return (internal_max + 1) / 2; }
};

// Helpers interpret guarded bytes; callers own the guard and mark writes dirty.
class BPlusLeafPage {
public:
    static void Initialize(PageBytes& page, PageId id);
    static void Validate(const PageBytes& page, PageId id, uint16_t maximum = kBPlusLeafMax);
    static uint16_t Count(const PageBytes& page);
    static PageId Next(const PageBytes& page);
    static void SetNext(PageBytes& page, PageId next);
    static BPlusKey Entry(const PageBytes& page, size_t index);
    static void WriteEntry(PageBytes& page, size_t index, BPlusKey entry);
    static void Write(PageBytes& page, std::span<const BPlusKey> entries,
                      uint16_t maximum = kBPlusLeafMax);
};

class BPlusInternalPage {
public:
    // Initialize is followed by Write before publishing a reachable internal page.
    static void Initialize(PageBytes& page, PageId id);
    static void Validate(const PageBytes& page, PageId id, uint16_t maximum = kBPlusInternalMax);
    static uint16_t ChildCount(const PageBytes& page);
    static PageId Child(const PageBytes& page, size_t index);
    static BPlusKey Separator(const PageBytes& page, size_t index);
    static void Write(PageBytes& page, std::span<const PageId> children,
                      std::span<const BPlusKey> separators, uint16_t maximum = kBPlusInternalMax);
};

struct BPlusMeta {
    PageId root;
    uint16_t height;
    uint16_t key_column;
};

class BPlusMetaPage {
public:
    static BPlusMeta Read(const PageBytes& page, PageId id);
    static void Write(PageBytes& page, PageId id, BPlusMeta meta);
};

}  // namespace valencedb
