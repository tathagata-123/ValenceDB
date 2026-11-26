#include "bplus_validator.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/database.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <random>

using namespace valencedb;
using namespace bplus_test;

namespace {
constexpr int64_t kMin = INT64_MIN;
constexpr int64_t kMax = INT64_MAX;
uint16_t maximum_height = 0;
uint64_t maximum_pins = 0;
size_t cases_passed = 0;

struct Files {
    std::filesystem::path directory, db, temp;
    explicit Files(const std::string& label) {
        static unsigned number = 0;
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        directory = std::filesystem::temp_directory_path() /
            ("valencedb_bplus_insert_" + label + "_" + std::to_string(stamp) + "_" + std::to_string(++number));
        std::filesystem::create_directory(directory);
        db = directory / "pages.db";
        temp = directory / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(directory); }
};

void Throws(const std::function<void()>& action) {
    bool thrown = false;
    try { action(); } catch (const DbError&) { thrown = true; }
    VDB_CHECK(thrown);
}

void NoPins(const Database& db) {
    VDB_CHECK(db.Metrics().active_pins == 0);
    maximum_pins = std::max(maximum_pins, db.Metrics().peak_pins);
    VDB_CHECK(db.Metrics().peak_pins <= 1);
}

std::vector<BPlusKey> Drain(BPlusIterator cursor, Database& db) {
    NoPins(db);
    std::vector<BPlusKey> result;
    while (auto entry = cursor.Next()) {
        NoPins(db);
        result.push_back(*entry);
    }
    VDB_CHECK(!cursor.Next());
    NoPins(db);
    return result;
}

std::vector<BPlusKey> Expected(const Oracle& oracle, int64_t low = kMin, int64_t high = kMax) {
    std::vector<BPlusKey> result;
    for (auto entry : oracle) if (low <= entry.key && entry.key <= high) result.push_back(entry);
    return result;
}

TreeShape CheckTree(Database& db, const BPlusTree& tree, const Oracle& oracle) {
    const auto shape = Validator(db.Buffer(), db.Disk().PageCount(FileKind::Database), tree).Run();
    maximum_height = std::max(maximum_height, shape.height);
    VDB_CHECK(shape.entries == Expected(oracle));
    VDB_CHECK(Drain(tree.LowerBound(kMin), db) == shape.entries);
    for (auto entry : oracle) {
        VDB_CHECK(tree.Contains(entry.key, entry.rid));
        NoPins(db);
    }
    NoPins(db);
    return shape;
}

void Insert(Database& db, BPlusTree& tree, Oracle& oracle, BPlusKey entry) {
    const auto epoch = tree.MutationEpoch();
    const bool expected = oracle.insert(entry).second;
    VDB_CHECK(tree.Insert(entry.key, entry.rid) == expected);
    VDB_CHECK(tree.MutationEpoch() == epoch + (expected ? 1 : 0));
    NoPins(db);
}

PageBytes Copy(Database& db, PageId id) {
    auto guard = db.Buffer().Fetch({FileKind::Database, id});
    return guard.Data();
}

void Replace(Database& db, PageId id, const PageBytes& bytes) {
    auto guard = db.Buffer().Fetch({FileKind::Database, id});
    guard.MutableData() = bytes;
    guard.MarkDirty();
}

void PageFormats() {
    VDB_CHECK((BPlusKey{10, {3, 1}} < BPlusKey{10, {3, 2}}));
    VDB_CHECK((BPlusKey{10, {3, 2}} < BPlusKey{10, {4, 0}}));
    VDB_CHECK((BPlusKey{10, {4, 0}} < BPlusKey{20, {1, 0}}));
    VDB_CHECK((BPlusKey{-1, {256, 1}} < BPlusKey{0, {1, 0}}));
    VDB_CHECK((BPlusKey{10, {255, 65535}} < BPlusKey{10, {256, 0}}));
    VDB_CHECK(BPlusCapacity{}.LeafMin() == 127 && BPlusCapacity{}.InternalMin() == 102);
    for (auto cap : {BPlusCapacity{1, 4}, {255, 4}, {4, 2}, {4, 205}})
        Throws([&] { cap.Validate(); });

    PageBytes leaf{};
    BPlusLeafPage::Initialize(leaf, 7);
    VDB_CHECK(ReadU16(leaf, 16) == 0 && ReadU32(leaf, 20) == kInvalidPageId);
    std::vector<BPlusKey> entries;
    for (int64_t i = 0; i < 254; ++i) entries.push_back({i - 100, {PageId(i + 256), uint16_t(i)}});
    BPlusLeafPage::Write(leaf, entries);
    BPlusLeafPage::SetNext(leaf, 9);
    BPlusLeafPage::Validate(leaf, 7);
    VDB_CHECK(ReadI64(leaf, 32) == -100 && ReadU32(leaf, 40) == 256);
    VDB_CHECK(RawKey(leaf, 32 + 253 * 16) == entries.back());
    VDB_CHECK(BPlusLeafPage::Entry(leaf, 253) == entries.back());
    const auto full_leaf = leaf;
    entries.push_back({154, {999, 0}});
    Throws([&] { BPlusLeafPage::Write(leaf, entries); });
    VDB_CHECK(leaf == full_leaf);
    Throws([&] { BPlusLeafPage::Validate(leaf, 7, 4); });
    Throws([&] { BPlusLeafPage::Entry(leaf, 254); });
    Throws([&] { BPlusLeafPage::WriteEntry(leaf, 254, entries[0]); });
    Throws([&] { BPlusLeafPage::SetNext(leaf, 7); });
    for (size_t offset : {size_t(0), size_t(4), size_t(6), size_t(8)}) {
        auto corrupt = full_leaf;
        corrupt[offset] ^= std::byte{1};
        Throws([&] { BPlusLeafPage::Validate(corrupt, 7); });
    }
    auto corrupt = full_leaf;
    WriteU16(corrupt, 16, 255);
    Throws([&] { BPlusLeafPage::Validate(corrupt, 7); });
    corrupt = full_leaf;
    WriteU32(corrupt, 40, kInvalidPageId);
    Throws([&] { BPlusLeafPage::Entry(corrupt, 0); });
    corrupt = full_leaf;
    corrupt[46] = std::byte{1}; // Unused RID/header padding is ignored by production readers.
    corrupt[12] = corrupt[18] = corrupt[24] = std::byte{1};
    BPlusLeafPage::Validate(corrupt, 7);
    VDB_CHECK(BPlusLeafPage::Entry(corrupt, 0) == entries[0]);

    PageBytes internal{};
    BPlusInternalPage::Initialize(internal, 300);
    std::vector<PageId> children;
    std::vector<BPlusKey> fences;
    for (PageId i = 0; i < 204; ++i) {
        children.push_back(i);
        if (i != 0) fences.push_back({i, {i, 0}});
    }
    BPlusInternalPage::Write(internal, children, fences);
    BPlusInternalPage::Validate(internal, 300);
    VDB_CHECK(ReadU16(internal, 16) == 204 && ReadU32(internal, 20) == 0);
    VDB_CHECK(ReadU32(internal, 48 + 202 * 20) == 203);
    VDB_CHECK(RawKey(internal, 32 + 202 * 20) == fences.back());
    const auto full_internal = internal;
    children.push_back(204);
    fences.push_back({204, {204, 0}});
    Throws([&] { BPlusInternalPage::Write(internal, children, fences); });
    VDB_CHECK(internal == full_internal);
    Throws([&] { BPlusInternalPage::Validate(internal, 300, 4); });
    for (uint16_t count : {uint16_t(0), uint16_t(1), uint16_t(205), uint16_t(UINT16_MAX)}) {
        corrupt = full_internal;
        WriteU16(corrupt, 16, count);
        Throws([&] { BPlusInternalPage::Validate(corrupt, 300); });
    }
    corrupt = full_internal;
    WriteU32(corrupt, 20, kInvalidPageId);
    Throws([&] { BPlusInternalPage::Child(corrupt, 0); });
    Throws([&] { BPlusInternalPage::Child(full_internal, 204); });
    Throws([&] { BPlusInternalPage::Separator(full_internal, 203); });
    children.resize(4);
    fences.resize(2);
    Throws([&] { BPlusInternalPage::Write(internal, children, fences); });

    PageBytes meta{};
    BPlusMetaPage::Write(meta, 2, {7, 1, 3});
    VDB_CHECK(ReadU32(meta, 16) == 7 && ReadU16(meta, 20) == 1 && ReadU16(meta, 22) == 3);
    VDB_CHECK(BPlusMetaPage::Read(meta, 2).root == 7);
    for (BPlusMeta bad : {BPlusMeta{kInvalidPageId, 1, 0}, {2, 1, 0}, {7, 0, 0}, {7, 33, 0}, {7, 1, 8}})
        Throws([&] { BPlusMetaPage::Write(meta, 2, bad); });
}

void EmptyAndSingleReopen() {
    Files files("empty_single");
    PageId meta, root;
    Oracle oracle;
    {
        Database db(files.db, OpenMode::CreateNew, files.temp, 1);
        auto tree = BPlusTree::Create(db.Buffer(), 3);
        meta = tree.MetaPage();
        root = tree.RootPage();
        VDB_CHECK(db.Disk().PageCount(FileKind::Database) == 4);
        VDB_CHECK(tree.Height() == 1 && tree.KeyColumn() == 3);
        const auto page = Copy(db, root);
        VDB_CHECK(ReadU16(page, 4) == uint16_t(PageType::BPlusLeaf));
        VDB_CHECK(ReadU16(page, 16) == 0 && ReadU32(page, 20) == kInvalidPageId);
        VDB_CHECK(!tree.Contains(1, {0, 0}));
        VDB_CHECK(Drain(tree.LowerBound(kMin), db).empty());
        VDB_CHECK(Drain(tree.EqualRange(1), db).empty());
        VDB_CHECK(Drain(tree.Range(kMin, kMax), db).empty());
        VDB_CHECK(Drain(tree.Range(2, 1), db).empty());
        CheckTree(db, tree, oracle);
        db.Close();
    }
    {
        Database db(files.db, OpenMode::OpenExisting, files.temp, 1);
        auto tree = BPlusTree::Open(db.Buffer(), meta);
        VDB_CHECK(tree.Height() == 1 && tree.RootPage() == root && tree.KeyColumn() == 3);
        CheckTree(db, tree, oracle);
        auto stale_empty = tree.LowerBound(1);
        Insert(db, tree, oracle, {42, {0, 0}});
        Throws([&] { stale_empty.Next(); });
        VDB_CHECK(tree.Height() == 1 && tree.RootPage() == root);
        auto cursor = tree.LowerBound(42);
        const auto before = Copy(db, root);
        const auto before_meta = Copy(db, meta);
        const auto allocations = db.Disk().PageCount(FileKind::Database);
        Insert(db, tree, oracle, {42, {0, 0}});
        VDB_CHECK(Copy(db, root) == before && Copy(db, meta) == before_meta);
        VDB_CHECK(db.Disk().PageCount(FileKind::Database) == allocations);
        VDB_CHECK(Drain(cursor, db) == Expected(oracle));
        VDB_CHECK(Drain(tree.EqualRange(42), db) == Expected(oracle));
        VDB_CHECK(Drain(tree.Range(42, 42), db) == Expected(oracle));
        VDB_CHECK(Drain(tree.LowerBound(43), db).empty());
        Throws([&] { tree.Insert(1, {kInvalidPageId, 0}); });
        Throws([&] { tree.Contains(1, {kInvalidPageId, 0}); });
        CheckTree(db, tree, oracle);
        db.Close();
    }
    {
        Database db(files.db, OpenMode::OpenExisting, files.temp, 1);
        auto tree = BPlusTree::Open(db.Buffer(), meta);
        VDB_CHECK(tree.RootPage() == root && tree.Height() == 1 && tree.MutationEpoch() == 0);
        CheckTree(db, tree, oracle);
        db.Close();
    }
}

void LeafAndInternalSplits() {
    Files files("worked_leaf");
    Database db(files.db, OpenMode::CreateNew, files.temp, 1);
    auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
    Oracle oracle;
    const PageId original = tree.RootPage();
    for (int64_t key : {10, 20, 30, 40}) Insert(db, tree, oracle, {key, {0, 0}});
    VDB_CHECK(tree.Height() == 1);
    Insert(db, tree, oracle, {25, {0, 0}});
    const auto shape = CheckTree(db, tree, oracle);
    VDB_CHECK(tree.Height() == 2 && shape.leaves.size() == 2 && shape.leaves[0] == original);
    const auto left = Copy(db, original);
    const auto right = Copy(db, shape.leaves[1]);
    const auto root = Copy(db, tree.RootPage());
    VDB_CHECK(ReadU16(left, 16) == 2 && ReadU16(right, 16) == 3);
    VDB_CHECK(ReadU32(left, 20) == shape.leaves[1] && ReadU32(right, 20) == kInvalidPageId);
    VDB_CHECK((RawKey(root, 32) == BPlusKey{25, {0, 0}}));
    VDB_CHECK(tree.Contains(25, {0, 0}));  // Exactly equal to the separator routes right.
    VDB_CHECK(Drain(tree.LowerBound(22), db) == Expected(oracle, 22));  // End of left leaf.
    db.Close();

    Files internal_files("worked_internal");
    Database internal_db(internal_files.db, OpenMode::CreateNew, internal_files.temp, 1);
    auto growing = BPlusTree::Create(internal_db.Buffer(), UINT16_MAX, {4, 4});
    Oracle expected;
    for (int64_t i = 0; i < 10; ++i) Insert(internal_db, growing, expected, {i, {0, 0}});
    const PageId old_root = growing.RootPage();
    const auto allocated = internal_db.Disk().PageCount(FileKind::Database);
    VDB_CHECK(growing.Height() == 2 && ReadU16(Copy(internal_db, old_root), 16) == 4);
    Insert(internal_db, growing, expected, {10, {0, 0}});
    VDB_CHECK(internal_db.Disk().PageCount(FileKind::Database) == allocated + 3);
    VDB_CHECK(growing.Height() == 3);
    CheckTree(internal_db, growing, expected);
    const auto new_root = Copy(internal_db, growing.RootPage());
    VDB_CHECK(ReadU32(new_root, 20) == old_root);
    const auto left_internal = Copy(internal_db, old_root);
    const auto right_internal = Copy(internal_db, ReadU32(new_root, 48));
    VDB_CHECK(ReadU16(left_internal, 16) == 2 && ReadU16(right_internal, 16) == 3);
    const auto promoted = RawKey(new_root, 32);
    VDB_CHECK(promoted.key == 4);
    for (const auto& node : {left_internal, right_internal})
        for (size_t i = 0; i + 1 < ReadU16(node, 16); ++i)
            VDB_CHECK(!(RawKey(node, 32 + i * 20) == promoted));
    NoPins(internal_db);
    internal_db.Close();
}

void MiddleLeafSplit() {
    Files files("middle");
    Database db(files.db, OpenMode::CreateNew, files.temp, 2);
    auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
    Oracle oracle;
    for (int64_t key = 0; key <= 80; key += 10) Insert(db, tree, oracle, {key, {1, 0}});
    const auto before = CheckTree(db, tree, oracle);
    VDB_CHECK(before.leaves.size() == 4);
    const PageId middle = before.leaves[1];
    for (int64_t key : {21, 22, 23}) Insert(db, tree, oracle, {key, {1, 0}});
    const auto after = CheckTree(db, tree, oracle);
    VDB_CHECK(after.leaves.size() == 5);
    VDB_CHECK(after.leaves[0] == before.leaves[0] && after.leaves[1] == middle);
    VDB_CHECK(after.leaves[3] == before.leaves[2] && after.leaves[4] == before.leaves[3]);
    VDB_CHECK(ReadU32(Copy(db, middle), 20) == after.leaves[2]);
    VDB_CHECK(ReadU32(Copy(db, after.leaves[2]), 20) == before.leaves[2]);
    db.Close();
}

void PhysicalBoundaries() {
    Files files("physical_leaf");
    Database db(files.db, OpenMode::CreateNew, files.temp, 3);
    auto tree = BPlusTree::Create(db.Buffer());
    Oracle oracle;
    const PageId first = tree.RootPage();
    for (int64_t key = 0; key < 254; ++key) Insert(db, tree, oracle, {key, {1, 0}});
    VDB_CHECK(tree.RootPage() == first && tree.Height() == 1);
    VDB_CHECK(ReadU16(Copy(db, first), 16) == 254);
    Insert(db, tree, oracle, {254, {1, 0}});
    const auto shape = CheckTree(db, tree, oracle);
    VDB_CHECK(tree.Height() == 2 && shape.leaves.size() == 2);
    VDB_CHECK(ReadU16(Copy(db, shape.leaves[0]), 16) == 127);
    VDB_CHECK(ReadU16(Copy(db, shape.leaves[1]), 16) == 128);
    VDB_CHECK(Drain(tree.Range(126, 128), db) == Expected(oracle, 126, 128));
    db.Close();

    // Actual 204-child internal boundary, with smaller leaves to keep the test quick.
    Files internal_files("physical_internal");
    Database idb(internal_files.db, OpenMode::CreateNew, internal_files.temp, 3);
    auto itree = BPlusTree::Create(idb.Buffer(), UINT16_MAX, {4, 204});
    Oracle io;
    for (int64_t key = 0; key < 410; ++key) Insert(idb, itree, io, {key, {1, 0}});
    VDB_CHECK(itree.Height() == 2 && ReadU16(Copy(idb, itree.RootPage()), 16) == 204);
    Insert(idb, itree, io, {410, {1, 0}});
    VDB_CHECK(itree.Height() == 3);
    CheckTree(idb, itree, io);
    const auto root = Copy(idb, itree.RootPage());
    const auto left = Copy(idb, ReadU32(root, 20));
    const auto right = Copy(idb, ReadU32(root, 48));
    VDB_CHECK(ReadU16(left, 16) == 102 && ReadU16(right, 16) == 103);
    const auto promoted = RawKey(root, 32);
    for (const auto& node : {left, right})
        for (size_t i = 0; i + 1 < ReadU16(node, 16); ++i)
            VDB_CHECK(!(RawKey(node, 32 + i * 20) == promoted));
    idb.Close();
}

void OrderedInserts() {
    for (bool reverse : {false, true}) {
        Files files(reverse ? "descending" : "ascending");
        Database db(files.db, OpenMode::CreateNew, files.temp, 1);
        auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
        Oracle oracle;
        size_t largest_allocation = 0;
        for (int64_t i = 0; i < 350; ++i) {
            const auto pages = db.Disk().PageCount(FileKind::Database);
            Insert(db, tree, oracle, {reverse ? 349 - i : i, {0, 0}});
            largest_allocation = std::max(largest_allocation,
                size_t(db.Disk().PageCount(FileKind::Database) - pages));
            if (i < 25 || i % 37 == 0) CheckTree(db, tree, oracle);
        }
        CheckTree(db, tree, oracle);
        VDB_CHECK(tree.Height() >= 5 && largest_allocation >= 5);
        VDB_CHECK(db.Metrics().evictions > 0 && db.Metrics().dirty_evictions > 0);
        VDB_CHECK(db.Metrics().peak_pins == 1);
        std::cout << (reverse ? "descending" : "ascending") << ": height=" << tree.Height()
                  << " largest split allocation=" << largest_allocation << " peak pins=" << db.Metrics().peak_pins << '\n';
        db.Close();
    }
}

void DuplicatesExtremesRanges() {
    Files files("ranges");
    Database db(files.db, OpenMode::CreateNew, files.temp, 2);
    auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
    Oracle oracle;
    for (int64_t key : {kMin, int64_t(-30), int64_t(-10), int64_t(0), int64_t(10), int64_t(30), kMax}) {
        for (RID rid : {RID{256, 65535}, {1, 5}, {2, 0}, {0, 0}, {1, 2}, {255, 0}, {1, 1}, {0, 65535}})
            Insert(db, tree, oracle, {key, rid});
    }
    CheckTree(db, tree, oracle);
    for (int64_t low : {kMin, kMin + 1, int64_t(-40), int64_t(-30), int64_t(-20), int64_t(0), int64_t(10), int64_t(11), kMax}) {
        VDB_CHECK(Drain(tree.LowerBound(low), db) == Expected(oracle, low));
        VDB_CHECK(Drain(tree.EqualRange(low), db) == Expected(oracle, low, low));
        for (int64_t high : {kMin, int64_t(-29), int64_t(-10), int64_t(0), int64_t(10), int64_t(20), int64_t(30), kMax})
            VDB_CHECK(Drain(tree.Range(low, high), db) == Expected(oracle, low, high));
    }
    VDB_CHECK(!tree.Contains(10, {1, 3}));
    auto cursor = tree.LowerBound(10);
    VDB_CHECK(cursor.Next().has_value());
    Insert(db, tree, oracle, {10, {1, 2}});  // Exact duplicate does not invalidate.
    VDB_CHECK(cursor.Next().has_value());
    auto unconsumed = tree.EqualRange(kMax);
    auto exhausted = tree.Range(2, 1);
    Insert(db, tree, oracle, {10, {1, 3}});
    Throws([&] { cursor.Next(); });
    Throws([&] { unconsumed.Next(); });
    Throws([&] { exhausted.Next(); });
    NoPins(db);
    CheckTree(db, tree, oracle);
    db.Close();
}

void RandomizedAndReopen() {
    for (uint32_t seed : {uint32_t(20260930), uint32_t(0xBADC0DE), uint32_t(71827), uint32_t(203)}) {
        std::cout << "random seed=" << seed << std::endl;
        Files files("random");
        Oracle oracle;
        PageId meta, root;
        uint16_t height;
        const BPlusCapacity cap = seed == 71827 ? BPlusCapacity{5, 5} :
                                  seed == 203 ? BPlusCapacity{2, 3} : BPlusCapacity{4, 4};
        {
            Database db(files.db, OpenMode::CreateNew, files.temp, 1);
            auto tree = BPlusTree::Create(db.Buffer(), 0, cap);
            meta = tree.MetaPage();
            std::mt19937 rng(seed);
            for (int step = 0; step < 4000; ++step) {
                BPlusKey key{int64_t(rng() % 51) - 25, {PageId(rng() % 7), uint16_t(rng() % 5)}};
                if (step % 59 == 0) key.key = step % 118 == 0 ? kMin : kMax;
                Insert(db, tree, oracle, key);
                if (step % 7 == 0) Insert(db, tree, oracle, key);  // Guaranteed duplicate attempts.
                if (step % 41 != 0) continue;
                CheckTree(db, tree, oracle);
                for (int query = 0; query < 8; ++query) {
                    const int64_t low = int64_t(rng() % 61) - 30;
                    const int64_t high = int64_t(rng() % 61) - 30;
                    VDB_CHECK(Drain(tree.Range(low, high), db) == Expected(oracle, low, high));
                    const BPlusKey probe{low, {PageId(rng() % 8), uint16_t(rng() % 6)}};
                    VDB_CHECK(tree.Contains(probe.key, probe.rid) == oracle.contains(probe));
                }
            }
            CheckTree(db, tree, oracle);
            VDB_CHECK(tree.Height() >= 3);
            VDB_CHECK(db.Disk().PageCount(FileKind::Database) > db.Buffer().FrameCount());
            root = tree.RootPage();
            height = tree.Height();
            std::cout << "random entries=" << oracle.size() << " height=" << height << '\n';
            db.Close();
        }
        {
            Database db(files.db, OpenMode::OpenExisting, files.temp, 2);
            auto tree = BPlusTree::Open(db.Buffer(), meta, cap);
            VDB_CHECK(tree.RootPage() == root && tree.Height() == height && tree.MutationEpoch() == 0);
            CheckTree(db, tree, oracle);
            for (const auto& bounds : {std::pair{kMin, kMax}, {int64_t(-9), int64_t(12)}, {kMax, kMax}, {kMin, kMin}})
                VDB_CHECK(Drain(tree.Range(bounds.first, bounds.second), db) == Expected(oracle, bounds.first, bounds.second));
            Insert(db, tree, oracle, {7, {500, 5}});  // Reopened handle remains writable.
            CheckTree(db, tree, oracle);
            db.Close();
        }
        {
            Database db(files.db, OpenMode::OpenExisting, files.temp, 1);
            auto tree = BPlusTree::Open(db.Buffer(), meta, cap);
            CheckTree(db, tree, oracle);
            db.Close();
        }
    }
}

void IoErrorsPropagate() {
    Files files("io_error");
    Database db(files.db, OpenMode::CreateNew, files.temp, 1);
    auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
    Oracle oracle;
    for (int64_t i = 0; i < 4; ++i) Insert(db, tree, oracle, {i, {0, 0}});
    auto cursor = tree.LowerBound(kMin);
    db.Flush();
    db.Buffer().Invalidate({FileKind::Database, tree.RootPage()});
    std::filesystem::resize_file(files.db, 0);
    Throws([&] { cursor.Next(); }); // A read error must not become normal EOF.
    NoPins(db);

    // Abandon the truncated instance; no retry or recovery is promised.
}

void StableFencesAndCorruption() {
    Files files("stable_fences");
    Database db(files.db, OpenMode::CreateNew, files.temp, 1);
    auto tree = BPlusTree::Create(db.Buffer(), UINT16_MAX, {4, 4});
    Oracle oracle;
    for (int64_t key : {10, 20, 30, 40, 50}) Insert(db, tree, oracle, {key, {0, 0}});
    const auto shape = CheckTree(db, tree, oracle);
    const auto original_root = Copy(db, tree.RootPage());
    auto stable = original_root;
    WriteI64(stable, 32, 25);  // max(left)=20 < fence=25 < min(right)=30.
    Replace(db, tree.RootPage(), stable);
    CheckTree(db, tree, oracle);  // Must accept a fence that is NOT the right minimum.
    VDB_CHECK(Drain(tree.LowerBound(25), db) == Expected(oracle, 25));
    Insert(db, tree, oracle, {25, {0, 0}});  // Equality routes to the right of this stable fence.
    CheckTree(db, tree, oracle);

    const auto good_root = Copy(db, tree.RootPage());
    const auto good_meta = Copy(db, tree.MetaPage());
    const PageId first_leaf = shape.leaves[0];
    const auto good_leaf = Copy(db, first_leaf);
    auto expect_checker_failure = [&](PageId id, const PageBytes& bad, const PageBytes& good) {
        Replace(db, id, bad);
        bool caught = false;
        try { Validator(db.Buffer(), db.Disk().PageCount(FileKind::Database), tree).Run(); }
        catch (const test::CheckFailure&) { caught = true; }
        catch (const DbError&) { caught = true; }
        VDB_CHECK(caught);
        NoPins(db);
        Replace(db, id, good);
    };
    auto bad = good_root;
    WriteI64(bad, 32, 20);  // Violates strict left inequality.
    expect_checker_failure(tree.RootPage(), bad, good_root);
    bad = good_root;
    WriteI64(bad, 32, 31);  // Violates right inequality.
    expect_checker_failure(tree.RootPage(), bad, good_root);
    bad = good_root;
    WriteU32(bad, 48, ReadU32(bad, 20));  // Shared child.
    expect_checker_failure(tree.RootPage(), bad, good_root);
    bad = good_root;
    WriteU32(bad, 48, tree.RootPage());  // Ancestor cycle.
    expect_checker_failure(tree.RootPage(), bad, good_root);
    bad = good_root;
    WriteU32(bad, 48, db.Disk().PageCount(FileKind::Database));
    expect_checker_failure(tree.RootPage(), bad, good_root);
    bad = good_leaf;
    WriteU32(bad, 20, first_leaf);  // Leaf chain cycle.
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_leaf;
    WriteU32(bad, 20, kInvalidPageId);  // Truncated leaf chain.
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_leaf;
    WriteU16(bad, 16, 1);  // Nonroot underfull page.
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_leaf;
    WriteI64(bad, 48, 10);  // Duplicate physical entry.
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_leaf;
    WriteU16(bad, 4, uint16_t(PageType::BPlusInternal));
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_meta;
    WriteU16(bad, 20, 3);
    expect_checker_failure(tree.MetaPage(), bad, good_meta);
    bad = good_meta;
    WriteU32(bad, 16, first_leaf);
    expect_checker_failure(tree.MetaPage(), bad, good_meta);

    bad = good_meta;
    bad[4095] = std::byte{1}; // Full reserved-area auditing belongs to the test validator.
    expect_checker_failure(tree.MetaPage(), bad, good_meta);
    bad = good_leaf;
    bad[46] = std::byte{1}; // RID padding.
    expect_checker_failure(first_leaf, bad, good_leaf);
    bad = good_leaf;
    bad[24] = std::byte{1};
    expect_checker_failure(first_leaf, bad, good_leaf);

    // Open rejects malformed metadata and the wrong root type, without leaking pins.
    for (auto mutation : std::array<std::function<void(PageBytes&)>, 5>{
            [](PageBytes& p) { WriteU32(p, 16, kInvalidPageId); },
            [](PageBytes& p) { WriteU16(p, 20, 0); },
            [](PageBytes& p) { WriteU16(p, 20, 1); },
            [](PageBytes& p) { WriteU16(p, 22, 8); },
            [](PageBytes& p) { WriteU16(p, 6, 2); }}) {
        bad = good_meta;
        mutation(bad);
        Replace(db, tree.MetaPage(), bad);
        Throws([&] { BPlusTree::Open(db.Buffer(), tree.MetaPage(), {4, 4}); });
        NoPins(db);
        Replace(db, tree.MetaPage(), good_meta);
    }
    bad = good_leaf;
    WriteU16(bad, 16, 5);
    Replace(db, first_leaf, bad);
    Throws([&] { tree.Contains(10, {0, 0}); });
    Throws([&] { tree.LowerBound(10); });
    Throws([&] { tree.Insert(15, {0, 0}); });
    Replace(db, first_leaf, good_leaf);
    auto cursor = tree.LowerBound(10);
    Replace(db, first_leaf, bad);
    Throws([&] { cursor.Next(); });
    Replace(db, first_leaf, good_leaf);
    const auto pages = db.Disk().PageCount(FileKind::Database);
    Throws([&] { BPlusTree::Create(db.Buffer(), 8); });
    Throws([&] { BPlusTree::Create(db.Buffer(), 0, {4, 2}); });
    Throws([&] { BPlusTree::Open(db.Buffer(), tree.MetaPage(), {255, 4}); });
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == pages);
    CheckTree(db, tree, oracle);

    // Full internal ordering is also the independent validator's responsibility.
    for (int64_t key : {60, 70, 80}) Insert(db, tree, oracle, {key, {0, 0}});
    const auto ordered_root = Copy(db, tree.RootPage());
    VDB_CHECK(ReadU16(ordered_root, 16) >= 3);
    bad = ordered_root;
    WriteI64(bad, 52, ReadI64(bad, 32));
    WriteRID(bad, 60, ReadRID(bad, 40)); // Second separator duplicates the first.
    expect_checker_failure(tree.RootPage(), bad, ordered_root);
    CheckTree(db, tree, oracle);
    db.Close();
}

void Run(const char* name, const std::function<void()>& test) {
    std::cout << "[B+ tree insert] " << name << std::endl;
    test();
    ++cases_passed;
}
}  // namespace

int main() {
    try {
        Run("page formats and bounds", PageFormats);
        Run("empty and single entry reopen", EmptyAndSingleReopen);
        Run("leaf and internal split examples", LeafAndInternalSplits);
        Run("middle leaf split", MiddleLeafSplit);
        Run("production capacity boundaries", PhysicalBoundaries);
        Run("increasing and decreasing insertion", OrderedInserts);
        Run("duplicates, extremes, ranges and cursor epochs", DuplicatesExtremesRanges);
        Run("randomized oracle and multilevel reopen", RandomizedAndReopen);
        Run("I/O errors propagate and release pins", IoErrorsPropagate);
        Run("stable lower fences and corruption detection", StableFencesAndCorruption);
        std::cout << "B+ tree insert passed: " << cases_passed << " cases; max height=" << maximum_height
                  << "; peak pins=" << maximum_pins << "; active pins=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
