#include "bplus_validator.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/database.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <random>

using namespace valencedb;
using namespace bplus_test;

namespace {
uint16_t maximum_height = 0;
uint64_t maximum_pins = 0;
size_t cases_passed = 0;
size_t contractions = 0;
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_bplus_delete_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string(++serial));
        std::filesystem::create_directory(directory);
        db = directory / "pages.db";
        temp = directory / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(directory); }
};
void Throws(const std::function<void()>& action) {
    bool caught = false;
    try { action(); } catch (const DbError&) { caught = true; }
    VDB_CHECK(caught);
}
void Pins(Database& db) {
    VDB_CHECK(db.Metrics().active_pins == 0);
    maximum_pins = std::max(maximum_pins, db.Metrics().peak_pins);
    VDB_CHECK(db.Metrics().peak_pins <= 1);
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
std::vector<BPlusKey> Drain(Database& db, BPlusIterator cursor) {
    std::vector<BPlusKey> result;
    Pins(db);
    while (auto entry = cursor.Next()) { result.push_back(*entry); Pins(db); }
    VDB_CHECK(!cursor.Next());
    Pins(db);
    return result;
}
std::vector<BPlusKey> Expected(const Oracle& oracle, int64_t low = INT64_MIN, int64_t high = INT64_MAX) {
    std::vector<BPlusKey> result;
    for (auto entry : oracle) if (entry.key >= low && entry.key <= high) result.push_back(entry);
    return result;
}
TreeShape Check(Database& db, const BPlusTree& tree, const Oracle& oracle,
                const std::set<PageId>& retired = {}) {
    auto shape = Validator(db.Buffer(), db.Disk().PageCount(FileKind::Database), tree, retired).Run();
    maximum_height = std::max(maximum_height, shape.height);
    VDB_CHECK(shape.entries == Expected(oracle));
    VDB_CHECK(Drain(db, tree.LowerBound(INT64_MIN)) == shape.entries);
    Pins(db);
    return shape;
}
void Insert(Database& db, BPlusTree& tree, Oracle& oracle, BPlusKey key) {
    const auto epoch = tree.MutationEpoch();
    const bool added = oracle.insert(key).second;
    VDB_CHECK(tree.Insert(key.key, key.rid) == added);
    VDB_CHECK(tree.MutationEpoch() == epoch + added);
    Pins(db);
}
void Erase(Database& db, BPlusTree& tree, Oracle& oracle, BPlusKey key) {
    const auto epoch = tree.MutationEpoch();
    const auto pages = db.Disk().PageCount(FileKind::Database);
    const auto height = tree.Height();
    auto stale = tree.LowerBound(INT64_MIN);
    const bool removed = oracle.erase(key) != 0;
    VDB_CHECK(tree.Erase(key.key, key.rid) == removed);
    VDB_CHECK(tree.MutationEpoch() == epoch + removed);
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == pages);
    if (removed) Throws([&] { stale.Next(); });
    else VDB_CHECK(Drain(db, stale) == Expected(oracle));
    if (tree.Height() < height) { ++contractions; VDB_CHECK(tree.Height() + 1 == height); }
    Pins(db);
}
BPlusKey K(int64_t key) { return {key, {1, 0}}; }
std::vector<PageId> Children(const PageBytes& page) {
    std::vector<PageId> result{ReadU32(page, 20)};
    for (size_t i = 1; i < ReadU16(page, 16); ++i) result.push_back(ReadU32(page, 48 + 20 * (i - 1)));
    return result;
}
std::vector<BPlusKey> Fences(const PageBytes& page) {
    std::vector<BPlusKey> result;
    for (size_t i = 1; i < ReadU16(page, 16); ++i) result.push_back(RawKey(page, 32 + 20 * (i - 1)));
    return result;
}
std::vector<BPlusKey> Entries(const PageBytes& page) {
    std::vector<BPlusKey> result;
    for (size_t i = 0; i < ReadU16(page, 16); ++i) result.push_back(RawKey(page, 32 + 16 * i));
    return result;
}

// Exact valid page fixtures, not a second deletion implementation. Gapped fences
// ensure internal rotation must preserve/promote the actual fences, not minima.
struct Fixture {
    PageId meta, root;
    std::vector<PageId> leaves, internals;
    Oracle oracle;
};
Fixture MakeFixture(Database& db, const std::vector<std::vector<std::vector<int64_t>>>& groups) {
    Fixture f{};
    for (const auto& group : groups) for (const auto& keys : group) {
        auto guard = db.Buffer().NewPage(FileKind::Database);
        const auto id = guard.Page().page_id;
        f.leaves.push_back(id);
        BPlusLeafPage::Initialize(guard.MutableData(), id);
        std::vector<BPlusKey> entries;
        for (auto key : keys) { entries.push_back(K(key)); f.oracle.insert(K(key)); }
        BPlusLeafPage::Write(guard.MutableData(), entries, 4);
        guard.MarkDirty();
    }
    for (size_t i = 0; i + 1 < f.leaves.size(); ++i) {
        auto guard = db.Buffer().Fetch({FileKind::Database, f.leaves[i]});
        BPlusLeafPage::SetNext(guard.MutableData(), f.leaves[i + 1]);
        guard.MarkDirty();
    }
    size_t at = 0;
    for (const auto& group : groups) {
        std::vector<PageId> children;
        std::vector<BPlusKey> separators;
        for (size_t i = 0; i < group.size(); ++i) {
            children.push_back(f.leaves[at++]);
            if (i) separators.push_back(K(group[i].front() - 1));
        }
        auto guard = db.Buffer().NewPage(FileKind::Database);
        f.internals.push_back(guard.Page().page_id);
        BPlusInternalPage::Initialize(guard.MutableData(), f.internals.back());
        BPlusInternalPage::Write(guard.MutableData(), children, separators, 4);
        guard.MarkDirty();
    }
    f.root = f.internals.front();
    if (groups.size() > 1) {
        std::vector<BPlusKey> separators;
        for (size_t i = 1; i < groups.size(); ++i) separators.push_back(K(groups[i][0][0] - 1));
        auto guard = db.Buffer().NewPage(FileKind::Database);
        f.root = guard.Page().page_id;
        BPlusInternalPage::Initialize(guard.MutableData(), f.root);
        BPlusInternalPage::Write(guard.MutableData(), f.internals, separators, 4);
        guard.MarkDirty();
    }
    auto guard = db.Buffer().NewPage(FileKind::Database);
    f.meta = guard.Page().page_id;
    BPlusMetaPage::Write(guard.MutableData(), f.meta, {f.root, uint16_t(groups.size() > 1 ? 3 : 2), UINT16_MAX});
    guard.MarkDirty();
    return f;
}
void Reopen(Files& files, PageId meta, PageId root, uint16_t height, const Oracle& oracle,
            const std::set<PageId>& retired = {}, BPlusCapacity cap = {4, 4}) {
    Database db(files.db, OpenMode::OpenExisting, files.temp, 1);
    auto tree = BPlusTree::Open(db.Buffer(), meta, cap);
    VDB_CHECK(tree.RootPage() == root && tree.Height() == height);
    Check(db, tree, oracle, retired);
    for (int64_t key : {INT64_MIN, int64_t(10), int64_t(40), INT64_MAX}) {
        VDB_CHECK(Drain(db, tree.EqualRange(key)) == Expected(oracle, key, key));
        VDB_CHECK(Drain(db, tree.Range(key, INT64_MAX)) == Expected(oracle, key));
    }
    db.Close();
}

void LeafBranches() {
    for (int branch = 0; branch < 5; ++branch) {
        std::cout << "leaf fixture=" << branch << " (left borrow/right borrow/left merge/right merge/right with left present)\n";
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 1);
        std::vector<std::vector<int64_t>> leaves;
        int64_t erased;
        if (branch == 0) { leaves = {{10,20,30},{40,50},{60,70,80}}; erased = 40; }
        else if (branch == 1) { leaves = {{10,20},{30,40,50},{60,70}}; erased = 10; }
        else if (branch == 2) { leaves = {{10,20},{30,40},{50,60}}; erased = 30; }
        else if (branch == 3) { leaves = {{10,20},{30,40},{50,60}}; erased = 10; }
        else { leaves = {{10,20},{30,40},{50,60,70}}; erased = 30; }
        auto f = MakeFixture(db, {leaves});
        auto tree = BPlusTree::Open(db.Buffer(), f.meta, {4,4});
        Check(db, tree, f.oracle);
        const auto old_root = Copy(db, f.root);
        Erase(db, tree, f.oracle, K(erased));
        std::set<PageId> retired;
        auto root = Copy(db, f.root);
        if (branch == 0) {
            VDB_CHECK((Entries(Copy(db,f.leaves[0])) == std::vector<BPlusKey>{K(10),K(20)}));
            VDB_CHECK((Entries(Copy(db,f.leaves[1])) == std::vector<BPlusKey>{K(30),K(50)}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(30),K(59)}));
        } else if (branch == 1) {
            VDB_CHECK((Entries(Copy(db,f.leaves[0])) == std::vector<BPlusKey>{K(20),K(30)}));
            VDB_CHECK((Entries(Copy(db,f.leaves[1])) == std::vector<BPlusKey>{K(40),K(50)}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(40),K(59)}));
        } else if (branch == 4) {
            VDB_CHECK((Entries(Copy(db,f.leaves[1])) == std::vector<BPlusKey>{K(40),K(50)}));
            VDB_CHECK((Entries(Copy(db,f.leaves[2])) == std::vector<BPlusKey>{K(60),K(70)}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(29),K(60)}));
        } else {
            retired.insert(f.leaves[1]);
            const auto expected = branch == 2 ? std::vector<BPlusKey>{K(10),K(20),K(40)}
                                             : std::vector<BPlusKey>{K(20),K(30),K(40)};
            VDB_CHECK(Entries(Copy(db, f.leaves[0])) == expected);
            VDB_CHECK((Children(root) == std::vector<PageId>{f.leaves[0],f.leaves[2]}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(49)}));
            VDB_CHECK(ReadU32(Copy(db, f.leaves[0]),20) == f.leaves[2]);
        }
        if (retired.empty()) {
            VDB_CHECK(Children(root) == Children(old_root));
            for (size_t i = 0; i < f.leaves.size(); ++i)
                VDB_CHECK(ReadU32(Copy(db,f.leaves[i]),20) == (i+1 == f.leaves.size() ? kInvalidPageId : f.leaves[i+1]));
        }
        Check(db, tree, f.oracle, retired);
        db.Close();
        Reopen(files,f.meta,tree.RootPage(),tree.Height(),f.oracle,retired);
    }
}

void InternalBranches() {
    for (int branch = 0; branch < 4; ++branch) {
        std::cout << "internal fixture=" << branch << " (left rotation/right rotation/left merge/right merge)\n";
        Files files;
        Database db(files.db, OpenMode::CreateNew, files.temp, 1);
        std::vector<std::vector<std::vector<int64_t>>> groups;
        if (branch == 0) groups = {{{10,11},{20,21},{30,31}},{{40,41},{50,51}}};
        else if (branch == 1) groups = {{{10,11},{20,21}},{{30,31},{40,41},{50,51}}};
        else groups = {{{10,11},{20,21}},{{30,31},{40,41}},{{50,51},{60,61}}};
        auto f = MakeFixture(db, groups);
        auto tree = BPlusTree::Open(db.Buffer(), f.meta, {4,4});
        Check(db,tree,f.oracle);
        const int64_t erased = branch == 0 ? 40 : branch == 2 ? 30 : 10;
        Erase(db,tree,f.oracle,K(erased));
        std::set<PageId> retired;
        auto root = Copy(db,tree.RootPage());
        if (branch == 0) {
            retired.insert(f.leaves[4]);
            VDB_CHECK((Children(Copy(db,f.internals[0])) == std::vector<PageId>{f.leaves[0],f.leaves[1]}));
            VDB_CHECK((Children(Copy(db,f.internals[1])) == std::vector<PageId>{f.leaves[2],f.leaves[3]}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(29)}));
            VDB_CHECK((Fences(Copy(db,f.internals[1])) == std::vector<BPlusKey>{K(39)}));
            VDB_CHECK((Fences(Copy(db,f.internals[0])) == std::vector<BPlusKey>{K(19)}));
        } else if (branch == 1) {
            retired.insert(f.leaves[1]);
            VDB_CHECK((Children(Copy(db,f.internals[0])) == std::vector<PageId>{f.leaves[0],f.leaves[2]}));
            VDB_CHECK((Children(Copy(db,f.internals[1])) == std::vector<PageId>{f.leaves[3],f.leaves[4]}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(39)}));
            VDB_CHECK((Fences(Copy(db,f.internals[0])) == std::vector<BPlusKey>{K(29)}));
            VDB_CHECK((Fences(Copy(db,f.internals[1])) == std::vector<BPlusKey>{K(49)}));
        } else {
            retired.insert(f.internals[1]);
            retired.insert(f.leaves[branch == 2 ? 3 : 1]);
            const auto expected_children = branch == 2 ? std::vector<PageId>{f.leaves[0],f.leaves[1],f.leaves[2]}
                                                       : std::vector<PageId>{f.leaves[0],f.leaves[2],f.leaves[3]};
            const auto expected_fences = branch == 2 ? std::vector<BPlusKey>{K(19),K(29)}
                                                     : std::vector<BPlusKey>{K(29),K(39)};
            VDB_CHECK(Children(Copy(db,f.internals[0])) == expected_children);
            VDB_CHECK(Fences(Copy(db,f.internals[0])) == expected_fences);
            VDB_CHECK((Children(root) == std::vector<PageId>{f.internals[0],f.internals[2]}));
            VDB_CHECK((Fences(root) == std::vector<BPlusKey>{K(49)}));
        }
        VDB_CHECK(tree.Height() == 3 && tree.RootPage() == f.root);
        Check(db,tree,f.oracle,retired);
        db.Close();
        Reopen(files,f.meta,tree.RootPage(),tree.Height(),f.oracle,retired);
    }
}

void StableFenceAndAbsent() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,1);
    auto tree = BPlusTree::Create(db.Buffer(),UINT16_MAX,{4,4});
    Oracle oracle;
    for (int64_t key : {10,20,30,40,50}) Insert(db,tree,oracle,K(key));
    const auto root = Copy(db,tree.RootPage());
    VDB_CHECK(RawKey(root,32) == K(30));
    auto exhausted = tree.Range(2,1);
    VDB_CHECK(!exhausted.Next());
    Erase(db,tree,oracle,K(30));
    Throws([&] { exhausted.Next(); });
    VDB_CHECK(Copy(db,tree.RootPage()) == root); // Fence 30 stays below the new minimum 40.
    Check(db,tree,oracle);
    VDB_CHECK(Drain(db,tree.LowerBound(30)) == Expected(oracle,30));
    const auto shape = Check(db,tree,oracle);
    std::map<PageId,PageBytes> before;
    for (auto id : shape.reachable) before.emplace(id,Copy(db,id));
    for (auto absent : {K(999),BPlusKey{40,{9,9}},K(30)}) {
        Erase(db,tree,oracle,absent);
        for (const auto& [id,bytes] : before) VDB_CHECK(Copy(db,id) == bytes);
        Check(db,tree,oracle);
    }
    Throws([&] { tree.Erase(10,{kInvalidPageId,0}); });
    Pins(db);
    db.Close();
    Reopen(files,tree.MetaPage(),tree.RootPage(),tree.Height(),oracle);
}

void CascadeEmptyReinsert() {
    for (bool descending : {false,true}) {
        Files files;
        Database db(files.db,OpenMode::CreateNew,files.temp,1);
        auto tree = BPlusTree::Create(db.Buffer(),UINT16_MAX,{4,4});
        Oracle oracle;
        for (int64_t i = 0; i < 350; ++i) Insert(db,tree,oracle,K(i));
        auto before = Check(db,tree,oracle);
        const auto initial_height = tree.Height();
        const auto initial_root = tree.RootPage();
        VDB_CHECK(initial_height >= 3);
        const auto pages = db.Disk().PageCount(FileKind::Database);
        std::set<PageId> retired;
        size_t largest_cascade = 0;
        for (int64_t i = 0; i < 350; ++i) {
            Erase(db,tree,oracle,K(descending ? 349-i : i));
            auto after = Check(db,tree,oracle,retired);
            size_t removed = 0;
            for (auto id : before.reachable) if (!after.reachable.contains(id)) { retired.insert(id); ++removed; }
            largest_cascade = std::max(largest_cascade,removed);
            for (const auto& frame : db.Buffer().InspectFrames())
                if (frame.page) VDB_CHECK(!retired.contains(frame.page->page_id));
            before = std::move(after);
        }
        VDB_CHECK(tree.Height() == 1 && tree.RootPage() != initial_root);
        VDB_CHECK(retired.contains(initial_root) && largest_cascade >= 3);
        const auto empty_root = tree.RootPage();
        VDB_CHECK(ReadU16(Copy(db,empty_root),16) == 0);
        VDB_CHECK(ReadU32(Copy(db,empty_root),20) == kInvalidPageId);
        VDB_CHECK(db.Disk().PageCount(FileKind::Database) == pages);
        std::cout << "cascade " << (descending ? "descending" : "ascending") << ": height " << initial_height
                  << " -> 1, largest retired cascade=" << largest_cascade << '\n';
        const auto meta = tree.MetaPage();
        db.Close();
        Reopen(files,meta,empty_root,1,oracle,retired);
        {
            Database reopened(files.db,OpenMode::OpenExisting,files.temp,1);
            auto again = BPlusTree::Open(reopened.Buffer(),meta,{4,4});
            Insert(reopened,again,oracle,K(42));
            VDB_CHECK(again.RootPage() == empty_root);
            for (int64_t i = 0; i < 80; ++i) Insert(reopened,again,oracle,K(i));
            auto repopulated = Check(reopened,again,oracle,retired);
            for (auto id : repopulated.reachable)
                VDB_CHECK(id == meta || id == empty_root || id >= pages);
            VDB_CHECK(reopened.Disk().PageCount(FileKind::Database) > pages);
            reopened.Close();
            Reopen(files,meta,again.RootPage(),again.Height(),oracle,retired);
        }
    }
}

void CascadingRootReopen() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,1);
    auto tree = BPlusTree::Create(db.Buffer(),UINT16_MAX,{4,4});
    Oracle oracle;
    for (int64_t i = 0; i <= 10; ++i) Insert(db,tree,oracle,K(i));
    VDB_CHECK(tree.Height() == 3);
    const auto old_root = tree.RootPage();
    const auto old_children = Children(Copy(db,old_root));
    auto before = Check(db,tree,oracle);
    std::set<PageId> retired;
    for (int64_t i = 0; i <= 2; ++i) {
        Erase(db,tree,oracle,K(i));
        auto after = Check(db,tree,oracle,retired);
        for (auto id : before.reachable) if (!after.reachable.contains(id)) retired.insert(id);
        before = std::move(after);
    }
    VDB_CHECK(tree.Height() == 2 && tree.RootPage() == old_children[0]);
    VDB_CHECK(retired.contains(old_root) && retired.contains(old_children[1]));
    VDB_CHECK((Fences(Copy(db,tree.RootPage())) == std::vector<BPlusKey>{K(6),K(8)}));
    // Prove that the extended checker rejects an allegedly retired reachable page.
    bool caught = false;
    try { Validator(db.Buffer(),db.Disk().PageCount(FileKind::Database),tree,{tree.RootPage()}).Run(); }
    catch (const test::CheckFailure&) { caught = true; }
    VDB_CHECK(caught);
    Pins(db);
    db.Close();
    Reopen(files,tree.MetaPage(),tree.RootPage(),tree.Height(),oracle,retired);
}

void DuplicatesExtremes() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,2);
    auto tree = BPlusTree::Create(db.Buffer(),UINT16_MAX,{4,4});
    Oracle oracle;
    for (int64_t key : {INT64_MIN,int64_t(10),INT64_MAX})
        for (uint16_t i = 0; i < 40; ++i) Insert(db,tree,oracle,{key,{PageId(i%3),i}});
    Check(db,tree,oracle);
    for (int64_t key : {INT64_MIN,int64_t(10),INT64_MAX}) {
        for (uint16_t i = 0; i < 40; i += 2) {
            Erase(db,tree,oracle,{key,{PageId(i%3),i}});
            Check(db,tree,oracle);
            VDB_CHECK(!tree.Contains(key,{PageId(i%3),i}));
            VDB_CHECK(tree.Contains(key,{PageId((i+1)%3),uint16_t(i+1)}));
        }
        VDB_CHECK(Drain(db,tree.EqualRange(key)) == Expected(oracle,key,key));
    }
    Erase(db,tree,oracle,{10,{0,0}});
    Check(db,tree,oracle);
    db.Close();
    Reopen(files,tree.MetaPage(),tree.RootPage(),tree.Height(),oracle);
}

void ProductionCapacity() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,1);
    auto tree = BPlusTree::Create(db.Buffer());
    Oracle oracle;
    for (int64_t i = 0; i < 255; ++i) Insert(db,tree,oracle,K(i));
    auto shape = Check(db,tree,oracle);
    VDB_CHECK(tree.Height() == 2 && shape.leaves.size() == 2);
    VDB_CHECK(ReadU16(Copy(db,shape.leaves[0]),16) == 127);
    VDB_CHECK(ReadU16(Copy(db,shape.leaves[1]),16) == 128);
    Erase(db,tree,oracle,K(0)); // Right lends its 128th entry.
    Check(db,tree,oracle);
    VDB_CHECK(ReadU16(Copy(db,shape.leaves[0]),16) == 127);
    VDB_CHECK(ReadU16(Copy(db,shape.leaves[1]),16) == 127);
    VDB_CHECK(RawKey(Copy(db,tree.RootPage()),32) == K(128));
    const auto old_root = tree.RootPage();
    Erase(db,tree,oracle,K(1)); // 126 + 127 merge, then root contraction.
    Check(db,tree,oracle,{old_root,shape.leaves[1]});
    VDB_CHECK(tree.Height() == 1 && tree.RootPage() == shape.leaves[0]);
    Erase(db,tree,oracle,K(2));
    Check(db,tree,oracle);
    db.Close();
    Reopen(files,tree.MetaPage(),tree.RootPage(),tree.Height(),oracle,{},{});
}

void RandomOracle() {
    for (uint32_t seed : {uint32_t(20260930),uint32_t(0xBADC0DE),uint32_t(71827),uint32_t(203)}) {
        std::cout << "tree seed=" << seed << " operations=4000\n";
        const BPlusCapacity cap = seed == 71827 ? BPlusCapacity{5,5} : seed == 203 ? BPlusCapacity{2,3} : BPlusCapacity{4,4};
        Files files;
        Database db(files.db,OpenMode::CreateNew,files.temp,1);
        auto tree = BPlusTree::Create(db.Buffer(),UINT16_MAX,cap);
        Oracle oracle;
        std::mt19937 rng(seed);
        for (size_t step = 0; step < 4000; ++step) {
            BPlusKey key{int64_t(rng()%51)-25,{PageId(rng()%8),uint16_t(rng()%8)}};
            if (step%71 == 0) key.key = step%142 ? INT64_MIN : INT64_MAX;
            const auto op = rng()%100;
            if (op < 43) Insert(db,tree,oracle,key);
            else if (op < 80) {
                if (!oracle.empty() && rng()%4 != 0) {
                    auto it = oracle.begin();
                    std::advance(it,rng()%oracle.size());
                    key = *it;
                }
                Erase(db,tree,oracle,key);
            } else VDB_CHECK(tree.Contains(key.key,key.rid) == oracle.contains(key));
            if (step%37 == 0) {
                Check(db,tree,oracle);
                for (int probe = 0; probe < 6; ++probe) {
                    const int64_t low = int64_t(rng()%61)-30, high = int64_t(rng()%61)-30;
                    VDB_CHECK(Drain(db,tree.Range(low,high)) == Expected(oracle,low,high));
                    VDB_CHECK(Drain(db,tree.EqualRange(low)) == Expected(oracle,low,low));
                    VDB_CHECK(Drain(db,tree.LowerBound(low)) == Expected(oracle,low));
                    const BPlusKey point{low,{PageId(rng()%8),uint16_t(rng()%8)}};
                    VDB_CHECK(tree.Contains(point.key,point.rid) == oracle.contains(point));
                }
            }
            Pins(db);
        }
        Check(db,tree,oracle);
        std::cout << "tree final entries=" << oracle.size() << " height=" << tree.Height() << '\n';
        VDB_CHECK(db.Metrics().evictions > 0);
        db.Close();
        Reopen(files,tree.MetaPage(),tree.RootPage(),tree.Height(),oracle,{},cap);
    }
}

Schema TableSchema() {
    return Schema(SchemaKind::Base,{{"id",TypeId::Int64},{"key",TypeId::Int64},{"tag",TypeId::Varchar,32}});
}
Tuple Row(int64_t key, int64_t id = 99) { return Tuple({Value(id),Value(key),Value(std::string("owned row"))}); }
using Rows = std::map<RID,Tuple>;
Oracle RowOracle(const Rows& rows) {
    Oracle oracle;
    for (const auto& [rid,tuple] : rows) oracle.insert({tuple.value(1).as_int64(),rid});
    return oracle;
}
void Consistent(Database& db, const std::string& name, const Rows& rows) {
    const auto table = db.Table(name);
    VDB_CHECK(table.live_rows == rows.size() && table.index_col == 1);
    const auto raw = Copy(db,table.meta_page);
    VDB_CHECK(ReadU16(raw,36) == table.index_col && ReadU32(raw,40) == table.index_meta);
    auto scan = db.Scan(name);
    std::set<RID> seen;
    while (auto row = scan.Next()) {
        VDB_CHECK(seen.insert(row->first).second && rows.contains(row->first));
        VDB_CHECK(EncodeTuple(table.schema,row->second) == EncodeTuple(table.schema,rows.at(row->first)));
        Pins(db);
    }
    VDB_CHECK(seen.size() == rows.size());
    const auto oracle = RowOracle(rows);
    Check(db,db.Index(name),oracle);
    auto cursor = db.IndexRange(name,INT64_MIN,INT64_MAX);
    seen.clear();
    while (auto entry = cursor.Next()) {
        VDB_CHECK(seen.insert(entry->rid).second);
        const auto tuple = db.Fetch(name,entry->rid);
        VDB_CHECK(tuple && tuple->value(1).as_int64() == entry->key);
        VDB_CHECK(rows.contains(entry->rid));
    }
    VDB_CHECK(seen.size() == rows.size());
    for (int64_t low : {INT64_MIN,int64_t(-10),int64_t(10),INT64_MAX}) {
        VDB_CHECK(Drain(db,db.IndexEqualRange(name,low)) == Expected(oracle,low,low));
        VDB_CHECK(Drain(db,db.IndexRange(name,low,INT64_MAX)) == Expected(oracle,low));
    }
    VDB_CHECK(Drain(db,db.IndexRange(name,10,-10)).empty());
    Pins(db);
}
void AddRow(Database& db, Rows& rows, int64_t key, int64_t id = 99) {
    const auto tuple = Row(key,id);
    const auto rid = db.Insert("items",tuple);
    VDB_CHECK(rows.emplace(rid,tuple).second);
    VDB_CHECK(db.Index("items").Contains(key,rid));
    const auto fetched = db.Fetch("items",rid);
    VDB_CHECK(fetched && fetched->value(1).as_int64() == key);
    Pins(db);
}
void DeleteRow(Database& db, Rows& rows, RID rid) {
    const auto key = rows.at(rid).value(1).as_int64();
    auto stale = db.IndexEqualRange("items",key);
    VDB_CHECK(db.Delete("items",rid));
    rows.erase(rid);
    VDB_CHECK(!db.Fetch("items",rid));
    VDB_CHECK(!db.Index("items").Contains(key,rid));
    Throws([&] { stale.Next(); });
    const auto epoch = db.Index("items").MutationEpoch();
    VDB_CHECK(!db.Delete("items",rid));
    VDB_CHECK(db.Index("items").MutationEpoch() == epoch);
    Pins(db);
}

void IndexLifecycle() {
    for (bool build_after : {false,true}) {
        Files files;
        Rows rows;
        PageId meta,root;
        {
            Database db(files.db,OpenMode::CreateNew,files.temp,1);
            db.CreateTable("items",TableSchema());
            VDB_CHECK(db.Table("items").index_meta == kInvalidPageId);
            VDB_CHECK(db.Table("items").index_col == UINT16_MAX);
            Throws([&] { db.IndexRange("items",0,1); });
            if (build_after) {
                for (int64_t i = 0; i < 420; ++i) {
                    const auto tuple = Row(i%19-9,i);
                    rows.emplace(db.Insert("items",tuple),tuple);
                }
                for (int i = 0; i < 25; ++i) {
                    auto rid = rows.begin()->first;
                    VDB_CHECK(db.Delete("items",rid));
                    rows.erase(rid);
                }
                for (int64_t key : {INT64_MIN,INT64_MAX}) {
                    auto tuple = Row(key);
                    rows.emplace(db.Insert("items",tuple),tuple);
                }
            }
            db.CreateIndex("items",1);
            meta = db.Table("items").index_meta;
            Consistent(db,"items",rows);
            if (!build_after) VDB_CHECK(db.Index("items").Height() == 1);
            // Exercise empty publication through an actual reopen before population.
            if (!build_after) {
                root = db.Index("items").RootPage();
                db.Close();
            } else {
                for (int i = 0; i < 30; ++i) AddRow(db,rows,10,i);
                Consistent(db,"items",rows);
                db.Close();
            }
        }
        {
            Database db(files.db,OpenMode::OpenExisting,files.temp,1);
            VDB_CHECK(db.Table("items").index_meta == meta);
            if (!build_after) VDB_CHECK(db.Index("items").RootPage() == root);
            Consistent(db,"items",rows);
            for (int64_t i = 0; i < 400; ++i) AddRow(db,rows,i%17-8,i);
            for (int64_t key : {INT64_MIN,INT64_MAX,int64_t(10),int64_t(10),int64_t(10)}) AddRow(db,rows,key);
            const auto duplicate_rids = Drain(db,db.IndexEqualRange("items",10));
            VDB_CHECK(duplicate_rids.size() >= 3);
            const auto a = duplicate_rids[0].rid, b = duplicate_rids[1].rid, c = duplicate_rids[2].rid;
            DeleteRow(db,rows,b);
            VDB_CHECK(db.Index("items").Contains(10,a) && db.Index("items").Contains(10,c));
            Consistent(db,"items",rows);
            VDB_CHECK(db.Index("items").Height() == 2);
            root = db.Index("items").RootPage();
            while (!rows.empty()) {
                DeleteRow(db,rows,rows.begin()->first);
                if (rows.size()%53 == 0) Consistent(db,"items",rows);
            }
            Consistent(db,"items",rows);
            VDB_CHECK(db.Index("items").Height() == 1 && db.Index("items").RootPage() != root);
            root = db.Index("items").RootPage();
            db.Close();
        }
        {
            Database db(files.db,OpenMode::OpenExisting,files.temp,1);
            VDB_CHECK(db.Table("items").index_meta == meta && db.Index("items").RootPage() == root);
            Consistent(db,"items",rows);
            AddRow(db,rows,INT64_MAX);
            AddRow(db,rows,10);
            Consistent(db,"items",rows);
            db.Close();
        }
        {
            Database db(files.db,OpenMode::OpenExisting,files.temp,1);
            Consistent(db,"items",rows);
            db.Close();
        }
    }
}

void IndexRejectionsAndCorruption() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,1);
    db.CreateTable("items",TableSchema());
    db.CreateTable("foreign",TableSchema());
    const auto pages = db.Disk().PageCount(FileKind::Database);
    const auto meta_before = Copy(db,db.Table("items").meta_page);
    Throws([&] { db.CreateIndex("items",2); }); // VARCHAR
    Throws([&] { db.CreateIndex("items",3); });
    Throws([&] { db.CreateIndex("items",size_t(-1)); });
    Throws([&] { db.CreateIndex("absent",1); });
    Throws([&] { Schema(SchemaKind::Base,{{"bad",TypeId::Double}}); });
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == pages);
    VDB_CHECK(Copy(db,db.Table("items").meta_page) == meta_before);
    db.CreateIndex("items",1);
    Rows rows;
    AddRow(db,rows,10);
    AddRow(db,rows,10);
    AddRow(db,rows,20);
    const auto table_before = Copy(db,db.Table("items").meta_page);
    const auto index_before = Copy(db,db.Index("items").RootPage());
    const auto allocated = db.Disk().PageCount(FileKind::Database);
    Throws([&] { db.CreateIndex("items",0); });
    Throws([&] { db.Insert("items",Tuple({Value(int64_t(1))})); });
    Throws([&] { db.Insert("items",Tuple({Value(int64_t(1)),Value(std::string("wrong")),Value(std::string("tag"))})); });
    Throws([&] { db.Insert("items",Tuple({Value(int64_t(1)),Value(int64_t(1)),Value(std::string(33,'x'))})); });
    Throws([&] { db.Insert("items",Tuple({Value(int64_t(1)),Value::Null(TypeId::Int64),Value(std::string("tag"))})); });
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == allocated);
    VDB_CHECK(Copy(db,db.Table("items").meta_page) == table_before);
    VDB_CHECK(Copy(db,db.Index("items").RootPage()) == index_before);
    const auto foreign = db.Insert("foreign",Row(10));
    const auto epoch = db.Index("items").MutationEpoch();
    Throws([&] { db.Delete("items",foreign); });
    VDB_CHECK(db.Fetch("foreign",foreign).has_value());
    VDB_CHECK(db.Index("items").MutationEpoch() == epoch);
    Consistent(db,"items",rows);
    VDB_CHECK(!db.Delete("items",{kInvalidPageId,0}));
    VDB_CHECK(!db.Delete("items",{rows.begin()->first.page_id,UINT16_MAX}));

    // Corrupt only the index page: a live heap row's exact pair is missing.
    const auto rid = rows.begin()->first;
    const auto root = db.Index("items").RootPage();
    auto broken = Copy(db,root);
    auto entries = Entries(broken);
    entries.erase(std::find(entries.begin(),entries.end(),BPlusKey{10,rid}));
    BPlusLeafPage::Write(broken,entries);
    Replace(db,root,broken);
    Throws([&] { db.Delete("items",rid); });
    VDB_CHECK(db.Fetch("items",rid).has_value());
    VDB_CHECK(db.Index("items").MutationEpoch() == epoch);
    VDB_CHECK(Copy(db,db.Table("items").meta_page) == table_before);
    VDB_CHECK(db.Table("items").live_rows == rows.size());
    Replace(db,root,index_before);
    Consistent(db,"items",rows);
    db.Close();
}

void BuildFailureUnpublished() {
    Files files;
    Database db(files.db,OpenMode::CreateNew,files.temp,1);
    db.CreateTable("items",TableSchema());
    const auto rid = db.Insert("items",Row(10));
    auto table = db.Table("items");
    const auto before = Copy(db,table.meta_page);
    const auto heap = Copy(db,rid.page_id);
    auto corrupt = heap;
    WriteU32(corrupt,28,table.meta_page+1); // Build fails after allocating its private tree.
    Replace(db,rid.page_id,corrupt);
    const auto allocated = db.Disk().PageCount(FileKind::Database);
    Throws([&] { db.CreateIndex("items",1); });
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == allocated+2);
    VDB_CHECK(Copy(db,table.meta_page) == before);
    VDB_CHECK(db.Table("items").index_col == UINT16_MAX);
    VDB_CHECK(db.Table("items").index_meta == kInvalidPageId);
    Throws([&] { db.Index("items"); });
    Replace(db,rid.page_id,heap); // Test-only repair of the deliberately injected corruption.
    db.Close();
    Database reopened(files.db,OpenMode::OpenExisting,files.temp,1);
    VDB_CHECK(reopened.Table("items").index_meta == kInvalidPageId);
    reopened.CreateIndex("items",1);
    Rows rows;
    rows.emplace(rid,Row(10));
    Consistent(reopened,"items",rows);
    reopened.Close();
}

void MetadataValidation() {
    Files files;
    PageId table_id,index_id;
    PageBytes table_bytes,index_bytes;
    {
        Database db(files.db,OpenMode::CreateNew,files.temp,1);
        db.CreateTable("items",TableSchema());
        db.CreateIndex("items",1);
        table_id = db.Table("items").meta_page;
        index_id = db.Table("items").index_meta;
        table_bytes = Copy(db,table_id);
        index_bytes = Copy(db,index_id);
        db.Close();
    }
    for (int corruption = 0; corruption < 5; ++corruption) {
        {
            DiskManager disk(files.db,OpenMode::OpenExisting,files.temp,OpenMode::OpenExisting);
            BufferPool pool(disk,1);
            auto page = pool.Fetch({FileKind::Database,corruption == 4 ? index_id : table_id});
            if (corruption == 0) WriteU16(page.MutableData(),36,UINT16_MAX);
            if (corruption == 1) WriteU32(page.MutableData(),40,kInvalidPageId);
            if (corruption == 2) WriteU16(page.MutableData(),36,2); // VARCHAR
            if (corruption == 3) WriteU16(page.MutableData(),36,8);
            if (corruption == 4) WriteU16(page.MutableData(),22,0); // Table says column 1.
            page.MarkDirty(); page.Drop(); pool.FlushAll(); disk.Close();
        }
        Throws([&] { Database db(files.db,OpenMode::OpenExisting,files.temp,1); });
        {
            DiskManager disk(files.db,OpenMode::OpenExisting,files.temp,OpenMode::OpenExisting);
            BufferPool pool(disk,1);
            auto page = pool.Fetch({FileKind::Database,table_id});
            page.MutableData() = table_bytes; page.MarkDirty(); page.Drop();
            page = pool.Fetch({FileKind::Database,index_id});
            page.MutableData() = index_bytes; page.MarkDirty(); page.Drop();
            pool.FlushAll(); disk.Close();
        }
    }
}

void RandomDatabase() {
    constexpr uint32_t seed = 5052026;
    std::cout << "database seed=" << seed << " operations=3000\n";
    Files files;
    Rows rows;
    PageId meta,root;
    uint16_t height;
    {
        Database db(files.db,OpenMode::CreateNew,files.temp,1);
        db.CreateTable("items",TableSchema());
        db.CreateIndex("items",1);
        std::mt19937 rng(seed);
        for (size_t step = 0; step < 3000; ++step) {
            const auto op = rng()%100;
            if (op < 46 || rows.empty()) {
                int64_t key = int64_t(rng()%41)-20;
                if (step%53 == 0) key = step%106 ? INT64_MIN : INT64_MAX;
                AddRow(db,rows,key,step);
            } else if (op < 80) {
                auto at = rows.begin(); std::advance(at,rng()%rows.size());
                DeleteRow(db,rows,at->first);
            } else {
                const int64_t low = int64_t(rng()%51)-25,high = int64_t(rng()%51)-25;
                const auto oracle = RowOracle(rows);
                VDB_CHECK(Drain(db,db.IndexRange("items",low,high)) == Expected(oracle,low,high));
                VDB_CHECK(Drain(db,db.IndexEqualRange("items",low)) == Expected(oracle,low,low));
            }
            if (step%43 == 0) Consistent(db,"items",rows);
            Pins(db);
        }
        Consistent(db,"items",rows);
        meta = db.Table("items").index_meta; root = db.Index("items").RootPage(); height = db.Index("items").Height();
        std::cout << "database final rows=" << rows.size() << " height=" << height << '\n';
        db.Close();
    }
    Database db(files.db,OpenMode::OpenExisting,files.temp,1);
    VDB_CHECK(db.Table("items").index_meta == meta && db.Index("items").RootPage() == root && db.Index("items").Height() == height);
    // Opening read only metadata/root, not hundreds of heap/index entries for a rebuild.
    VDB_CHECK(db.Metrics().new_pages == 0 && db.Metrics().buffer_fetches <= 8);
    Consistent(db,"items",rows);
    AddRow(db,rows,INT64_MIN);
    DeleteRow(db,rows,rows.begin()->first);
    Consistent(db,"items",rows);
    db.Close();
}

void Run(const char* name, const std::function<void()>& action) {
    std::cout << "[B+ tree delete] " << name << std::endl;
    action(); ++cases_passed;
}
} // namespace
int main() {
    try {
        Run("leaf borrow/merge both directions and clean reopen",LeafBranches);
        Run("internal rotations/merges both directions and clean reopen",InternalBranches);
        Run("stable fence, absent pair, unchanged bytes/epoch",StableFenceAndAbsent);
        Run("cascading contraction, empty/reinsert, append-only retirement",CascadeEmptyReinsert);
        Run("height 3 to 2 cascading contraction and reopen",CascadingRootReopen);
        Run("selective duplicates and INT64 extremes",DuplicatesExtremes);
        Run("production capacity borrow/merge/contraction",ProductionCapacity);
        Run("random insert/delete oracle",RandomOracle);
        Run("index before/after population and empty/indexed reopen",IndexLifecycle);
        Run("validation, foreign RID, missing-pair corruption",IndexRejectionsAndCorruption);
        Run("failed build never publishes metadata",BuildFailureUnpublished);
        Run("persisted index metadata validation",MetadataValidation);
        Run("random Database heap/index consistency",RandomDatabase);
        std::cout << "B+ tree delete passed: " << cases_passed << " groups; max height=" << maximum_height
                  << "; root contractions=" << contractions << "; peak pins=" << maximum_pins << "; active pins=0\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
