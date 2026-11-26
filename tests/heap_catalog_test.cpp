#include "test_harness.h"
#include "slotted_page_validator.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/database.hpp"
#include "valencedb/storage/slotted_page.hpp"

#include <filesystem>
#include <functional>
#include <iostream>
#include <random>

using namespace valencedb;

namespace {
struct Files {
    std::filesystem::path db;
    std::filesystem::path temp;
    explicit Files(const std::string& label) {
        static unsigned number = 0;
        const auto base = std::filesystem::temp_directory_path() /
                          ("valencedb_heap_" + label + "_" + std::to_string(++number));
        db = base.string() + ".db";
        temp = base.string() + ".tmp";
        std::filesystem::remove(db);
        std::filesystem::remove(temp);
    }
    ~Files() {
        std::filesystem::remove(db);
        std::filesystem::remove(temp);
    }
};

void Throws(const std::function<void()>& action) {
    static int check_number = 0;
    ++check_number;
    bool thrown = false;
    try { action(); } catch (const DbError&) { thrown = true; }
    if (!thrown) std::cerr << "expected DbError check " << check_number << " did not throw\n";
    VDB_CHECK(thrown);
}

Schema Schema1() { return Schema(SchemaKind::Base, {{"id", TypeId::Int64}}); }
Schema Schema2() {
    return Schema(SchemaKind::Base, {{"id", TypeId::Int64},
                                     {"tag", TypeId::Varchar, 256}});
}
Tuple Row(int64_t id, std::string tag) {
    return Tuple({Value(id), Value(std::move(tag))});
}
void CheckRow(const Tuple& row, int64_t id, const std::string& tag) {
    VDB_CHECK(row.value(0).as_int64() == id);
    VDB_CHECK(row.value(1).as_string() == tag);
}

void SlottedLayoutAndBounds() {
    PageBytes page{};
    SlottedPage::Initialize(page, 7, 5);
    test::ValidateSlottedPage(page, 7, 5);
    VDB_CHECK(ReadU16(page, 16) == 0);
    VDB_CHECK(ReadU16(page, 18) == 32);
    VDB_CHECK(ReadU16(page, 20) == 4096);
    VDB_CHECK(ReadU16(page, 22) == 0);
    VDB_CHECK(SlottedPage::Next(page) == kInvalidPageId);
    VDB_CHECK(ReadU32(page, 28) == 5);
    auto schema = Schema2();
    Throws([&] { (void)EncodeTuple(schema, Row(0, std::string(257, 'x'))); });
    for (int i = 0; i < 3; ++i) {
        const auto bytes = EncodeTuple(schema, Row(i, i == 2 ? std::string(256, 'z') : ""));
        VDB_CHECK(SlottedPage::Insert(page, bytes) == i);
        VDB_CHECK(ReadU16(page, 18) == 32 + 4 * (i + 1));
        CheckRow(DecodeTuple(schema, *SlottedPage::Fetch(page, i)), i,
                 i == 2 ? std::string(256, 'z') : "");
    }
    VDB_CHECK(SlottedPage::Erase(page, 1));
    VDB_CHECK(!SlottedPage::Erase(page, 1));
    VDB_CHECK(!SlottedPage::Fetch(page, 1));
    VDB_CHECK(!SlottedPage::Fetch(page, 400));
    VDB_CHECK(SlottedPage::Insert(page, EncodeTuple(schema, Row(4, "D"))) == 3);
    SlottedPage::Compact(page);
    test::ValidateSlottedPage(page, 7, 5);
    CheckRow(DecodeTuple(schema, *SlottedPage::Fetch(page, 2)), 2, std::string(256, 'z'));
    VDB_CHECK(SlottedPage::Erase(page, 0));
    VDB_CHECK(SlottedPage::Erase(page, 2));
    VDB_CHECK(SlottedPage::Erase(page, 3));
    VDB_CHECK(SlottedPage::LiveCount(page) == 0);
    test::ValidateSlottedPage(page, 7, 5);
    Throws([&] { SlottedPage::Validate(page, 7, 6); });

    PageBytes exact{};
    SlottedPage::Initialize(exact, 8, 5);
    std::vector<std::byte> fill(4096 - 32 - 4, std::byte{'x'});
    VDB_CHECK(SlottedPage::Insert(exact, fill) == 0);
    VDB_CHECK(!SlottedPage::Insert(exact, std::vector<std::byte>(1)));
    VDB_CHECK(ReadU16(exact, 20) == 36);
    PageBytes over{};
    SlottedPage::Initialize(over, 9, 5);
    VDB_CHECK(!SlottedPage::Insert(over, std::vector<std::byte>(fill.size() + 1)));
    test::ValidateSlottedPage(over, 9, 5);
    VDB_CHECK(SlottedPage::SlotCount(over) == 0);
}

void CorruptPages() {
    PageBytes good{};
    SlottedPage::Initialize(good, 3, 2);
    auto bytes = EncodeTuple(Schema1(), Tuple({Value(int64_t{9})}));
    VDB_CHECK(SlottedPage::Insert(good, bytes) == 0);
    auto bad = good;
    WriteU16(bad, 4, static_cast<uint16_t>(PageType::Catalog));
    Throws([&] { SlottedPage::Validate(bad, 3, 2); });
    bad = good; WriteU16(bad, 18, 31);
    Throws([&] { SlottedPage::Validate(bad, 3, 2); });
    bad = good; WriteU16(bad, 20, 5000);
    Throws([&] { SlottedPage::Validate(bad, 3, 2); });
    bad = good; WriteU16(bad, 32, 20);
    SlottedPage::Validate(bad, 3, 2); // Unused slots are no longer scanned here.
    Throws([&] { SlottedPage::Fetch(bad, 0); });
    Throws([&] { SlottedPage::Compact(bad); });
    Throws([&] { test::ValidateSlottedPage(bad, 3, 2); });
    bad = good; WriteU16(bad, 34, 20);
    Throws([&] { SlottedPage::Fetch(bad, 0); });
    Throws([&] { SlottedPage::Compact(bad); });
    Throws([&] { test::ValidateSlottedPage(bad, 3, 2); });
    bad = good; WriteU16(bad, ReadU16(bad, 32), 2);
    SlottedPage::Validate(bad, 3, 2);
    Throws([&] { DecodeTuple(Schema1(), *SlottedPage::Fetch(bad, 0)); });

    // Bounds are checked even for direct accesses without a prior Validate.
    for (const uint16_t offset : {uint16_t{4096}, uint16_t{65535}}) {
        bad = good; WriteU16(bad, 32, offset);
        Throws([&] { SlottedPage::Fetch(bad, 0); });
        Throws([&] { SlottedPage::Compact(bad); });
    }
    bad = good; WriteU16(bad, 16, UINT16_MAX);
    Throws([&] { SlottedPage::Fetch(bad, 0); });
    Throws([&] { SlottedPage::Insert(bad, bytes); });
    Throws([&] { SlottedPage::Erase(bad, 0); });
    Throws([&] { SlottedPage::Compact(bad); });
    bad = good; WriteU16(bad, 22, 0);
    SlottedPage::Validate(bad, 3, 2);
    Throws([&] { test::ValidateSlottedPage(bad, 3, 2); });
    Throws([&] { SlottedPage::Erase(bad, 0); });

    VDB_CHECK(SlottedPage::Insert(good, bytes) == 1);
    bad = good; WriteU16(bad, 36, ReadU16(bad, 32));
    SlottedPage::Validate(bad, 3, 2);
    Throws([&] { test::ValidateSlottedPage(bad, 3, 2); });

    // Both source ranges fit individually, but their sum cannot fit the page.
    WriteU16(bad, 20, 40);
    for (const size_t slot : {size_t{32}, size_t{36}}) {
        WriteU16(bad, slot, 40);
        WriteU16(bad, slot + 2, 3000);
    }
    Throws([&] { SlottedPage::Compact(bad); });
}

void RandomizedSlottedPages() {
    for (const unsigned seed : {17u, 930u, 2026u}) {
        PageBytes page{};
        SlottedPage::Initialize(page, 3, 2);
        std::mt19937 rng(seed);
        std::vector<std::optional<std::vector<std::byte>>> rows;
        for (size_t step = 0; step < 600; ++step) {
            if (rows.empty() || rng() % 3 == 0) {
                std::vector<std::byte> bytes(1 + rng() % 300, std::byte(rng() % 256));
                if (auto slot = SlottedPage::Insert(page, bytes)) {
                    VDB_CHECK(*slot == rows.size());
                    rows.push_back(bytes);
                }
            } else if (rng() % 2 == 0) {
                const auto slot = static_cast<uint16_t>(rng() % rows.size());
                VDB_CHECK(SlottedPage::Erase(page, slot) == rows[slot].has_value());
                rows[slot].reset();
            } else {
                SlottedPage::Compact(page);
            }
            test::ValidateSlottedPage(page, 3, 2);
            for (size_t slot = 0; slot < rows.size(); ++slot)
                VDB_CHECK(SlottedPage::Fetch(page, static_cast<uint16_t>(slot)) == rows[slot]);
        }
    }
}

void DatabaseRoundTrip() {
    Files files("roundtrip");
    std::vector<RID> rids;
    {
        Database db(files.db, OpenMode::CreateNew, files.temp, 2);
        VDB_CHECK(db.Disk().PageCount(FileKind::Database) == 2);
        auto super = db.Buffer().Fetch({FileKind::Database, 0});
        ValidatePageHeader(super.Data(), PageType::Superblock, 0);
        VDB_CHECK(ReadU32(super.Data(), 16) == 4096 && ReadU32(super.Data(), 20) == 1);
        super.Drop();
        auto directory = db.Buffer().Fetch({FileKind::Database, 1});
        ValidatePageHeader(directory.Data(), PageType::Catalog, 1);
        VDB_CHECK(ReadU16(directory.Data(), 16) == 0);
        directory.Drop();
        auto empty = db.CreateTable("empty", Schema1());
        VDB_CHECK(empty.first_page == kInvalidPageId && empty.heap_pages == 0);
        auto table = db.CreateTable("rows", Schema2());
        VDB_CHECK(table.meta_page != empty.meta_page);
        for (int i = 0; i < 140; ++i)
            rids.push_back(db.Insert("rows", Row(i, std::string(80, char('a' + i % 26)))));
        VDB_CHECK(db.Table("rows").heap_pages > 2);
        VDB_CHECK(db.Table("rows").live_rows == 140);
        {
            auto info = db.Table("rows");
            PageId page_id = info.first_page;
            uint32_t pages = 0;
            while (page_id != kInvalidPageId) {
                auto page = db.Buffer().Fetch({FileKind::Database, page_id});
                test::ValidateSlottedPage(page.Data(), page_id, info.meta_page);
                const PageId next = SlottedPage::Next(page.Data());
                ++pages;
                if (next == kInvalidPageId) VDB_CHECK(page_id == info.last_page);
                page_id = next;
                VDB_CHECK(pages <= info.heap_pages);
            }
            VDB_CHECK(pages == info.heap_pages);
        }
        for (int i = 0; i < 140; i += 7) VDB_CHECK(db.Delete("rows", rids[i]));
        VDB_CHECK(!db.Delete("rows", rids[0]));
        VDB_CHECK(!db.Fetch("rows", rids[0]));
        VDB_CHECK(!db.Fetch("rows", RID{kInvalidPageId, 0}));
        VDB_CHECK(!db.Fetch("rows", RID{rids[1].page_id, UINT16_MAX}));
        VDB_CHECK(db.Metrics().active_pins == 0);
        auto scan = db.Scan("rows");
        VDB_CHECK(scan.Next().has_value());
        scan.Reset();
        size_t seen = 0;
        while (auto row = scan.Next()) {
            const int64_t id = row->second.value(0).as_int64();
            VDB_CHECK(id % 7 != 0);
            VDB_CHECK(row->first == rids[id]);
            ++seen;
        }
        VDB_CHECK(seen == 120);
        VDB_CHECK(db.Metrics().active_pins == 0);
        auto foreign = db.Insert("empty", Tuple({Value(int64_t{1000})}));
        Throws([&] { (void)db.Fetch("rows", foreign); });
        Throws([&] { (void)db.Delete("rows", foreign); });
        Throws([&] { (void)db.Delete("rows", {foreign.page_id, UINT16_MAX}); });
        VDB_CHECK(db.Fetch("empty", foreign).has_value());
        Throws([&] { (void)scan.Next(); });
        VDB_CHECK(db.Metrics().active_pins == 0);
        db.Flush();
        db.Close();
    }
    {
        Database db(files.db, OpenMode::OpenExisting, files.temp, 2);
        VDB_CHECK(db.TableNames().size() == 2);
        VDB_CHECK(db.Table("rows").live_rows == 120);
        VDB_CHECK(db.Table("rows").heap_pages > 2);
        VDB_CHECK(db.Table("rows").first_page == rids.front().page_id);
        VDB_CHECK(db.Table("rows").last_page == rids.back().page_id);
        VDB_CHECK(db.Table("rows").schema.column(1).varchar_limit == 256);
        for (int i = 0; i < 140; ++i) {
            auto row = db.Fetch("rows", rids[i]);
            VDB_CHECK(row.has_value() == (i % 7 != 0));
            if (row) CheckRow(*row, i, std::string(80, char('a' + i % 26)));
        }
        VDB_CHECK(db.Metrics().active_pins == 0);
        db.Close();
    }
}

void CatalogValidation() {
    Files files("catalog");
    Database db(files.db, OpenMode::CreateNew, files.temp, 2);
    Throws([&] { db.CreateTable("", Schema1()); });
    Throws([&] { db.CreateTable(std::string(32, 'x'), Schema1()); });
    Throws([&] { db.CreateTable("bad", Schema(SchemaKind::Base,
        {{std::string(24, 'x'), TypeId::Int64}})); });
    VDB_CHECK(db.TableNames().empty());
    db.CreateTable(std::string(31, 't'), Schema(SchemaKind::Base,
        {{std::string(23, 'c'), TypeId::Int64}}));
    Throws([&] { db.CreateTable(std::string(31, 't'), Schema1()); });
    for (int i = 1; i < 16; ++i) db.CreateTable("t" + std::to_string(i), Schema1());
    Throws([&] { db.CreateTable("seventeen", Schema1()); });
    VDB_CHECK(db.TableNames().size() == 16);
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == 18);
    VDB_CHECK(db.Metrics().active_pins == 0);
    db.Close();
    Database reopened(files.db, OpenMode::OpenExisting, files.temp, 2);
    VDB_CHECK(reopened.TableNames().size() == 16);
    VDB_CHECK(reopened.Table(std::string(31, 't')).schema.column(0).name ==
              std::string(23, 'c'));
    VDB_CHECK(reopened.Metrics().active_pins == 0);
    reopened.Close();
}

void CompactionAndDirectoryExhaustion() {
    PageBytes page{};
    SlottedPage::Initialize(page, 4, 2);
    std::vector<std::byte> large(1000, std::byte{'a'});
    for (int i = 0; i < 4; ++i) VDB_CHECK(SlottedPage::Insert(page, large) == i);
    VDB_CHECK(SlottedPage::Erase(page, 1));
    VDB_CHECK(!SlottedPage::CanFit(page, 500));
    VDB_CHECK(SlottedPage::Insert(page, std::vector<std::byte>(500)) == 4);
    test::ValidateSlottedPage(page, 4, 2);
    VDB_CHECK(!SlottedPage::Fetch(page, 1));
    VDB_CHECK(SlottedPage::Fetch(page, 2)->size() == 1000);
    PageBytes tiny{};
    SlottedPage::Initialize(tiny, 5, 2);
    std::vector<std::byte> one(1, std::byte{'b'});
    size_t count = 0;
    while (auto slot = SlottedPage::Insert(tiny, one)) {
        VDB_CHECK(*slot == count++);
        VDB_CHECK(SlottedPage::Erase(tiny, *slot));
    }
    VDB_CHECK(count == (4096 - 32) / 4 - 1);
    test::ValidateSlottedPage(tiny, 5, 2);
    VDB_CHECK(!SlottedPage::Insert(tiny, one));
}

void HeapDirectoryExhaustion() {
    Files files("slots");
    Database db(files.db, OpenMode::CreateNew, files.temp, 2);
    db.CreateTable("t", Schema1());
    const Tuple value({Value(int64_t{1})});
    RID previous = db.Insert("t", value);
    const PageId first = previous.page_id;
    VDB_CHECK(db.Delete("t", previous));
    size_t attempts = 1;
    while (db.Table("t").heap_pages == 1) {
        previous = db.Insert("t", value);
        ++attempts;
        if (previous.page_id == first) VDB_CHECK(db.Delete("t", previous));
        VDB_CHECK(attempts <= 1100);
    }
    VDB_CHECK(previous.page_id != first);
    VDB_CHECK(db.Table("t").heap_pages == 2);
    VDB_CHECK(db.Table("t").live_rows == 1);
    VDB_CHECK(db.Metrics().active_pins == 0);
    db.Close();
}

void CrossOwnerAndFlushFailure() {
    Files files("owner");
    Database db(files.db, OpenMode::CreateNew, files.temp, 2);
    db.CreateTable("a", Schema1());
    db.CreateTable("b", Schema1());
    auto a = db.Insert("a", Tuple({Value(int64_t{1})}));
    auto b = db.Insert("b", Tuple({Value(int64_t{2})}));
    {
        auto page = db.Buffer().Fetch({FileKind::Database, a.page_id});
        SlottedPage::SetNext(page.MutableData(), b.page_id);
        page.MarkDirty();
    }
    auto scan = db.Scan("a");
    VDB_CHECK(scan.Next().has_value());
    Throws([&] { (void)scan.Next(); });
    VDB_CHECK(db.Metrics().active_pins == 0);
    db.Disk().FailNextIo(FailNext::Write);
    Throws([&] { db.Flush(); });
    db.Flush();
    db.Close();
}

void RandomizedRows() {
    Files files("random");
    Database db(files.db, OpenMode::CreateNew, files.temp, 3);
    db.CreateTable("r", Schema2());
    std::mt19937 rng(20260930);
    std::vector<RID> ids;
    std::vector<bool> alive;
    for (int step = 0; step < 500; ++step) {
        if (ids.empty() || rng() % 3 != 0) {
            const int id = ids.size();
            ids.push_back(db.Insert("r", Row(id, std::string(rng() % 257, 'q'))));
            alive.push_back(true);
        } else {
            const size_t index = rng() % ids.size();
            VDB_CHECK(db.Delete("r", ids[index]) == alive[index]);
            alive[index] = false;
        }
    }
    size_t expected = 0;
    for (size_t i = 0; i < ids.size(); ++i) {
        auto row = db.Fetch("r", ids[i]);
        VDB_CHECK(row.has_value() == alive[i]);
        if (row) VDB_CHECK(row->value(0).as_int64() == static_cast<int64_t>(i));
        expected += alive[i];
    }
    size_t scanned = 0;
    auto scan = db.Scan("r");
    while (scan.Next()) ++scanned;
    VDB_CHECK(scanned == expected && db.Table("r").live_rows == expected);
    VDB_CHECK(db.Metrics().active_pins == 0);
    db.Close();
}
}

int main() {
    try {
        SlottedLayoutAndBounds();
        CorruptPages();
        RandomizedSlottedPages();
        CompactionAndDirectoryExhaustion();
        HeapDirectoryExhaustion();
        DatabaseRoundTrip();
        CatalogValidation();
        CrossOwnerAndFlushFailure();
        RandomizedRows();
        std::cout << "heap and catalog tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
