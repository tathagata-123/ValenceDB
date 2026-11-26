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
struct Files {
    std::filesystem::path directory, db, temp;
    Files() {
        static unsigned serial = 0;
        directory = std::filesystem::temp_directory_path() / ("valencedb_index_integration_" +
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
TreeShape Check(Database& db, const BPlusTree& tree, const Oracle& oracle) {
    auto shape = Validator(db.Buffer(), db.Disk().PageCount(FileKind::Database), tree).Run();
    maximum_height = std::max(maximum_height, shape.height);
    VDB_CHECK(shape.entries == Expected(oracle));
    VDB_CHECK(Drain(db, tree.LowerBound(INT64_MIN)) == shape.entries);
    Pins(db);
    return shape;
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
void RejectDelete(Database& db, const Rows& rows, RID rid) {
    const auto key = rows.at(rid).value(1).as_int64();
    const auto epoch = db.Index("items").MutationEpoch();
    const auto heap_before = Copy(db, rid.page_id);
    const auto table_before = Copy(db, db.Table("items").meta_page);
    auto cursor = db.IndexEqualRange("items", key);
    auto scan = db.Scan("items");
    Throws([&] { db.Delete("items", rid); });
    VDB_CHECK(Copy(db, rid.page_id) == heap_before);
    VDB_CHECK(Copy(db, db.Table("items").meta_page) == table_before);
    VDB_CHECK(db.Index("items").MutationEpoch() == epoch);
    VDB_CHECK(db.Fetch("items", rid).has_value());
    VDB_CHECK(db.Index("items").Contains(key, rid));
    VDB_CHECK(Drain(db, cursor) == Expected(RowOracle(rows), key, key));
    size_t count = 0;
    while (scan.Next()) ++count; // Rejection also preserves heap cursor validity.
    VDB_CHECK(count == rows.size());
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
            if (!build_after) {
                VDB_CHECK(db.Index("items").Height() == 1);
                Throws([&] { db.Delete("items",{kInvalidPageId,0}); });
            }
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
            RejectDelete(db,rows,b);
            VDB_CHECK(db.Index("items").Contains(10,a) && db.Index("items").Contains(10,c));
            Consistent(db,"items",rows);
            VDB_CHECK(db.Index("items").Height() == 2);
            root = db.Index("items").RootPage();
            RejectDelete(db,rows,rows.begin()->first);
            Consistent(db,"items",rows);
            VDB_CHECK(db.Index("items").Height() == 2 && db.Index("items").RootPage() == root);
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

void IndexRejections() {
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
    Throws([&] { db.Delete("items",{kInvalidPageId,0}); });
    Throws([&] { db.Delete("items",{rows.begin()->first.page_id,UINT16_MAX}); });

    // Snapshot every persistent page to check rejection cannot change heap or index.
    std::vector<PageBytes> before;
    for (PageId id = 0; id < db.Disk().PageCount(FileKind::Database); ++id) before.push_back(Copy(db,id));
    RejectDelete(db,rows,rows.begin()->first);
    VDB_CHECK(db.Disk().PageCount(FileKind::Database) == before.size());
    for (PageId id = 0; id < before.size(); ++id) VDB_CHECK(Copy(db,id) == before[id]);
    // The non-indexed table still supports deletion, absent RIDs, and persistence.
    VDB_CHECK(db.Delete("foreign",foreign));
    VDB_CHECK(!db.Delete("foreign",foreign));
    VDB_CHECK(!db.Fetch("foreign",foreign));
    VDB_CHECK(db.Table("foreign").live_rows == 0);
    Consistent(db,"items",rows);
    db.Close();
    Database reopened(files.db,OpenMode::OpenExisting,files.temp,1);
    VDB_CHECK(!reopened.Fetch("foreign",foreign));
    VDB_CHECK(reopened.Table("foreign").live_rows == 0);
    RejectDelete(reopened,rows,rows.begin()->first);
    Consistent(reopened,"items",rows);
    reopened.Close();
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
                RejectDelete(db,rows,at->first);
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
    RejectDelete(db,rows,rows.begin()->first);
    Consistent(db,"items",rows);
    db.Close();
}

void Run(const char* name, const std::function<void()>& action) {
    std::cout << "[Index integration] " << name << std::endl;
    action(); ++cases_passed;
}
} // namespace
int main() {
    try {
        Run("index before/after population and empty/indexed reopen",IndexLifecycle);
        Run("index validation and deletion rejection before heap mutation",IndexRejections);
        Run("failed build never publishes metadata",BuildFailureUnpublished);
        Run("persisted index metadata validation",MetadataValidation);
        Run("random Database heap/index consistency",RandomDatabase);
        std::cout << "Index integration passed: " << cases_passed << " groups; max height=" << maximum_height
                  << "; peak pins=" << maximum_pins << "; active pins=0\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
