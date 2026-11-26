#include "test_harness.h"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/disk_manager.hpp"
#include "valencedb/types/tuple.hpp"

#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

using namespace valencedb;

template <class F>
void Reject(F action) {
    bool rejected = false;
    try { action(); } catch (const DbError&) { rejected = true; }
    VDB_CHECK(rejected);
}

std::vector<Column> IntColumns(size_t count) {
    std::vector<Column> columns;
    for (size_t i = 0; i < count; ++i)
        columns.push_back({"c" + std::to_string(i), TypeId::Int64});
    return columns;
}

void TestSchema() {
    Schema one(SchemaKind::Base, IntColumns(1));
    VDB_CHECK(one.size() == 1);
    VDB_CHECK(Schema(SchemaKind::Base, IntColumns(8)).size() == 8);
    VDB_CHECK(Schema(SchemaKind::Intermediate, IntColumns(16)).size() == 16);
    Reject([] { Schema(SchemaKind::Base, {}); });
    Reject([] { Schema(SchemaKind::Base, IntColumns(9)); });
    Reject([] { Schema(SchemaKind::Intermediate, IntColumns(17)); });
    VDB_CHECK(Schema(SchemaKind::Base, {{"s", TypeId::Varchar, 256}}).size() == 1);
    Reject([] { Schema(SchemaKind::Base, {{"s", TypeId::Varchar, 257}}); });
    Reject([] { Schema(SchemaKind::Base, {{"d", TypeId::Double}}); });
    Reject([] { Schema(SchemaKind::Base, {{"x", TypeId::Int64, 0, true}}); });
    VDB_CHECK(Schema(SchemaKind::Intermediate, {{"d", TypeId::Double, 0, true}}).size() == 1);
    Reject([] { Schema(SchemaKind::Base, {{"a", TypeId::Varchar, 256},
                                      {"b", TypeId::Varchar, 256},
                                      {"c", TypeId::Varchar, 256},
                                      {"d", TypeId::Varchar, 256}}); });
}

void TestTuple() {
    Schema ints(SchemaKind::Base, {{"n", TypeId::Int64}});
    const std::array<int64_t, 5> numbers{0, 1, -1,
        std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
    for (int64_t n : numbers) {
        auto bytes = EncodeTuple(ints, Tuple({Value(n)}));
        VDB_CHECK(bytes.size() == 12);
        VDB_CHECK(DecodeTuple(ints, bytes).value(0).as_int64() == n);
    }
    VDB_CHECK(EncodeTuple(ints, Tuple({Value(int64_t(1))})) ==
              (std::vector<std::byte>{std::byte{1}, std::byte{0}, std::byte{0},
                std::byte{0}, std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0},
                std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}}));
    auto minus_one = EncodeTuple(ints, Tuple({Value(int64_t(-1))}));
    for (size_t i = 4; i < 12; ++i) VDB_CHECK(minus_one[i] == std::byte{0xff});
    auto minimum = EncodeTuple(ints, Tuple({Value(std::numeric_limits<int64_t>::min())}));
    for (size_t i = 4; i < 11; ++i) VDB_CHECK(minimum[i] == std::byte{0});
    VDB_CHECK(minimum[11] == std::byte{0x80});

    Schema strings(SchemaKind::Base, {{"s", TypeId::Varchar, 256}});
    for (const std::string& s : {std::string(), std::string("hello"),
                                  std::string(256, 'z'), std::string("a\0b", 3),
                                  std::string("\xc3\xa9\xff", 3)}) {
        auto encoded = EncodeTuple(strings, Tuple({Value(s)}));
        VDB_CHECK(encoded.size() == 6 + s.size());
        VDB_CHECK(DecodeTuple(strings, encoded).value(0).as_string() == s);
    }
    Reject([&] { EncodeTuple(strings, Tuple({Value(std::string(257, 'x'))})); });

    Schema result(SchemaKind::Intermediate,
                  {{"d", TypeId::Double, 0, true}, {"n", TypeId::Int64, 0, true}});
    for (double d : {0.0, -0.0, 1.5, -123.25, 1.0 / 3.0}) {
        auto decoded = DecodeTuple(result, EncodeTuple(result, Tuple({Value(d), Value(int64_t(7))})));
        VDB_CHECK(std::bit_cast<uint64_t>(decoded.value(0).as_double()) ==
                  std::bit_cast<uint64_t>(d));
    }
    auto null_bytes = EncodeTuple(result, Tuple({Value::Null(TypeId::Double), Value(int64_t(9))}));
    VDB_CHECK(null_bytes[2] == std::byte{1});
    VDB_CHECK(DecodeTuple(result, null_bytes).value(0).is_null());
    Reject([&] { EncodeTuple(ints, Tuple({Value::Null(TypeId::Int64)})); });
    Reject([&] { EncodeTuple(ints, Tuple({})); });
    Reject([&] { EncodeTuple(ints, Tuple({Value(1.5)})); });

    auto good_int = EncodeTuple(ints, Tuple({Value(int64_t(4))}));
    auto malformed = good_int;
    malformed[2] = std::byte{2};
    Reject([&] { DecodeTuple(ints, malformed); });
    malformed = good_int;
    malformed[0] = std::byte{2};
    Reject([&] { DecodeTuple(ints, malformed); });
    malformed = good_int;
    malformed.pop_back();
    Reject([&] { DecodeTuple(ints, malformed); });
    malformed = good_int;
    malformed.push_back(std::byte{0});
    Reject([&] { DecodeTuple(ints, malformed); });
    Reject([&] { DecodeTuple(ints, std::span<const std::byte>{}); });
    auto good_double = EncodeTuple(result, Tuple({Value(1.25), Value::Null(TypeId::Int64)}));
    good_double.pop_back();
    Reject([&] { DecodeTuple(result, good_double); });
    auto good_string = EncodeTuple(strings, Tuple({Value(std::string("abc"))}));
    Reject([&] { DecodeTuple(strings, std::span<const std::byte>(good_string).first(5)); });
    good_string.pop_back();
    Reject([&] { DecodeTuple(strings, good_string); });
    auto huge = std::vector<std::byte>(1025);
    Reject([&] { DecodeTuple(ints, huge); });
    VDB_CHECK(std::numeric_limits<double>::is_iec559 && sizeof(double) == 8);
}

void TestPageCodec() {
    std::array<std::byte, 8> integer_bytes{};
    WriteU64(integer_bytes, 0, 0x0123456789abcdefULL);
    VDB_CHECK(integer_bytes == (std::array<std::byte, 8>{std::byte{0xef},
        std::byte{0xcd}, std::byte{0xab}, std::byte{0x89}, std::byte{0x67},
        std::byte{0x45}, std::byte{0x23}, std::byte{0x01}}));
    VDB_CHECK(ReadU64(integer_bytes, 0) == 0x0123456789abcdefULL);
    WriteDouble(integer_bytes, 0, 1.5);
    VDB_CHECK(ReadU64(integer_bytes, 0) == 0x3ff8000000000000ULL);
    VDB_CHECK(ReadDouble(integer_bytes, 0) == 1.5);

    std::array<std::byte, 8> rid_bytes{};
    WriteRID(rid_bytes, 0, RID{0x12345678, 0xabcd});
    VDB_CHECK(rid_bytes == (std::array<std::byte, 8>{std::byte{0x78}, std::byte{0x56},
        std::byte{0x34}, std::byte{0x12}, std::byte{0xcd}, std::byte{0xab},
        std::byte{0}, std::byte{0}}));
    VDB_CHECK(ReadRID(rid_bytes, 0) == (RID{0x12345678, 0xabcd}));
    VDB_CHECK((RID{1, 99} < RID{2, 0}));
    VDB_CHECK((RID{1, 1} < RID{1, 2}));
    rid_bytes[7] = std::byte{1};
    Reject([&] { ReadRID(rid_bytes, 0); });
    Reject([&] { WriteRID(std::span<std::byte>(rid_bytes).first(7), 0, RID{1, 1}); });

    VDB_CHECK(!(PageRef{FileKind::Database, 5} == PageRef{FileKind::Temporary, 5}));
    std::unordered_map<PageRef, int, PageRefHash> pages;
    pages[{FileKind::Database, 5}] = 1;
    pages[{FileKind::Temporary, 5}] = 2;
    VDB_CHECK(pages.size() == 2);

    PageBytes page{};
    WritePageHeader(page, PageType::Heap, 0x12345678);
    VDB_CHECK(page[0] == std::byte{0x56} && page[1] == std::byte{0x44} &&
              page[2] == std::byte{0x42} && page[3] == std::byte{0x50});
    VDB_CHECK(page[4] == std::byte{4} && page[5] == std::byte{0});
    VDB_CHECK(page[6] == std::byte{1} && page[7] == std::byte{0});
    VDB_CHECK(page[8] == std::byte{0x78} && page[11] == std::byte{0x12});
    ValidatePageHeader(page, PageType::Heap, 0x12345678);
    Reject([&] { ValidatePageHeader(page, PageType::Catalog, 0x12345678); });
    Reject([&] { ValidatePageHeader(page, PageType::Heap, 3); });
    page[0] = std::byte{0};
    Reject([&] { ValidatePageHeader(page, PageType::Heap, 0x12345678); });
    page[0] = std::byte{0x56};
    page[6] = std::byte{2};
    Reject([&] { ValidatePageHeader(page, PageType::Heap, 0x12345678); });
    page[6] = std::byte{1};
    page[12] = std::byte{1};
    Reject([&] { ValidatePageHeader(page, PageType::Heap, 0x12345678); });
    Reject([&] { ValidatePageHeader(std::span<const std::byte>(page).first(15),
                                    PageType::Heap, 0x12345678); });
    Reject([&] { WriteU64(std::span<std::byte>(page).first(4), 0, 5); });
}

struct TestDirectory {
    std::filesystem::path path;
    TestDirectory() {
        auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        path = std::filesystem::temp_directory_path() /
               ("valencedb_storage_" + std::to_string(ticks));
        VDB_CHECK(std::filesystem::create_directory(path));
    }
    ~TestDirectory() { std::error_code error; std::filesystem::remove_all(path, error); }
};

void TestDisk() {
    TestDirectory dir;
    const auto db = dir.path / "db.pages";
    const auto tmp = dir.path / "tmp.pages";
    PageBytes a{};
    PageBytes b{};
    for (size_t i = 0; i < kPageSize; ++i) {
        a[i] = std::byte(i % 251);
        b[i] = std::byte((i + 17) % 251);
    }
    {
        DiskManager disk(db, OpenMode::CreateNew, tmp, OpenMode::CreateNew);
        VDB_CHECK(disk.Allocate(FileKind::Database) == 0);
        VDB_CHECK(disk.Allocate(FileKind::Temporary) == 0);
        VDB_CHECK(disk.Allocate(FileKind::Database) == 1);
        VDB_CHECK(std::filesystem::file_size(db) == 2 * kPageSize);
        PageBytes read{};
        disk.Read({FileKind::Database, 1}, read);
        VDB_CHECK(read == PageBytes{});
        disk.Write({FileKind::Database, 0}, a);
        disk.Write({FileKind::Temporary, 0}, b);
        disk.Read({FileKind::Database, 0}, read);
        VDB_CHECK(read == a);
        disk.Read({FileKind::Temporary, 0}, read);
        VDB_CHECK(read == b);
        Reject([&] { disk.Read({FileKind::Database, kInvalidPageId}, read); });
        Reject([&] { disk.Read({FileKind::Database, 2}, read); });
        Reject([&] { disk.Write({FileKind::Temporary, 2}, a); });
        disk.FailNextIo(FailNext::Read);
        Reject([&] { disk.Read({FileKind::Database, 0}, read); });
        disk.Read({FileKind::Database, 0}, read);
        VDB_CHECK(read == a);
        disk.FailNextIo(FailNext::Write);
        Reject([&] { disk.Write({FileKind::Database, 0}, b); });
        disk.Read({FileKind::Database, 0}, read);
        VDB_CHECK(read == a);
        disk.Write({FileKind::Database, 0}, a);
        VDB_CHECK(disk.counters().database_allocations == 2);
        VDB_CHECK(disk.counters().temporary_allocations == 1);
        VDB_CHECK(disk.counters().database_reads == 4);
        VDB_CHECK(disk.counters().temporary_reads == 1);
        VDB_CHECK(disk.counters().database_writes == 2);
        VDB_CHECK(disk.counters().temporary_writes == 1);
        disk.FlushAll();
        disk.Close();
        Reject([&] { disk.Read({FileKind::Database, 0}, read); });
    }
    Reject([&] { DiskManager(db, OpenMode::CreateNew, tmp, OpenMode::OpenExisting); });
    {
        DiskManager disk(db, OpenMode::OpenExisting, tmp, OpenMode::OpenExisting);
        VDB_CHECK(disk.PageCount(FileKind::Database) == 2);
        VDB_CHECK(disk.PageCount(FileKind::Temporary) == 1);
        PageBytes read{};
        disk.Read({FileKind::Database, 0}, read);
        VDB_CHECK(read == a);
        disk.Read({FileKind::Temporary, 0}, read);
        VDB_CHECK(read == b);
        disk.ResetTemporary();
        VDB_CHECK(disk.PageCount(FileKind::Temporary) == 0);
        VDB_CHECK(std::filesystem::file_size(tmp) == 0);
        VDB_CHECK(disk.Allocate(FileKind::Temporary) == 0);
        disk.Read({FileKind::Temporary, 0}, read);
        VDB_CHECK(read == PageBytes{});
        disk.Close();
    }
    const auto corrupt = dir.path / "corrupt.pages";
    { std::ofstream stream(corrupt, std::ios::binary); stream.put('x'); }
    Reject([&] { DiskManager(corrupt, OpenMode::OpenExisting, tmp, OpenMode::OpenExisting); });
    VDB_CHECK(std::filesystem::file_size(corrupt) == 1);
    Reject([&] { DiskManager(db, OpenMode::OpenExisting, db, OpenMode::OpenExisting); });
}

void TestPhysicalIoErrors() {
    TestDirectory dir;
    const auto db = dir.path / "db.pages";
    const auto tmp = dir.path / "tmp.pages";
    {
        DiskManager disk(db, OpenMode::CreateNew, tmp, OpenMode::CreateNew);
        disk.Allocate(FileKind::Database);
        disk.Allocate(FileKind::Database);
        disk.Close();
    }
    {
        DiskManager disk(db, OpenMode::OpenExisting, tmp, OpenMode::OpenExisting);
        // A real short transfer must throw, even though the page ID was allocated.
        std::filesystem::resize_file(db, kPageSize + 17);
        PageBytes bytes{};
        Reject([&] { disk.Read({FileKind::Database, 1}, bytes); });
        VDB_CHECK(disk.counters().database_reads == 0);
        // Abandon the instance; failed-read output and retry are not guaranteed.
    }
    std::filesystem::resize_file(db, kPageSize);
    {
        DiskManager disk(db, OpenMode::OpenExisting, tmp, OpenMode::OpenExisting);
        std::filesystem::rename(tmp, dir.path / "moved.pages");
        bool failed = false;
        try { disk.ResetTemporary(); }
        catch (const std::filesystem::filesystem_error&) { failed = true; }
        VDB_CHECK(failed); // Native filesystem errors propagate without translation.
    }
}

int main() {
    try {
        TestSchema();
        TestTuple();
        TestPageCodec();
        TestDisk();
        TestPhysicalIoErrors();
        std::cout << "storage primitives tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
