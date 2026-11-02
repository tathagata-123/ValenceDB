// Standalone Phase 3 comparison probe; build commands are in docs/phase3_refactor.md.
// TSV output is separate from the schema-v2 query benchmark stream.
#include "bplus_validator.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>

using namespace valencedb;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
    try {
        if (argc != 4) throw std::runtime_error("usage: probe frames rows leaf_capacity");
        const size_t frames = std::stoul(argv[1]), rows = std::stoul(argv[2]);
        const auto leaf_max = static_cast<uint16_t>(std::stoul(argv[3]));
        const BPlusCapacity capacity{leaf_max, uint16_t(leaf_max == 254 ? 204 : leaf_max)};
        const auto dir = std::filesystem::temp_directory_path() /
            ("valencedb_phase3_probe_" + std::to_string(Clock::now().time_since_epoch().count()));
        std::filesystem::create_directory(dir);
        {
            DiskManager disk(dir / "db", OpenMode::CreateNew, dir / "tmp", OpenMode::CreateNew);
            BufferPool pool(disk, frames);
            auto tree = BPlusTree::Create(pool, UINT16_MAX, capacity);
            std::vector<size_t> order(rows);
            std::iota(order.begin(), order.end(), 0);
            std::mt19937 random(5032026);
            std::shuffle(order.begin(), order.end(), random);
            // Duplicate logical keys; each physical key remains unique.
            const auto key = [](size_t i) { return BPlusKey{int64_t(i / 3), {PageId(i + 1), 0}}; };
            bplus_test::Oracle oracle;
            for (size_t i = 0; i < rows; ++i) oracle.insert(key(i));
            const std::vector<BPlusKey> expected(oracle.begin(), oracle.end());
            std::cout << "operation\tms\tfetches\treads\twrites\tallocations\tevictions\tpeak_pins\n";
            for (int operation = 0; operation < 4; ++operation) {
                const auto before = pool.metrics();
                const auto io = disk.counters();
                const auto begin = Clock::now();
                if (operation == 0) {
                    for (auto i : order) { auto e = key(i); VDB_CHECK(tree.Insert(e.key, e.rid)); }
                } else if (operation == 1) {
                    for (auto i : order) { auto e = key(i); VDB_CHECK(tree.Contains(e.key, e.rid)); }
                } else if (operation == 2) {
                    auto cursor = tree.Range(INT64_MIN, INT64_MAX);
                    size_t i = 0;
                    while (auto e = cursor.Next()) { VDB_CHECK(i < rows && *e == expected[i]); ++i; }
                    VDB_CHECK(i == rows);
                } else {
                    for (auto i : order) { auto e = key(i); VDB_CHECK(tree.Erase(e.key, e.rid)); }
                }
                pool.FlushAll(); // Include final dirty writes in each operation's counters/timing.
                const double ms = std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
                VDB_CHECK(pool.metrics().active_pins == 0 && pool.metrics().peak_pins == 1);
                const char* names[] = {"insert", "contains", "range", "erase"};
                std::cout << names[operation] << '\t' << ms << '\t'
                          << pool.metrics().buffer_fetches - before.buffer_fetches << '\t'
                          << disk.counters().database_reads - io.database_reads << '\t'
                          << disk.counters().database_writes - io.database_writes << '\t'
                          << disk.counters().database_allocations - io.database_allocations << '\t'
                          << pool.metrics().evictions - before.evictions << '\t'
                          << pool.metrics().peak_pins << '\n';
                // Independent raw-page validation is outside the timed operation.
                const auto shape = bplus_test::Validator(pool, disk.PageCount(FileKind::Database), tree).Run();
                VDB_CHECK(shape.entries == (operation == 3 ? std::vector<BPlusKey>{} : expected));
            }
            pool.FlushAll();
            disk.Close();
        }
        std::filesystem::remove_all(dir);
        return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
