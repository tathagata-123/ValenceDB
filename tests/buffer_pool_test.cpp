#include "test_harness.h"
#include "valencedb/buffer/buffer_pool.hpp"
#include "valencedb/common/error.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>

using namespace valencedb;

template <class Action>
void Reject(Action action) {
    bool failed = false;
    try { action(); } catch (const DbError&) { failed = true; }
    VDB_CHECK(failed);
}

struct Fixture {
    std::filesystem::path path;
    DiskManager disk;

    static std::filesystem::path MakePath() {
        static uint64_t serial = 0;
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        auto path = std::filesystem::temp_directory_path() /
                    ("valencedb_buffer_" + std::to_string(ticks) + "_" + std::to_string(++serial));
        VDB_CHECK(std::filesystem::create_directory(path));
        return path;
    }

    Fixture() : path(MakePath()),
                disk(path / "db.pages", OpenMode::CreateNew,
                     path / "tmp.pages", OpenMode::CreateNew) {}
    ~Fixture() {
        try { disk.Close(); } catch (const DbError&) {}
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }

    PageRef Add(FileKind kind, uint8_t marker) {
        PageRef page{kind, disk.Allocate(kind)};
        PageBytes bytes{};
        bytes[0] = std::byte(marker);
        disk.Write(page, bytes);  // Fixture setup, before BufferPool owns the page.
        return page;
    }
};

size_t Index(const BufferPool& pool, PageRef page) {
    auto frames = pool.InspectFrames();
    size_t found = frames.size();
    for (size_t i = 0; i < frames.size(); ++i) {
        if (frames[i].page == page) {
            VDB_CHECK(found == frames.size());
            found = i;
        }
    }
    return found;
}

FrameSnapshot State(const BufferPool& pool, PageRef page) {
    const size_t index = Index(pool, page);
    VDB_CHECK(index < pool.FrameCount());
    return pool.InspectFrames()[index];
}

void NoPins(const BufferPool& pool) {
    VDB_CHECK(pool.metrics().active_pins == 0);
    for (const auto& frame : pool.InspectFrames()) VDB_CHECK(frame.pin_count == 0);
}

void TestHitsUsageAndGuards() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 0x31);
    BufferPool pool(f.disk, 2);
    {
        auto first = pool.Fetch(a);
        VDB_CHECK(first.Page() == a && first.Data()[0] == std::byte{0x31});
        VDB_CHECK(State(pool, a).usage_count == 1);
        VDB_CHECK(f.disk.counters().database_reads == 1);
        auto second = pool.Fetch(a);
        VDB_CHECK(Index(pool, a) == 0);
        VDB_CHECK(State(pool, a).pin_count == 2);
        VDB_CHECK(State(pool, a).usage_count == 2);
        VDB_CHECK(f.disk.counters().database_reads == 1);
        PageGuard moved(std::move(second));
        VDB_CHECK(!second && moved);
        moved.Drop();
        moved.Drop();
        VDB_CHECK(State(pool, a).pin_count == 1);
        PageGuard replacement = pool.Fetch(a);
        VDB_CHECK(State(pool, a).usage_count == 3);
        replacement = std::move(first);  // Releases its previous pin first.
        VDB_CHECK(!first && State(pool, a).pin_count == 1);
        { auto hit = pool.Fetch(a); VDB_CHECK(State(pool, a).usage_count == 4); }
        { auto hit = pool.Fetch(a); VDB_CHECK(State(pool, a).usage_count == 5); }
        for (int i = 0; i < 6; ++i) { auto hit = pool.Fetch(a); }
        VDB_CHECK(State(pool, a).usage_count == 5);
        VDB_CHECK(State(pool, a).pin_count == 1);
    }
    NoPins(pool);
    VDB_CHECK(pool.metrics().buffer_fetches == 11);
    VDB_CHECK(pool.metrics().buffer_hits == 10);
    VDB_CHECK(pool.metrics().buffer_misses == 1);
    VDB_CHECK(pool.metrics().peak_pins == 2);
    VDB_CHECK(f.disk.counters().database_reads == 1);
    { auto scan = pool.Fetch(a, AccessMode::Scan); VDB_CHECK(State(pool, a).usage_count == 5); }
    NoPins(pool);
}

PageGuard EarlyReturn(BufferPool& pool, PageRef page) {
    auto first = pool.Fetch(page);
    return first;
}

void TestScanHitsAndClock() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 1);
    PageRef b = f.Add(FileKind::Database, 2);
    PageRef c = f.Add(FileKind::Database, 3);
    PageRef d = f.Add(FileKind::Database, 4);
    BufferPool pool(f.disk, 3);
    { auto g = EarlyReturn(pool, a); }
    { auto g = pool.Fetch(a); }
    { auto g = pool.Fetch(b); }
    { auto g = pool.Fetch(c); }
    VDB_CHECK(State(pool, a).usage_count == 2);
    VDB_CHECK(pool.SweepHand() == 0);
    { auto g = pool.Fetch(d); }
    VDB_CHECK(Index(pool, b) == pool.FrameCount());
    VDB_CHECK(State(pool, a).usage_count == 0);
    VDB_CHECK(State(pool, c).usage_count == 0);
    VDB_CHECK(pool.SweepHand() == 2);
    VDB_CHECK(pool.metrics().sweep_inspections == 5);
    VDB_CHECK(pool.metrics().sweep_decrements == 4);
    { auto g = pool.Fetch(a, AccessMode::Scan); VDB_CHECK(State(pool, a).usage_count == 1); }
    { auto g = pool.Fetch(a, AccessMode::Scan); VDB_CHECK(State(pool, a).usage_count == 1); }
    { auto g = pool.Fetch(d, AccessMode::Normal); }
    VDB_CHECK(State(pool, d).usage_count == 2);
    { auto g = pool.Fetch(d, AccessMode::Scan); VDB_CHECK(State(pool, d).usage_count == 2); }
    NoPins(pool);

    BufferPool repeated(f.disk, 2);
    { auto g = repeated.Fetch(a); }
    { auto g = repeated.Fetch(b); }
    { auto g = repeated.Fetch(c); }
    VDB_CHECK(repeated.metrics().sweep_inspections == 3);
    VDB_CHECK(repeated.metrics().sweep_decrements == 2);
    NoPins(repeated);
}

void TestPinnedAndOneFrame() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 1);
    PageRef b = f.Add(FileKind::Database, 2);
    PageRef c = f.Add(FileKind::Database, 3);
    BufferPool pool(f.disk, 2);
    auto pinned = pool.Fetch(a);
    auto second = pool.Fetch(b);
    Reject([&] { pool.Fetch(c); });
    VDB_CHECK(State(pool, a).usage_count == 1);
    VDB_CHECK(State(pool, b).usage_count == 1);
    second.Drop();
    { auto g = pool.Fetch(c); VDB_CHECK(Index(pool, a) < 2); }
    VDB_CHECK(State(pool, a).usage_count == 1);
    pinned.Drop();
    NoPins(pool);

    BufferPool one(f.disk, 1);
    auto first = one.Fetch(a);
    Reject([&] { one.Fetch(b); });
    first.MutableData()[1] = std::byte{0x77};
    first.MarkDirty();
    first.Drop();
    { auto g = one.Fetch(b); }
    VDB_CHECK(one.metrics().dirty_evictions == 1);
    { auto g = one.Fetch(a); VDB_CHECK(g.Data()[1] == std::byte{0x77}); }
    NoPins(one);
}

void TestFlushInvalidateAndFailures() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 1);
    PageRef b = f.Add(FileKind::Database, 2);
    PageRef c = f.Add(FileKind::Database, 3);
    BufferPool pool(f.disk, 2);
    auto pinned = pool.Fetch(a);
    pinned.MutableData()[1] = std::byte{0x41};
    pinned.MarkDirty();
    pool.Flush(a);
    VDB_CHECK(!State(pool, a).dirty && State(pool, a).pin_count == 1);
    VDB_CHECK(Index(pool, a) < 2);
    PageBytes disk_bytes{};
    f.disk.Read(a, disk_bytes);
    VDB_CHECK(disk_bytes[1] == std::byte{0x41});
    Reject([&] { pool.Invalidate(a); });
    pinned.Drop();
    { auto g = pool.Fetch(b); g.MutableData()[1] = std::byte{0x42}; g.MarkDirty(); }
    { auto g = pool.Fetch(a); g.MutableData()[2] = std::byte{0x43}; g.MarkDirty(); }
    pool.FlushAll();
    VDB_CHECK(!State(pool, a).dirty && !State(pool, b).dirty);
    f.disk.Read(b, disk_bytes);
    VDB_CHECK(disk_bytes[1] == std::byte{0x42});
    f.disk.Read(a, disk_bytes);
    VDB_CHECK(disk_bytes[2] == std::byte{0x43});
    pool.Invalidate(b);
    VDB_CHECK(Index(pool, b) == 2);
    pool.Invalidate(b);
    pool.Flush(b);

    { auto g = pool.Fetch(b); g.MutableData()[4] = std::byte{0x45}; g.MarkDirty(); }
    pool.Invalidate(b);
    VDB_CHECK(Index(pool, b) == 2);
    f.disk.Read(b, disk_bytes);
    VDB_CHECK(disk_bytes[4] == std::byte{0x45});

    // A failed incoming read retains the old frame's mapping, contents, and pins.
    { auto g = pool.Fetch(b); g.MutableData()[5] = std::byte{0x46}; g.MarkDirty(); }
    const auto before = State(pool, b);
    f.disk.Flush(FileKind::Database);
    std::filesystem::resize_file(f.path / "db.pages", uint64_t(c.page_id) * kPageSize);
    Reject([&] { pool.Fetch(c); });
    VDB_CHECK(Index(pool, b) < 2 && Index(pool, c) == 2);
    VDB_CHECK(State(pool, b).generation == before.generation);
    VDB_CHECK(State(pool, b).dirty);
    { auto g = pool.Fetch(b); VDB_CHECK(g.Data()[5] == std::byte{0x46}); }
    NoPins(pool);
}

void TestNewPageAndIdentity() {
    Fixture f;
    BufferPool pool(f.disk, 2);
    auto db = pool.NewPage(FileKind::Database);
    auto tmp = pool.NewPage(FileKind::Temporary);
    VDB_CHECK(db.Page() == (PageRef{FileKind::Database, 0}));
    VDB_CHECK(tmp.Page() == (PageRef{FileKind::Temporary, 0}));
    VDB_CHECK(db.Data() == PageBytes{} && tmp.Data() == PageBytes{});
    VDB_CHECK(Index(pool, db.Page()) != Index(pool, tmp.Page()));
    db.MutableData()[0] = std::byte{0xaa}; db.MarkDirty();
    tmp.MutableData()[0] = std::byte{0xbb}; tmp.MarkDirty();
    VDB_CHECK(db.Data()[0] == std::byte{0xaa} && tmp.Data()[0] == std::byte{0xbb});
    db.Drop(); tmp.Drop();
    pool.FlushAll();
    VDB_CHECK(State(pool, {FileKind::Temporary, 0}).dirty);
    NoPins(pool);

    BufferPool exhausted(f.disk, 1);
    auto held = exhausted.Fetch({FileKind::Database, 0});
    Reject([&] { exhausted.NewPage(FileKind::Database); });
    VDB_CHECK(f.disk.PageCount(FileKind::Database) == 2); // Append-only allocation remains.
    held.Drop();
    NoPins(exhausted);

    BufferPool tiny(f.disk, 1);
    { auto g = tiny.Fetch({FileKind::Temporary, 0});
      g.MutableData()[2] = std::byte{0xcc}; g.MarkDirty(); }
    { auto g = tiny.Fetch({FileKind::Database, 0}); }
    VDB_CHECK(tiny.metrics().dirty_evictions == 1);
    { auto g = tiny.Fetch({FileKind::Temporary, 0});
      VDB_CHECK(g.Data()[2] == std::byte{0xcc}); }
    NoPins(tiny);
}

void TestRingRulesAndDirtyReuse() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 1);
    PageRef b = f.Add(FileKind::Database, 2);
    PageRef c = f.Add(FileKind::Database, 3);
    PageRef d = f.Add(FileKind::Database, 4);
    BufferPool pool(f.disk, 2);
    ScanRing ring(pool, 1);
    VDB_CHECK(ScanRing(pool).size() == 1);
    { auto g = pool.Fetch(a, AccessMode::Scan, &ring); }
    const RingHandle first = ring.Current();
    VDB_CHECK(first.occupied && ring.cursor() == 0);
    { auto g = pool.Fetch(b, AccessMode::Scan, &ring); g.MutableData()[1] = std::byte{0xee}; g.MarkDirty(); }
    VDB_CHECK(ring.Current().frame_index == Index(pool, b));
    { auto g = pool.Fetch(a, AccessMode::Scan, &ring); }
    VDB_CHECK(ring.Current().frame_index == Index(pool, b)); // Global hit does not advance ring.
    { auto g = pool.Fetch(c, AccessMode::Scan, &ring); }
    VDB_CHECK(pool.metrics().ring_reuses == 1);
    VDB_CHECK(pool.metrics().dirty_evictions == 1);
    VDB_CHECK(Index(pool, b) == 2);
    PageBytes persisted{};
    f.disk.Read(b, persisted);
    VDB_CHECK(persisted[1] == std::byte{0xee});
    VDB_CHECK(Index(pool, a) < 2);
    { auto g = pool.Fetch(d, AccessMode::Scan, &ring); }
    VDB_CHECK(pool.metrics().ring_reuses == 2);
    NoPins(pool);

    // A pinned ring candidate is rejected without aging that pinned frame.
    BufferPool pinned_pool(f.disk, 2);
    ScanRing pinned_ring(pinned_pool, 1);
    auto held = pinned_pool.Fetch(a, AccessMode::Scan, &pinned_ring);
    { auto g = pinned_pool.Fetch(b); }
    { auto g = pinned_pool.Fetch(c, AccessMode::Scan, &pinned_ring); }
    VDB_CHECK(Index(pinned_pool, a) < 2 && State(pinned_pool, a).usage_count == 1);
    VDB_CHECK(pinned_pool.metrics().ring_reuses == 0);
    held.Drop();
    NoPins(pinned_pool);

    // A hot candidate falls back to counted CLOCK; the ring itself does not cool it.
    BufferPool hot_pool(f.disk, 2);
    ScanRing hot_ring(hot_pool, 1);
    { auto g = hot_pool.Fetch(a, AccessMode::Scan, &hot_ring); }
    { auto g = hot_pool.Fetch(b); }
    { auto g = hot_pool.Fetch(a); }
    VDB_CHECK(State(hot_pool, a).usage_count == 2);
    { auto g = hot_pool.Fetch(c, AccessMode::Scan, &hot_ring); }
    VDB_CHECK(hot_pool.metrics().ring_reuses == 0);
    VDB_CHECK(hot_pool.metrics().ring_fallbacks == 2);
    VDB_CHECK(Index(hot_pool, a) < 2);
    NoPins(hot_pool);

    BufferPool cursor_pool(f.disk, 2);
    ScanRing cursor_ring(cursor_pool, 2);
    VDB_CHECK(cursor_ring.size() == 2 && cursor_ring.cursor() == 0);
    { auto g = cursor_pool.Fetch(a, AccessMode::Scan, &cursor_ring); }
    VDB_CHECK(cursor_ring.cursor() == 1);
    { auto g = cursor_pool.Fetch(a, AccessMode::Scan, &cursor_ring); }
    VDB_CHECK(cursor_ring.cursor() == 1); // Hits do not consume slots.
    { auto g = cursor_pool.Fetch(b, AccessMode::Scan, &cursor_ring); }
    VDB_CHECK(cursor_ring.cursor() == 0);
    NoPins(cursor_pool);

    BufferPool medium(f.disk, 16);
    BufferPool large(f.disk, 80);
    VDB_CHECK(ScanRing(medium).size() == 2);
    VDB_CHECK(ScanRing(large).size() == 8);
    Reject([&] { ScanRing(pool, 3); });
    ScanRing wrong_pool(medium);
    Reject([&] { pool.Fetch(a, AccessMode::Scan, &wrong_pool); });
}

void TestStaleAndTemporaryDiscard() {
    Fixture f;
    PageRef a = f.Add(FileKind::Database, 1);
    PageRef b = f.Add(FileKind::Database, 2);
    PageRef c = f.Add(FileKind::Database, 3);
    PageRef d = f.Add(FileKind::Database, 4);
    BufferPool pool(f.disk, 2);
    ScanRing ring(pool, 1);
    { auto g = pool.Fetch(a, AccessMode::Scan, &ring); }
    RingHandle stale = ring.Current();
    { auto g = pool.Fetch(b); }
    { auto g = pool.Fetch(c); }
    VDB_CHECK(pool.InspectFrames()[stale.frame_index].generation != stale.generation);
    { auto g = pool.Fetch(d, AccessMode::Scan, &ring); }
    VDB_CHECK(pool.metrics().ring_reuses == 0 && pool.metrics().ring_fallbacks >= 2);
    NoPins(pool);

    Fixture temp_fixture;
    PageRef persistent = temp_fixture.Add(FileKind::Database, 0x11);
    PageRef scratch = temp_fixture.Add(FileKind::Temporary, 0x22);
    PageRef other = temp_fixture.Add(FileKind::Database, 0x33);
    BufferPool temp_pool(temp_fixture.disk, 2);
    ScanRing temp_ring(temp_pool, 1);
    { auto g = temp_pool.Fetch(persistent); }
    auto pinned = temp_pool.Fetch(scratch, AccessMode::Scan, &temp_ring);
    pinned.MutableData()[1] = std::byte{0x99}; pinned.MarkDirty();
    RingHandle old = temp_ring.Current();
    Reject([&] { temp_pool.DiscardTemporaryPages(); });
    VDB_CHECK(Index(temp_pool, scratch) < 2);
    pinned.Drop();
    const auto writes = temp_fixture.disk.counters().temporary_writes;
    temp_pool.DiscardTemporaryPages();
    VDB_CHECK(Index(temp_pool, scratch) == 2 && Index(temp_pool, persistent) < 2);
    VDB_CHECK(temp_fixture.disk.counters().temporary_writes == writes);
    VDB_CHECK(temp_pool.InspectFrames()[old.frame_index].generation != old.generation);
    { auto g = temp_pool.Fetch(other); }
    VDB_CHECK(temp_pool.InspectFrames()[old.frame_index].generation != old.generation);
    { auto g = temp_pool.Fetch(scratch, AccessMode::Scan, &temp_ring); }
    VDB_CHECK(temp_pool.metrics().ring_reuses == 0);
    NoPins(temp_pool);
}

struct PollutionResult { uint64_t hot_misses; uint64_t total_misses; uint64_t evictions; uint64_t ring_reuses; };

PollutionResult ScanWorkload(bool use_ring) {
    Fixture f;
    for (int i = 0; i < 48; ++i) f.Add(FileKind::Database, static_cast<uint8_t>(i));
    BufferPool pool(f.disk, 8);
    ScanRing ring(pool, 1);
    for (int i = 0; i < 8; ++i) { auto g = pool.Fetch({FileKind::Database, PageId(i)}); }
    for (int repeat = 0; repeat < 5; ++repeat)
        for (PageId hot = 0; hot < 2; ++hot) { auto g = pool.Fetch({FileKind::Database, hot}); }
    for (PageId scan = 8; scan < 48; ++scan) {
        auto g = pool.Fetch({FileKind::Database, scan}, AccessMode::Scan,
                            use_ring ? &ring : nullptr);
    }
    const uint64_t before = pool.metrics().buffer_misses;
    for (PageId hot = 0; hot < 2; ++hot) { auto g = pool.Fetch({FileKind::Database, hot}); }
    NoPins(pool);
    return {pool.metrics().buffer_misses - before, pool.metrics().buffer_misses,
            pool.metrics().evictions, pool.metrics().ring_reuses};
}

void TestScanPollution() {
    const auto ordinary = ScanWorkload(false);
    const auto ring = ScanWorkload(true);
    VDB_CHECK(ordinary.hot_misses == 2);
    VDB_CHECK(ring.hot_misses == 0);
    VDB_CHECK(ring.total_misses < ordinary.total_misses);
    VDB_CHECK(ring.ring_reuses > 0 && ordinary.ring_reuses == 0);
}

int main() {
    try {
        TestHitsUsageAndGuards();
        TestScanHitsAndClock();
        TestPinnedAndOneFrame();
        TestFlushInvalidateAndFailures();
        TestNewPageAndIdentity();
        TestRingRulesAndDirtyReuse();
        TestStaleAndTemporaryDiscard();
        TestScanPollution();
        std::cout << "buffer pool tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
