# Buffer manager

`BufferPool` caches fixed-size database and temporary pages in a fixed number of frames. `PageRef`, including its `FileKind`, is the cache key. The page table maps each resident `PageRef` to exactly one frame. This is a single-threaded, educational manager; pins express page lifetime, not locks.

## Frames, guards, and access modes

Each frame has a valid flag, page identity, pin count, dirty flag, usage count in `[0,5]`, generation, and 4096 bytes. `PageGuard` is move-only. Each returned guard owns one pin; `Drop()` and destruction release it once. Move assignment releases the destination's previous pin. The pool and `DiskManager` must outlive guards, and byte references from `Data()` or `MutableData()` are valid only while the guard owns its pin. Callers explicitly call `MarkDirty()` after changing bytes.

A normal cache hit increments usage up to 5, even when another guard already pins the frame. A scan hit uses `max(usage, 1)`; it neither cools a hot page nor heats a page above 1. A newly installed page starts with one pin, usage 1, and clean state. `NewPage` allocates through `DiskManager` and installs a zero page without initializing any domain-specific header. If buffer acquisition fails after physical allocation, the append-only page remains allocated and may be unreachable.

## Counted CLOCK sweep

`BufferPool` owns a single `size_t clock_hand_`, identifying the **next frame to inspect**. `SweepVictim()` advances it directly; there is no separate CLOCK state wrapper. Each inspection advances it once, wrapping at the pool size. An invalid frame is consumed before evicting any valid page. Otherwise, the sweep follows:

```text
for at most 6 * frame_count inspections:
    frame = frames[hand]
    advance hand
    if invalid: return frame
    if pinned: continue                 # do not age it
    if usage > 0: usage--; continue
    return frame                        # unpinned and usage 0
throw DbError("buffer pool exhausted: all frames pinned")
```

Because usage is at most 5 and pin states cannot change during this single-threaded search, an unpinned frame can be selected within 6F inspections. The bound includes repeated revolutions and works for F=1.

For a miss, the pool first checks for an invalid frame. With none available, an eligible scan-ring handle may select a victim; otherwise CLOCK runs. Incoming bytes are read into a separate `PageBytes` before frame identity changes. A dirty victim is written before its old mapping is removed. Failed reads and writes leave the old mapping, bytes, and dirty obligation intact. After successful I/O, the new map entry is allocated, the old entry is removed, bytes and metadata change, and the new frame gets a pin. CLOCK aging performed before an I/O failure is retained.

## Scan ring

`ScanRing` holds only `(frame_index, generation)` handles and a cursor. Its default size is `min(8, max(1, F/8))`; callers may specify a size up to F. The pool must outlive the ring. A successful scan miss records the installed frame in the current slot and advances the cursor. Cache hits and failed misses leave the cursor unchanged.

The global page table is searched **before** ring selection, so the ring cannot create a duplicate cached page. A ring candidate qualifies only when its index exists, the handle generation still matches, the frame is valid and unpinned, and usage is at most 1. Otherwise the pool falls back to CLOCK. The ring never changes usage to force eligibility. Identity changes on replacement, invalidation, and temporary discard increment generation, making old handles stale. Dirty ring victims use ordinary replacement I/O, including temporary victims that may be read later in the same query.

## Flush, invalidation, and temporary teardown

`Flush(page)` does nothing when the page is not resident or is clean. For a dirty resident page it writes and flushes that page's file, then marks the frame clean. It does not unpin or evict. `FlushAll()` visits only database frames, then flushes the database file; temporary dirty frames remain dirty. Call this checked path before ordinary database shutdown rather than relying on the pool destructor. `Invalidate(page)` is a no-op for an uncached page, rejects a pinned resident page, flushes a dirty resident page, and then removes its mapping. It applies to either file kind.

`DiscardTemporaryPages()` first rejects **any** pinned temporary frame, then removes all cached temporary mappings and advances their generations. It intentionally discards dirty scratch bytes without writing them. Persistent frames are untouched. Query teardown must release temporary guards, discard temporary cache entries, and only then reset or remove the scratch file. Runtime temporary eviction still writes dirty pages normally.

## Metrics and boundaries

`BufferMetrics` distinguishes logical `buffer_fetches`, hits, misses, new pages, evictions, dirty evictions, sweep inspections/decrements, ring reuses/fallbacks, active pins, and peak pins. Fetch counters include failed attempts after ring validation; ring reuse counts only completed installations. `explicit_flush_writes` and `dirty_eviction_writes` count successful file Write calls by cause, separately from completed evictions. `DiskManager::counters()` exposes `IoCounters` for file-page reads and writes. Buffer counters last for the pool lifetime; copy the struct for a snapshot. See [io_cost_models.md](io_cost_models.md) for exact boundaries, allocation I/O, and OS-cache limits. `InspectFrames()` returns read-only snapshots, and `SweepHand()` exposes the next inspection position for deterministic tests.

Production database, heap, catalog, index, and temporary page contents pass through `BufferPool`. Direct `DiskManager` content access remains suitable for physical tests and narrowly documented bootstrap validation; allocation remains physical. PostgreSQL inspired the bounded usage count, CLOCK sweep, and bulk scan ring ideas. ValenceDB omits its concurrency, atomics, WAL coordination, background writer, checkpointing, backend-specific refcounts, and asynchronous I/O. It does not claim PostgreSQL compatibility or crash durability.

## Source, complexity, and self-check

Read `BufferPool::Fetch`, `ChooseFrame`, `SweepVictim`, and `Install` in [buffer_pool.cpp](../src/buffer/buffer_pool.cpp), then [PageGuard::Drop](../src/buffer/page_guard.cpp) and [ScanRing::Record](../src/buffer/scan_ring.cpp). Expected hit lookup is O(1); a miss scans up to F invalid candidates and may inspect up to 6F CLOCK positions, so metadata work is O(F). The pool owns F page arrays and bounded metadata. A miss reads one file page; replacing a dirty victim adds one write, while NewPage performs separate zero initialization. Cache/device distinctions are in the I/O document.

LRU would require recency-list maintenance on every access; one-bit CLOCK would lose the bounded multi-use history. Counted CLOCK plus a small scan ring makes that tradeoff explicit. No concurrency, background writing or WAL coordination is hidden here.

The deterministic tests in [`tests/buffer_pool_test.cpp`](../tests/buffer_pool_test.cpp) exercise pinning, CLOCK state, scan-ring behavior, flush/invalidation, temporary-page teardown, failure handling, and counter invariants.
