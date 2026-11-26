# Phase 3: B+ tree and buffer implementation

> Historical report for the pre-V1 implementation. Deletion, repair, reservation, and their measurements below describe that preserved revision. Current behavior is documented in [B+ tree V1](bplus_tree.md) and the [V1 Phase 1 report](v1_phase1_refactor.md).

## Checkpoints and scope

Phase 1 remains commit `092b80b`. All 17 uncommitted files matched the Phase 2
report and were checkpointed as `21c452a` before Phase 3 edits. Work is on
`refactor/phase3-btree-buffer`; Phase 3 remains uncommitted for review. No reset,
push, merge, Phase 4 work, optimizer architecture change, or execution algorithm
change was made. The Phase 2 smoke, timing, and comparison artifacts remain
unchanged. Its current Debug baseline passed 13/13 suites (78.46 seconds).

Before editing, Release was rebuilt and its executable and static library copied
to `/private/tmp/valencedb-phase2-bench` and `/private/tmp/valencedb-phase2.a`.
The standalone comparison probe links that library against headers extracted
from `21c452a`, so it measures the actual Phase 2 implementation.

## Files changed

Modified:
- `CMakeLists.txt`
- `include/valencedb/buffer/buffer_pool.hpp`
- `src/buffer/buffer_pool.cpp`
- `src/index/bplus_tree.cpp`
- `docs/bplus_tree.md`
- `docs/buffer_manager.md`

Added:
- `bench/bplus_tree_probe.cpp`
- `docs/phase3_refactor.md`

Deleted:
- `include/valencedb/buffer/clock_sweep.hpp`
- `src/buffer/clock_sweep.cpp`

Generated logs, comparison scripts, and JSON/TSV evidence are under the existing
ignored `benchmark_results/` directory, listed below. Test sources and the raw
page validator are unchanged. The standalone probe is compiled separately and
uses that validator; it does not add a test or change the smoke runner schema.

## Structural changes and readability

Insertion formerly duplicated internal-page validation and decoding, with a
parent guard, two local vectors, and manual pin release inside the split loop.
It now calls the existing `ReadInternal()` once and directly inserts into
`parent.children` and `parent.separators`. This uses the same one fetch and two
vector allocations as before. The decoder reserves one spare child slot and
separator space, preserving the old insertion allocation behavior. Leaf decoding
also reserves its existing count once instead of repeatedly growing its vector;
an empty leaf needs no allocation.

The split algorithm remains in Insert: reserve the entire predicted cascade,
split the leaf, promote its right boundary, insert a separator/right page at the
saved child index, split overflowing parents, and publish a new root if needed.
Spans identify the two halves without allocating split copies. The promoted
internal separator is excluded from both children. Ordinary leaf insertion
still writes through its already-held guard. Descent, Contains, Start, iterator
advancement, and physical page codecs are unchanged.

Erase formerly carried both leaf and internal state through one loop and used
`repairing_leaf` to dispatch each iteration. It now repairs the leaf once, then
loops over internal underflow only. Root contraction and sufficient occupancy
are explicit stop conditions; the underfull parent moves into current scratch.
No new state enum or helper framework replaces the removed flag.

Both repair functions formerly decoded a sibling to check lending, discarded
it, then fetched/validated/decoded it again to merge. They now keep a nonlending
left value while checking the right. The no-left case merges right in that
right value's scope; otherwise the final case merges left. Left/right borrowing,
separator rotations, parent-fence insertion on internal merge, leaf links, and
parent child removal remain explicit. A sibling is decoded once per decision;
write helpers still fetch destination pages independently. This removes work
and makes the lifetime of the sibling used for merging visible.

CLOCK formerly delegated its hand increment to a class in two separate files.
BufferPool now owns `size_t clock_hand_`; SweepVictim visibly inspects, advances,
wraps, skips pinned frames, decrements positive usage, and selects zero usage.
The 6F bound and exact victim policy remain. PageGuard and ScanRing were already
small useful abstractions and are unchanged. Fetch/ChooseFrame/Install retain
staged incoming bytes, dirty writeback, and map allocation before replacing the
old resident identity. No diagnostic counter was removed: the counters have
behavioral tests and distinct success/failure boundaries. Schema version 2 and
all Phase 2 consumers remain unchanged.

## Verified invariants

The existing independent validator checks raw disk fields, not the refactored
node readers. Tests retain physical `(INT64 key, RID.page_id, RID.slot_id)` order,
logical duplicates, exact duplicate no-ops, signed extremes, equality routing
right, and `max(left) < fence <= min(right)`. Gapped-fence fixtures verify real
separator rotations rather than substituting subtree minima.

Occupancy, child/separator counts, promoted-separator exclusion, equal leaf
depth, leaf links, all split levels, root exceptions/growth/contraction, empty
roots, retired-page unreachability, metadata, and clean reopen remain covered.
Insertion reaches height 7; deletion tests reach height 8 and observe 20 root
contractions. Both report peak pins=1 and final active pins=0. The 1-frame traces,
streaming ranges, mutation epochs, and cursor invalidation remain intact.

Buffer tests retain deterministic CLOCK hand/victim assertions, usage saturation
and decay, pinned-frame protection and exhaustion, dirty eviction, hits,
scan-ring eligibility/reuse/fallback, stale generations, temporary teardown,
move-only guard release, and allocation/I/O failure behavior. Complete execution,
storage, optimizer, and integration suites test table-index maintenance and
IndexScan consumers without changing expectations.

## Commands and verification

Commands ran from the repository root; configure/build/test output is retained
in `benchmark_results/phase3_logs/`.

```sh
# Before edits (baseline-* logs):
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
cp build-release/valencedb_bench /private/tmp/valencedb-phase2-bench
cp build-release/libvalencedb.a /private/tmp/valencedb-phase2.a

# After insertion/CLOCK, then after deletion (each followed a build):
cmake --build build -j2
ctest --test-dir build -R 'bplus_insert|buffer_pool' --output-on-failure
cmake --build build -j2
ctest --test-dir build -R 'bplus_delete|bplus_insert|buffer_pool' --output-on-failure

# Final builds and full suites:
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j2
ctest --test-dir build-sanitized --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 --output benchmark_results/phase3_smoke.jsonl

# Standalone probe, with matching headers for each library:
mkdir -p /private/tmp/valencedb-phase2-headers
git archive 21c452a include | tar -x -C /private/tmp/valencedb-phase2-headers
c++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -I/private/tmp/valencedb-phase2-headers/include -Itests bench/bplus_tree_probe.cpp /private/tmp/valencedb-phase2.a -o /private/tmp/valencedb-phase2-probe
c++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Iinclude -Itests bench/bplus_tree_probe.cpp build-release/libvalencedb.a -o /private/tmp/valencedb-phase3-probe
/private/tmp/valencedb-phase2-probe 1 8000 254
python3 benchmark_results/phase3_logs/compare.py
git diff --check
```

The focused runs passed 2/2 and 3/3 suites. Final Debug passed 13/13 (98.78
seconds); final ASan/UBSan passed 13/13 (200.07 seconds), with no sanitizer
diagnostics. Both complete suites and smoke were rerun after tightening the
leaf reserve to its existing count, including zero for an empty leaf. Full suite wall times include concurrent build/test work and are not
performance comparisons. The Release smoke run passed 63/63 cases.

Randomized tests run within `valencedb_bplus_insert` and `valencedb_bplus_delete`,
both in the focused and complete Debug/sanitizer invocations above:
- Insertion: seeds 20260930, 195936478, 71827; 900 generated entries each,
  additional exact duplicates, periodic full/raw/range/point checks and reopen.
- Mixed tree operations: those three seeds plus 203; 4,000 operations per seed,
  capacities 4/4, 5/5, and 2/3, one frame, independent lexicographic set oracle.
- Database heap/index consistency: seed 5052026; 3,000 operations with reopen.

Failure injection runs in the same insertion and buffer suites. The insertion
fixture forces a dirty-eviction write failure during split reservation and
verifies unchanged existing bytes, entries, epoch, cursor validity, zero pins,
and successful retry, allowing only an append-only orphan allocation. Buffer
fixtures inject failed incoming reads, dirty victim writes, and invalidation
writes; they retain resident mapping, bytes, generation, and dirty obligation.
All-frames-pinned and allocation-before-frame-exhaustion cases remain covered.
No new generalized fault-injection machinery was added.

## Memory and remaining limits

The tree still uses O(height) path/reservation/retired IDs and O(page capacity)
mutation scratch. No whole-tree representation or persistent decoded-node cache
was added. Lookup has no decoded vectors; cursors retain only their existing
small state and no page pins. Buffer frame count and page sizes are unchanged.

Retaining a nonlending left sibling while reading the right can temporarily add
one bounded sibling value: at production minima, at most 127 BPlusKey slots
(2,032 bytes), or 103 PageIds plus 102 BPlusKeys (2,044 bytes), excluding vector
headers/allocator overhead. The alternative was the old reread/revalidation and
reallocation. Reserving decoded counts also removes geometric-growth slack and
repeated allocations. The comparison probe reports whole-process maximum RSS;
that includes its dataset, oracle, validator, library, and allocator, and cannot
isolate these few KiB of scratch.

Disk formats, capacities, append-only allocation and retirement are unchanged.
Unused reservations and retired pages are not reclaimed. Unexpected I/O failure
after structural writes still has no rollback guarantee; this phase adds no
transaction, WAL, or crash recovery. Clean close/reopen remains supported.
Full page validation still costs O(B) per visited page, including cursor calls;
this deliberately preserved validation is a remaining performance/readability
tradeoff, not a newly introduced regression. Timing samples below are limited
observations rather than statistical speedup claims.

## Final performance comparison and investigation

All 63 smoke records match Phase 2 in schema version, results/digests, buffer
counters and state, physical I/O, scratch metrics, algorithm diagnostics, and
physical plans. No execution benchmark schema or consumer changes were needed.
Historical single-run timings are not used to infer regressions because builds
and correctness tests ran concurrently with smoke.

After those jobs finished, eight representative benchmark cases ran in alternating
Phase 2/3 order for five trials: **80/80 reference-validated runs**, with all
compared result/counter fields equal. Median elapsed milliseconds:

| Case | Phase 2 | Phase 3 |
| --- | ---: | ---: |
| clock_off | 0.828 | 0.821 |
| clock_on | 0.763 | 0.755 |
| btree_insert | 7.984 | 7.848 |
| point_unique_index | 0.837 | 0.835 |
| point_duplicate_index | 7.100 | 7.046 |
| range_unique_short_index | 0.205 | 0.199 |
| range_unique_long_index | 5.738 | 5.889 |
| range_duplicate_long_index | 4.600 | 4.641 |

An earlier short point-query sample looked slower. Increasing its `--lookups`
from 8 to 800 and alternating six paired trials gave **12/12 successes** with
matching results/counters: medians 85.967 ms (Phase 2) and 83.377 ms (Phase 3).
The final eight-lookup comparison above also shows no sustained increase.
The direct 8,000-entry Contains probe below is stable. No lookup regression or
speedup is established by these samples.

The standalone probe fills a tree in shuffled order (seed 5032026), checks every
physical key, traverses an inclusive full range, then erases in shuffled order.
Keys repeat logically three times with distinct RIDs. Each timed operation
includes its final FlushAll; raw-page validation runs outside the timing.
Configurations are 8,000 entries at production capacity 254/204 and 800 entries
at capacity 4/4, each with 1 and 16 frames. Five alternating paired trials give
**40 successful process runs / 160 validated operation records**. Medians for
production capacity:

| Frames | Operation | Phase 2 ms | Phase 3 ms | Fetches, Phase 2 → 3 | Reads, Phase 2 → 3 | Writes, Phase 2 → 3 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | insert | 96.552 | 96.996 | 15921 → 15921 | 15538 → 15538 | 7837 → 7837 |
| 1 | contains | 62.001 | 62.064 | 16000 → 16000 | 16000 → 16000 | 0 → 0 |
| 1 | range | 21.736 | 22.093 | 8002 → 8002 | 46 → 46 | 0 → 0 |
| 1 | erase | 146.705 | 132.512 | 32956 → 32912 | 22525 → 22525 | 12056 → 12056 |
| 16 | insert | 59.630 | 59.593 | 15921 → 15921 | 2364 → 2364 | 2411 → 2411 |
| 16 | contains | 41.508 | 41.030 | 16000 → 16000 | 5213 → 5213 | 0 → 0 |
| 16 | range | 22.251 | 21.644 | 8002 → 8002 | 46 → 46 | 0 → 0 |
| 16 | erase | 90.158 | 76.092 | 32956 → 32912 | 3242 → 3259 | 3084 → 3101 |

Production deletion eliminates exactly 44 redundant sibling fetches. Small-capacity
deletion eliminates 411 (8,216 → 7,805). Insert, Contains, and range fetch/I/O
counts are identical at both capacities and frame counts; allocation counts are
identical throughout and deletion allocates no disk pages. Peak pins stay one.

The 16-frame deletion access trace has less repeated heating of sibling frames,
so CLOCK's unchanged usage-count policy can choose different victims later.
Production reads/writes rise by 17 (0.52%/0.55%); small-capacity reads fall by nine
(3,623 → 3,614), while writes rise by three (1,598 → 1,601). The comparison script
initially assumed identical deletion write totals; inspection identified this
cache-aging effect, and the final script records those differences explicitly.
The simpler alternative of rereading siblings would restore the old trace but
also restore duplicate decoding/validation and allocations. The bounded I/O
variation is accepted; no material elapsed-time regression was observed.
Deletion's lower measured times are observations, not a general speedup claim.

Whole-process peak RSS medians from `/usr/bin/time -l`:

| Capacity | Frames | Phase 2 bytes | Phase 3 bytes |
| --- | ---: | ---: | ---: |
| 254 | 1 | 3227648 | 3424256 |
| 254 | 16 | 3325952 | 3571712 |
| 4 | 1 | 2064384 | 2015232 |
| 4 | 16 | 2162688 | 2080768 |

Production RSS is about 0.19–0.23 MiB higher in the final paired run; small-capacity
RSS is lower. An earlier run showed about 0.25–0.28 MiB higher production RSS.
These are process/allocator measurements, not live node-storage measurements.
To evaluate a simpler alternative, an isolated library variant removed only
ReadLeaf's reserve, restoring geometric vector growth. Three paired 16-frame
runs retained identical results and I/O. That variant's RSS median was 3,375,104
bytes versus 3,489,792 for the final implementation, but erase median was
81.296 ms versus 75.650 ms. This supports an allocation-policy contribution to
RSS; it does not isolate the entire difference. The exact-count reserve remains:
it avoids repeated decoding allocations, needs no allocation on empty leaves,
and keeps live scratch bounded. The measured process RSS increase is retained
as a documented tradeoff, not described as a memory improvement.

The alternative was built only under `/private/tmp/valencedb-phase3-growth/`:
a copy of bplus_tree.cpp with `node.entries.reserve(count)` omitted, compiled
with `c++ -std=c++20 -O3 -DNDEBUG -Iinclude -c`, substituted for
`bplus_tree.cpp.o` in a copy of the final static library using `ar -r`, and linked
to the same probe. It did not change repository sources. Each binary ran as
`/usr/bin/time -l <probe> 16 8000 254`. The full measurements are preserved.

## Evidence and final review

- Preserved baseline: `benchmark_results/phase2_smoke.jsonl`, its summary, and
  all existing Phase 2 comparison/timing/log artifacts.
- Final smoke: `benchmark_results/phase3_smoke.jsonl` and `.summary.json`.
- Field comparison: `benchmark_results/phase3_comparison.json`.
- Alternating benchmarks: `benchmark_results/phase3_timing_recheck.jsonl` and
  `phase3_timing_summary.json` (exact command arrays included).
- Larger point-query investigation: `benchmark_results/phase3_point_recheck.jsonl`
  (exact command arrays included).
- Direct tree probes: `benchmark_results/phase3_probe.jsonl` and
  `phase3_probe_summary.json` (exact command arrays included).
- Logs and scripts: `benchmark_results/phase3_logs/`, including full Debug and
  sanitizer CTest output, `compare.py`, and `memory-recheck.json`.

`git diff --check` passed. The complete source/document diff and added probe
were reviewed for scope, guard lifetimes, removed-file references, and unchanged
test expectations. Builds produced no compiler warnings/errors. No unresolved
correctness failures remain. Remaining limits are the measured RSS/I/O tradeoffs,
limited timing samples, full page-validation cost, and existing append-only and
nontransactional behavior described above. Phase 4 remains untouched.
