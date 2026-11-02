# Phase 2: execution state and diagnostics

Branch: `refactor/phase1-temp-runs`. Phase 1 was initially uncommitted; its
24 files were inspected and checkpointed as local commit `092b80b` before Phase 2
edits. No unrelated work was included. The current Phase 1 Debug baseline passed
13/13 suites. Existing Phase 1 benchmarks remain unchanged. No push, merge,
reset, or Phase 3/4 implementation was performed. Phase 2 changes are left in
the working tree for review.

## Files

Modified:
- `include/valencedb/execution/{aggregate,external_sort,grace_hash_join}.hpp`
- `src/execution/{aggregate,external_sort,grace_hash_join}.cpp`
- `tests/{aggregation,grace_hash_join,optimizer}_test.cpp`
- `bench/valencedb_bench.cpp`, `tools/run_benchmarks.py`
- `docs/{aggregation,benchmarks,external_sort,joins,optimizer}.md`

Added: `docs/phase2_refactor.md`. No files deleted. Generated evidence lives in
ignored `benchmark_results/phase2*` files and `benchmark_results/phase2_logs/`.
No B+ tree, buffer replacement, optimizer architecture, or TempRun implementation
changes were needed.

## Before and after

HashAggregate previously kept the build table, writer, sealed partial descriptor,
and eight phase values on the executor, coordinating destruction manually before
sorting. `BuildGroups()` now owns the build map, writer, and triggering tuple
locally. Existing keys update even at capacity; only a new group triggers a
flush. On the no-spill path the map transfers to the output member, with no
extra vector. On the spill path the final batch is flushed and a sealed TempRun
returns by value. Function return destroys the entire map including buckets,
the writer including staging, and the input tuple before Init constructs the
sorter. The map member now serves only output iteration. Persistent writer and
partial-run members, ConsumeHash/FlushFinal/SortPartials phase values, and the
SortPartials orchestration helper are gone. Five output/lifecycle states remain.

SortAggregate previously selected between owning child_ and sort_ through
Input(). It now owns only input_; construction wraps it once in ExternalSort
when ordering requires it. All lifecycle and group-reading operations use that
one tree, which survives Close for replay. The existing nonzero sort allowance
identifies the wrapper for safe static metric access. No dynamic cast, observer
pointer, second owning pointer, or new ownership abstraction was introduced.
Nested sort metric snapshots are retained because they give direct run/pass and
memory evidence, including after Close. The optimizer's existing internal-sort
plan unwrapping remains unchanged, preserving exactly one sorter and its reduced
budget.

Grace Hash Join still partitions both inputs to disk, skips empty pairs, chooses
the smaller modeled build footprint, performs a bounded unordered_multimap build,
resumes equal_range probing, and falls back to BNLJ for oversized pairs or a
failed defensive build. Per-partition diagnostic records and their strategy enum
are gone, along with copied row/page counts, partition-write totals, synthetic
completed hash/fallback reads, and PartitionLeft/PartitionRight/Build phase values.
Selection now visibly separates descriptor admission from the defensive Build
failure. Five output/lifecycle states remain. Summary counters preserve path,
orientation, output, peak memory, and failed-build evidence; three scalar fallback
counters report left/right outer choice and actual inner scans at pair EOF.

ExternalSort retains GenerateRuns, SpillRun, OpenMerge, PullMerge, and MergePasses.
Its five-state phase enum becomes a two-mode output enum: private run or merge.
Executor already guards initialization and EOF. The next-pass run vector is
local to MergePasses, so no cleanup member is needed and its capacity dies before
streaming. Run rows still release their capacity before merge readers open.
The existing conservative two-array allowance remains valid during intermediate
merging; capacities and final output streaming are unchanged.

## Memory and instrumentation

Removing Grace's diagnostic vector changes the shared executor/planner formula:

```text
Phase 1: p=min(64,(M-S)/(P+320)), Dghj=192*p
Phase 2: p=min(64,(M-S)/(P+256)), Dghj=128*p
```

Each partition still charges two 64-byte TempRun slots and P+128 for a writer,
its pointer, and allocation overhead. Hash rows remain T+96, fallback rows T;
the per-operator M and all admission checks remain intact. For narrow inputs at
M=20000, p=2 and hash capacity=36 remain unchanged; Dghj=384→256 and fallback
memory=19616→19744. The two-partition minimum is 18840→18712. Planner tests use
the new shared formula. Paths can change for other inputs near these boundaries.

TempStore metrics were inspected and retained. Allocated and peak extent agree
in append-only storage, while current extent becomes zero after checked reset.
Existing consumers use both historical evidence and current cleanup checks.
Removing the few repeated assignments would require more consumer machinery than
it saves. Allocation-before-frame-failure accounting, cumulative page caps,
handle registration, and checked reset behavior remain unchanged.

Benchmark schema_version is now 2. Removed Grace fields are `pairs`,
`partition_write_pages`, `completed_hash_read_pages`, and
`completed_fallback_read_pages`. New fields are `left_fallback_pairs`,
`right_fallback_pairs`, and `completed_fallback_inner_scans`. The last includes
only fully consumed fallback pairs; partial cancellation/failure is excluded.
Existing fields retain their meanings. Physical I/O is still reported by
DiskManager and BufferPool, independently of operator diagnostics.

## Correctness and test evidence

Existing tests preserve empty/global/single-group aggregation, COUNT-only,
selected-field signed overflow, repeated spill/final flush, exactly-once trigger
handling, unequal partial-count AVG recombination, and one-row lookahead.
Raw partial inspection now tests BuildGroups' returned descriptor directly in a
new test group, preserving field/schema/count checks without retaining production
state merely for tests. Ordered-input reuse, internal multi-pass sorting,
cancellation, exceptions, and three-execution replay remain covered.

Grace tests replace copied-diagnostic comparisons with independently decoded
partition row counts, routing/multiset checks, descriptor-derived expected path
and scan counts, and actual BufferPool fetch deltas. They inspect live table size
and fallback block peaks against their budgets. Duplicate cross-products, both
build and fallback orientations, skew, forced runtime-cap abandonment, physical
I/O, cancellation/replay, failed reads/writes, and cleanup remain covered.

External sort tests retain the exactly-full private run, boundary tuple, tiny
budgets, finite descriptor boundary, odd-run carry, duplicate keys, four
intermediate merge levels, and streaming final output. All temporary allocation,
handle, buffer pin, stale-run, partial failure, and cleanup suites are retained.

Commands from the repository root (outputs retained in phase2_logs):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure

# Focused checks after the corresponding operator edits:
ctest --test-dir build -R aggregation --output-on-failure
ctest --test-dir build -R 'aggregation|optimizer|integration' --output-on-failure
ctest --test-dir build -R 'grace_hash_join|optimizer|integration' --output-on-failure
ctest --test-dir build -R 'external_sort|aggregation' --output-on-failure

cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j2
ctest --test-dir build-sanitized --output-on-failure

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 --output benchmark_results/phase2_smoke.jsonl
python3 benchmark_results/phase2_logs/compare.py
python3 benchmark_results/phase2_logs/timing_recheck.py
git diff --check
```

Each focused CTest invocation followed `cmake --build build -j2`. Full Debug
configure/build/CTest was run both before edits and after all operator changes.

## Readability assessment and limits

Hash aggregation's phase lifetime is now expressed by function scope, rather than
member reset order spread across setup and Close. Sort aggregation has one
ownership path to understand. Grace's loop describes join work instead of also
maintaining a second model of every partition; decisions remain next to their
algorithmic inputs. External sort retains its recognizable functions while
separating blocking setup from resumable output. Tests carry the detailed audit
logic, where it can be checked against independent resource counters.

RAII, immutable schemas, checked arithmetic, conservative memory allowances,
and bounded descriptors remain necessary for disk-backed execution. No generic
state machine or metric framework was added. TempRun schema/store lifetimes are
still caller obligations. Memory measurements remain a model, not process RSS;
append-only temporary storage is reclaimed at query cleanup. Timing observations
are limited samples, not statistically established performance claims.

## Final results and baseline comparison

- Current Phase 1 Debug baseline: 13/13 suites passed (89.18 seconds).
- Final Debug: 13/13 suites passed (91.21 seconds).
- Final ASan/UBSan: 13/13 suites passed (191.04 seconds), no sanitizer diagnostics.
- Focused passes: HashAggregate 1/1, SortAggregate with optimizer/integration 3/3,
  Grace with optimizer/integration 3/3, ExternalSort with aggregation 2/2.
- Aggregation has 15 test groups (one added); external sort has 13, Grace has 14,
  execution has 18, nested-loop joins have 14, optimizer has 12, integration has 5.
- Release smoke: 63/63 reference-validated cases succeeded, zero failures.
- Build logs contain no compiler warnings/errors. Complete diff inspected;
  `git diff --check` passed.

All 63 result digests and row counts match Phase 1. Ordered digests also match
for sorted results. Physical file-I/O, BufferPool counters, and scratch metrics
are identical in all 63 cases. Post-cleanup pins, temporary cache mappings, and
physical/current scratch extents are zero. All common algorithm fields match
in 58 cases; the five Grace cases differ only in expected memory diagnostics.
At M=32768, p remains 5, Dghj falls 960→640, hash capacities rise 57→58, fallback
memory rises 31808→32128, and fallback block capacities rise 77→79. Hash/fallback/
empty counts, build orientations, peak build rows, and output counts remain the
same. New fallback orientation/scan totals match the old per-pair records.

Representative counters, identical in Phase 1 and Phase 2:

| Case | DB reads | Temp reads | Temp writes | Temp allocations | Algorithm evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| sort_m_24576 | 19 | 96 | 96 | 97 | 21 runs, 4 intermediate passes, streamed final fan-in 2 |
| sort_m_32768 | 20 | 59 | 59 | 65 | 13 runs, 2 intermediate passes, streamed final fan-in 2 |
| join_skew_ghj_auto | 4 | 0 | 0 | 12 | 4 hash pairs, 1 fallback, 2 inner scans, 8697 rows |
| aggregate_spill_hash_seq | 11 | 33 | 50 | 64 | 5 overflow flushes, 6 batches, 649 partials, 4 sort passes |
| aggregate_spill_sort_seq | 11 | 17 | 23 | 32 | 12 sort runs, 2 intermediate passes, 350 groups |

Writes above exclude separately reported allocation initialization. The
M=32768 spilling aggregate retains capacity=117 and modeled hash peak=32760;
its partial sort still receives M=26568. SortAggregate's comparable internal
sort still receives M=30664. Memory and descriptor limits did not weaken.

The historical single-repeat smoke median Phase 2 / Phase 1 time ratio is
0.9924. Some untouched short-running cases vary more than 5%; this is not evidence
of algorithmic regression or speedup. For a contemporary comparison, the Phase 1
Release executable was copied before rebuilding to
`/private/tmp/valencedb-phase1-bench`. Seven cases were run against both binaries
in alternating order, three repeats each: 42/42 successful reference-validated
runs, matching paired results and physical I/O throughout. Median timings:

| Case | Phase 1 ms | Phase 2 ms | Ratio |
| --- | ---: | ---: | ---: |
| clock_off | 0.816 | 0.857 | 1.0508 |
| btree_insert | 7.885 | 7.884 | 0.9999 |
| sort_m_24576 | 1222.816 | 1210.569 | 0.9900 |
| join_balanced_ghj_auto | 78.806 | 77.996 | 0.9897 |
| aggregate_spill_hash_seq | 721.596 | 725.718 | 1.0057 |
| aggregate_spill_sort_seq | 412.494 | 360.846 | 0.8748 |
| join_skew_ghj_auto | 100.493 | 99.371 | 0.9888 |

No material regression was established in the changed execution paths. The hash
aggregate's 0.6% increase and the larger SortAggregate decrease are observations,
not statistical claims. The untouched sub-millisecond clock control also varied.
No unresolved correctness failures remain. Future consumers must recognize JSON
schema 2; old synthetic page counters are intentionally absent. Budget-boundary
path changes outside these fixtures remain possible because Grace now charges
only the structures it retains.

Evidence:
- `benchmark_results/phase1_smoke.jsonl` (preserved baseline)
- `benchmark_results/phase2_smoke.jsonl` and `.summary.json`
- `benchmark_results/phase2_comparison.json`
- `benchmark_results/phase2_timing_recheck.jsonl` (exact commands for each run)
- `benchmark_results/phase2_timing_summary.json`
- `benchmark_results/phase2_logs/` (baseline/final/focused build and test logs,
  comparison scripts, timing scripts and outputs)
