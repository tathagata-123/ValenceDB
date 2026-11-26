# V1 Phase 4: algorithm-first query processing

## Baseline and contracts decided before implementation

Baseline commit: `c7ee4eba8a26f2ae8a3596ed72b0ac3fd29c5dd4`, preserved on local
branch `archive/pre-v1-query-c7ee4eb` before implementation. The worktree was
clean. No AGENTS.md was found in the repository or its ancestors. No commit or
push is authorized. Complete Debug baseline build and regression executed:
13/13 suites passed in 70.74 seconds. Logs: `benchmark_results/v1_phase4/`.

Dependency inspection: the five operator source/header pairs, shared memory
formulas, Executor lifecycle, TempWriter/Reader exact descriptors, TempScan,
optimizer capacity/cost callers, operator/integration tests, benchmark emitter
and Python validator, and current operator documentation.

Contracts to change:

- BNLJ fills only while space remains. No boundary tuple, pending flag, or
  redundant outer EOF flag; the child's normal iterator EOF contract suffices.
  Early output therefore consumes at most one block of outer rows.
- Sort keeps one lookahead only to distinguish an exactly full private first
  run from a spilling sort. Later runs fill without lookahead. Initial run
  descriptors remain bounded; each materialized merge pass strictly reduces
  their count. Fan-in below two fails when input exceeds the private run.
- Grace trusts exact successful TempWriter row counts for fit selection. A
  build/count mismatch fails the query before exceeding the reserved table;
  it no longer abandons a partial build and retries using BNLJ. Oversized/skewed
  partition pairs still use BNLJ. Remove the special test friend and public
  partition hash; keep independent partition/build hash mixing internally.
- Remove redundant diagnostic counters: outer row/block copies, boundary
  handoffs, descriptor/head peaks already bounded by initial runs/fan-in,
  modeled byte peaks derivable from capacity formulas, aggregate input row
  counting and overflow flushes derivable from completed spill batches.
  Keep algorithm work, occupancy, spill and output counters.
- Retain common Executor error propagation/RAII, finite memory and scratch
  caps, real I/O errors, checked arithmetic, supported schemas/results,
  persistence, replay, duplicate multiplicity and order guarantees. No
  rollback, recovery or successful continuation after unexpected failure.

Implementation, pseudocode, measurements and executed validation follow.

## Removed mechanisms and reasons

| Mechanism | Replacement and reason |
| --- | --- |
| NLJ outer-row counter | Successful inner scans already count processed outer tuples. The outer tuple and inner cursor remain the algorithm's continuation state. |
| BNLJ pending tuple, pending flag, outer-exhausted flag and boundary fetch | FillBlock tests available capacity before calling Next. Executor already caches child EOF. No tuple needs to cross a block boundary outside the vector. |
| BNLJ outer-row/block counters and modeled-byte counter | Source-consumption tests check cardinality; inner_scans counts blocks. Memory() gives the reservation formula. |
| Sort pending flag and per-run lookahead | One local row provides first-run lookahead only. Later runs fill, sort and spill directly. |
| Sort OutputMode | An empty disk-run vector identifies private output; a nonempty vector identifies merge output. |
| Sort exhaustive is_sorted assertion | std::sort establishes the invariant; independent tests decode disk chains and check ordering and multisets. |
| Sort merge descriptor limit recheck | Initial spills enforce D. With fan-in at least two, each later pass has fewer descriptors. Both arrays still reserve bounded space. |
| Sort boundary, descriptor/head peak and modeled-byte diagnostics | Initial run count bounds all descriptor arrays; fan-in bounds heads. Retained-row occupancy and Memory() formulas preserve algorithm/budget evidence. |
| Grace bool-returning Build and secondary build-overflow recovery | SelectPair uses exact writer row counts. Build reserves that count, rejects extra rows before insertion, and rejects missing rows at EOF. It returns normally only after a complete build. |
| GraceHashJoinTestAccess and public PartitionHash | Tests use the public page format and an independent partition reference. PartitionHash is file-local; BuildHash remains private with a separate seed. |
| Grace Closed/Done phases | Common Executor owns initialization and EOF. SelectPair/Probe/Fallback are the only persistent algorithm phases. |
| Grace modeled build-byte counter | Published capacities and peak build rows establish the same modeled bound. The former failed-build-row metric was already absent in the baseline. No recovery-path-only metric remains. |
| Aggregate input-row instrumentation and metrics parameters in group/global loops | Source observers and result oracles test consumption. The loops now contain only accumulation and group boundaries. |
| Aggregate overflow_flushes and modeled hash-byte counter | On successful spilling, overflow flushes equal spill_batches minus the final batch. Capacity formulas provide the byte bound. |
| Aggregate temporary table ownership transfer | BuildGroups directly owns its table in table_. A fitting table is emitted; a spilling table is destroyed before sorting. |
| Aggregate empty-flush guard, post-insert capacity assertion and pre-sort empty-table assertion | Every flush is reached with a nonempty table, insertion follows the capacity branch, and BuildGroups explicitly releases the table before returning partials. |
| Aggregate Closed phase, redundant Done assignment at EOF, and Close-time sort metric copies | Executor caches EOF. Sort counters are finalized during successful sort Init, so later snapshots add no information. |
| Explicit hash max_load_factor(1.0f) settings | Newly constructed standard unordered containers already have load factor one. Reserve and logical row/group caps remain explicit. |

No per-operator catch/translation blocks remained to remove: Phase 3 already
centralized them in Executor. Its error boundary is unchanged. No storage,
buffer, index, optimizer algorithm, capacity formula, build target or supported
query operation was changed. Changes outside the five operator implementations
are limited to their test/benchmark callers and documentation.

Exact removed metric fields (source API changes):

- NestedLoopJoinMetrics: `outer_rows`.
- BlockNestedLoopJoinMetrics: `outer_rows`, `outer_blocks`,
  `pending_boundary_tuples`, `modeled_peak_bytes`.
- ExternalSortMetrics: `pending_boundary_tuples`, `peak_run_descriptors`,
  `peak_merge_heads`, `modeled_peak_bytes`.
- GraceHashJoinMetrics: `modeled_peak_build_bytes`.
- AggregateMetrics: `input_rows`, `modeled_peak_hash_bytes`, `overflow_flushes`.

Benchmark emitter and validator now use schema version 6. Historical JSON and
previous phase reports remain historical evidence; current operator documents,
benchmark documentation and README describe the new contracts.

## Textbook algorithms and source map

The pseudocode uses yield for a successful Volcano Next return: local traversal
positions that must survive yield are executor members. Join output is always
logical Left columns followed by Right columns, regardless of physical sides.

### Tuple nested-loop join

```text
initialize outer
for each outer tuple:
    initialize inner
    for each inner tuple:
        if keys equal: yield logical_left_plus_right(outer, inner)
    close inner
```

[NestedLoopJoin](../src/execution/nested_loop_join.cpp): DoInit initializes
outer; DoNext holds outer_row_/have_outer_ across returns and resumes the inner
executor; DoClose releases both children and the retained tuple. Constructor
swaps physical children/key positions when Right is selected as outer.

### Block nested-loop join

```text
initialize outer
repeat:
    block = []
    while block.size < capacity and outer has a next tuple:
        append that tuple
    if block empty: return EOF
    initialize inner
    for each inner tuple:
        for each tuple in block:
            if keys equal: yield logical_left_plus_right(block_tuple, inner)
    close inner
```

[BlockNestedLoopJoin](../src/execution/block_nested_loop_join.cpp): FillBlock
implements exactly the bounded fill and inner initialization. DoNext keeps
inner_row_, have_inner_ and position_ across matches. It clears the block only
after the complete inner scan. DoClose releases vector capacity. Capacity one,
partial final blocks and exact multiples use the same loop.

### External merge sort

```text
repeat:
    fill run vector while size < capacity and child has a next tuple
    if vector empty: stop run generation
    sort vector by key using std::sort
    if this is the first run:
        fetch one lookahead
        if EOF: keep this sorted vector as private output; return
        require fan_in >= 2
    write sorted vector as a temporary disk run; clear vector
    if first run: move lookahead into vector
release run-vector capacity
while number of disk runs > fan_in:
    for each group of at most fan_in runs:
        carry a singleton unchanged, otherwise materialize Merge(group)
    replace runs by the new descriptors
stream Merge(remaining runs) to the parent

Merge(group):
    open each reader and put its first (key, reader_number) in a priority queue
    while queue nonempty:
        pop the smallest head; yield its owned tuple
        advance only that reader; push its replacement head if present
```

[ExternalSort](../src/execution/external_sort.cpp): GenerateRuns/SpillRun,
MergePasses, OpenMerge/PullMerge and DoNext correspond to these steps. PullMerge
advances the selected reader before returning; only one owned tuple head per
input is retained. The final merge has no writer. Scratch pages and bounded
descriptors remain real TempRuns, not an unbounded in-memory replacement.

For n>k and k>=2, a pass leaves ceil(n/k)<n runs. GenerateRuns rejects k=0/1
as soon as its first lookahead proves external merging is needed, before the
first spill. Empty/private input remains legal even with insufficient merge
fan-in. Tests exercise both k=0 and k=1, including a one-row run capacity.

### Grace hash join

```text
Partition(left); Partition(right)
Partition(input):
    open p temporary writers
    for each row: append to writer[partition_hash(key) mod p]
    finish all writers, retaining exact row counts

for each corresponding partition pair:
    skip if either side empty
    choose build side with smaller rows * hash_row_charge
    if build row count exceeds capacity:
        stream BNLJ(pair), choosing outer with fewer blocks
    else:
        Build: reserve exact rows; insert every build tuple into a multimap
               require actual rows == descriptor rows, never exceed the count
        Probe: for each probe tuple:
                   for every matching entry in equal_range(key):
                       yield logical_left_plus_right(build, probe)
    release the pair's active readers/table/fallback
```

[GraceHashJoin](../src/execution/grace_hash_join.cpp): Partition, SelectPair,
Build, Probe, BeginFallback, ReleasePair and DoNext implement those steps.
The build reader dies before the probe reader opens. Duplicate build entries
are retained independently, producing all m*n equal-key matches. Different
mixed hash seeds for partition routing and table buckets preserve distribution
within a partition. Fallback remains an ordinary BNLJ over two TempScans using
M minus the partition descriptor reservation; it never replays original input.

### Global, hash and sort aggregation

```text
Global:
    state = empty
    for each input row: Update(state, measure)
    yield Finalize(state) once, including for empty input

Hash grouping:
    groups = bounded hash map; spill writer initially absent
    for each input row:
        find its group
        if new group and map full:
            open spill writer if needed
            FlushBatch: write every (key, raw_state); clear groups
        insert group if absent; Update(group, measure)
    if never spilled: emit Finalize for every group
    else:
        flush final groups; seal partial run; destroy map
        externally sort partial states by group key
        NextGroup: Combine adjacent partials; yield Finalize for each key

Sort grouping:
    reuse input ordering if it is on the grouping key
    otherwise wrap input in ExternalSort with the reduced allowance
    NextGroup: Update adjacent raw rows; yield Finalize for each key
    retain one boundary row to start the next group
```

[Aggregation](../src/execution/aggregate.cpp): ConsumeGlobal, BuildGroups,
FlushBatch, NextGroup, AggregateState::Update/Combine and Finalize implement
these steps. HashAggregate::DoInit selects the global, retained-table or
partial-sort path; DoNext emits it. SortAggregate's constructor reuses ordering
or wraps input once, so Close/Init replays the same tree.

Update/Combine use checked INT64 additions for selected COUNT and SUM/AVG
fields; MIN/MAX combine extrema. AVG retains raw sum/count until finalization,
so unequal spill batch sizes receive correct weighting. Empty global COUNT is
zero; selected SUM/MIN/MAX/AVG are typed NULL. Empty grouped input emits nothing.
Only selected fields are updated; unused SUM cannot overflow a COUNT-only query.

## Guarantees and remaining complexity

Results, multiplicity, clean persistence, replay, disk spilling and documented
ordering are preserved. NLJ/BNLJ/GHJ and HashAggregate remain unordered; sort
advertises ascending INT64 order without a stable tie order; SortAggregate
advertises group-key order. BNLJ's time of consuming the next block changes:
an error in its next outer row is now encountered after the current block's
inner scan. Any failed query still invalidates its entire result prefix.

Unexpected Grace build/count disagreement is now an error, with no BNLJ retry.
This relaxes the removed defensive recovery guarantee; it does not remove skew
fallback or silently return EOF. No arbitrary temporary-corruption recovery is
promised. Original I/O, arithmetic, allocation and resource-limit failures still
propagate through common Executor cleanup. Failed aggregate sort initialization
no longer copies partial sort diagnostics during Close; failure-time metrics
are not complete work accounting. The public partition-hash function and listed
metric fields are removed APIs. No on-disk format changes.

Necessary complexity remains in bounded-memory formulas, finite descriptors,
checked size/signed arithmetic, separate partition/build hashing, physical side
selection, saved iterator positions, one lookahead at group boundaries,
priority-queue merge cursors, raw aggregate states and phase-specific resource
release. These implement the algorithms and prevent unsafe access or overflow.
Unique ownership of readers/writers and optional hash tables releases staging
pages and buckets at phase transitions. Immutable schemas outlive borrowed
TempRun descriptors. AggregateTestAccess remains solely for existing raw-partial
and ownership tests; it supplies no runtime strategy or failure recovery path.

Budgets remain conservative per-operator educational models, not allocator or
process RSS limits and not whole-query admission. Shared BufferPool frames F
are independent of operator memory M. Scratch remains append-only until checked
query cleanup; finite initial descriptors and max_temp_pages still bound input
size. HashAggregate may accept a tiny fitting table but fail when spilling
requires an infeasible reduced sort budget. No recursive Grace repartitioning,
rollback, crash recovery or partially mutated database repair was added.

## Code size

Counts compare the baseline commit with the final files. Physical/nonblank lines
include comments; bytes are UTF-8 source bytes. Each row includes its source and
public header; aggregation includes both implementations in aggregate.cpp/hpp.

| Operator | Physical lines before → after | Nonblank before → after | Bytes before → after |
| --- | ---: | ---: | ---: |
| NLJ | 92 → 91 | 81 → 80 | 2,978 → 2,927 |
| BNLJ | 144 → 123 | 131 → 110 | 5,150 → 4,244 |
| External sort | 261 → 231 | 241 → 211 | 10,253 → 8,488 |
| Grace join | 268 → 263 | 248 → 242 | 11,007 → 10,470 |
| Aggregation | 354 → 336 | 334 → 316 | 15,278 → 14,299 |
| Total | 1,119 → 1,044 | 1,035 → 959 | 44,666 → 40,428 |

This removes 75 physical lines (6.7%) and 4,238 bytes (9.5%). The larger gain is
fewer independent states and responsibilities. Grace gains a named Probe method
and explicit mismatch failure checks while losing its recovery branch; its small
line reduction is intentional. Tests are not counted as production simplification.
Machine-readable counts are in `benchmark_results/v1_phase4/code-size.json`.

## Test coverage and executed verification

All 13 CTest suites/build targets remain. The artificial RuntimeCap scenario
(which called private Build with a deliberately false capacity and required
successful retry) was replaced by BuildCountMismatch. Its two cases add/remove
a physical build row and require a query error, bounded insertion, no fallback,
no success/EOF and checked cleanup. All ordinary skew cases remain.

Assertions tied only to removed metrics/private fields were revised, not used
to discard result, replay, memory or cleanup scenarios. Grace's partition audit
now decodes every physical scratch page, checks routing, per-partition row/page
counts, single forward chains, tails and encoded multisets. An independent
packing model predicts build/fallback decisions and BufferPool fetches. Released
resources are checked through query cleanup, child observers, zero pins/cache
mappings, replay and sanitizer execution. TempWriter descriptor tests remain in
the common execution suite.

| Operators | Retained or strengthened verification |
| --- | --- |
| NLJ and BNLJ | Empty sides, one row, duplicate multiplicity, signed extremes, asymmetric schemas and both physical orientations; capacities 1/3 and exact/partial/multiple blocks; match resumption; fixed-seed oracle bags; Close/Init and early cancellation; persistent scan/filter/project and TempScan pipelines; child/I/O failures and zero pins. ResumeWithoutLookahead now checks the exact outer Next-call count before a block finishes. |
| External sort | Empty/single/private/exact-capacity runs; std::sort reference order and duplicate bags; independent decoding of initial/intermediate disk runs; randomized inputs and wide/nullable payloads; capacity one, fan-in zero/one, descriptor cap; multiple merge levels, fan-in above buffer frame count, final streaming without new allocations; replay, early destruction, temp limits and I/O failure cleanup. |
| Grace join | Empty/disjoint/single inputs, duplicates, randomized oracle comparisons, build-side choice by bytes, exact fit and one-row-over fit, real all-equal skew, both fallback outers, mixed hash/fallback/empty partitions, p>F, maximum schemas; replay/cancellation; temp caps, child/I/O failures, actual disk-backed sort composition. BuildCountMismatch checks both extra and missing rows through public storage operations. |
| Hash/global aggregation | All COUNT/SUM/MIN/MAX/AVG selections, empty and explicit single-row cases, full tables with existing/new keys, capacity one, repeated groups across multiple spills, raw partial states, weighted AVG, checked overflow and unused-field behavior, independent map references and randomized cases; wide rows, scale spill, replay/cancellation and failures. |
| Sort/global aggregation | Same semantic/reference cases, unordered external sorting, ordered IndexScan reuse, one group spanning the input, boundary-row continuation, strict group ordering, replay and early output; ordered Init consumes no child tuples. Integration compares initialization fetches with a standalone index descent. |
| Cross-component regression | Persistent/reopened pipelines, index order through filters/projections, forced NLJ/BNLJ/GHJ with both aggregate methods, Grace → spilling HashAggregate → ExternalSort → Projection, randomized storage/index tests, common resource/lifecycle checks and benchmark exact reference validation. |

Executed results:

- Baseline complete Debug build and regression: 13/13, 70.74 seconds.
- Focused implementation regression: all operator suites and optimizer passed;
  the revised integration assertion failed because IndexScan initialization
  legitimately descends the tree. It was corrected to compare with standalone
  index initialization, then the full integration executable passed all five
  groups. A separate corrected fan-in filter ran and passed its boundary group.
- Complete final Debug build/regression: 13/13, 100.05 seconds.
- Complete final Release build/regression: 13/13, 49.19 seconds.
- Release benchmark smoke: 63/63 successful, reference-validated cases. A separate
  recursive audit verified schema version 6 and absence of all removed algorithm
  fields in every record.
- Complete final ASan/UBSan build/regression: 13/13, 129.25 seconds, with no
  sanitizer diagnostics. Final complete builds contain no compiler warnings or
  errors. Stale-reference searches across headers, sources, callers, tests,
  benchmarks and current documentation, plus git diff --check, passed.

The first implementation build found the removed public hash caller and an
incorrectly assumed test lifecycle accessor; both were fixed. Initial Release
and sanitizer runs still contained the old integration assertion and failed that
suite; rebuilds/reruns use the corrected assertion. One manual test filter used
`fan_in` instead of `fanin_descriptor` and ran no cases; the corrected command
passed. These unsuccessful logs are retained, not counted as passing evidence.
Build/test overlap affects durations; these are verification timings, not a
performance comparison or speedup claim.

Disk execution evidence from the successful smoke run:

| Case | Algorithm evidence | Temp allocations | File-page reads / writes |
| --- | --- | ---: | ---: |
| sort_m_24576 | 21 initial runs, 4 intermediate passes, streamed fan-in 2 | 97 | 96 / 96 |
| join_skew_ghj_auto | 4 hash pairs, 1 skew fallback, 2 inner scans, 8,697 results | 12 | 0 / 0 |
| aggregate_spill_hash_seq | 6 batches, 649 partials, 19 sort runs, 4 passes, 350 groups | 64 | 31 / 50 |
| aggregate_spill_sort_seq | 12 sort runs, 2 passes, 350 groups | 32 | 17 / 23 |
| aggregate_few_sort_index | Reused ordering, 4 groups, no sort | 0 | 0 / 0 |

Allocations initialize real temporary-file pages; reads/writes exclude those
initialization writes. The small skew fixture remains cached. Larger GHJ sort
composition tests explicitly require nonzero temporary file reads and writes.
All 63 successful records report zero scratch extent, temporary cache mappings
and temporary pins after cleanup. No unbounded in-memory substitute is used.

Reproduction commands (raw logs/JSON are in ignored `benchmark_results/v1_phase4/`):

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 4
ctest --test-dir build --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 4
ctest --test-dir build-release --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 4
ctest --test-dir build-sanitized --output-on-failure
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --output benchmark_results/v1_phase4/smoke.jsonl
build/valencedb_external_sort_tests fanin_descriptor
build/valencedb_integration_tests
git diff --check
```

The final HEAD and archive branch both remain at the recorded baseline commit.
All implementation changes are uncommitted; nothing was pushed.
