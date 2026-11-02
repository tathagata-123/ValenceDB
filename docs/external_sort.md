# External merge sort

`ExternalSort` sorts one **NONNULL INT64** output column ascending and preserves the child's complete Schema and tuple multiset. It is a physical Executor, not a SQL ORDER BY frontend. Equal-key order is unspecified. VARCHAR, DOUBLE, and typed NULL can occur in payload columns; they are never compared. The selected key itself must be nonnullable INT64 according to its Schema, even when actual nullable-key rows happen to contain values.

An input can exceed private operator memory. The implementation therefore follows explicit phases:

```text
bounded chunk + std::sort -> sealed temporary runs
                         -> materialized multiway passes as needed
                         -> final merge streamed by Next
```

The only retained tuple collection during generation is one bounded chunk. There is no complete external-result vector or final sorted TempRun.

## API and lifecycle

Include `valencedb/execution/external_sort.hpp`. Construct directly like the other executors:

```cpp
ExternalSort sort(query, ExternalSortPlan{2, 24576},
                  std::make_unique<SeqScan>(query, SeqScanPlan{"input", true}));
sort.Init();
Tuple row({});
while (sort.Next(row)) {
    // Consume owned row; discard the whole result prefix if this query later fails.
}
query.FinishQuery(sort);
```

The plan contains `column` and optional `memory_bytes`; omitting the override uses QueryResources. One ascending key is the entire configuration: descending and multiple-key plans are not representable. Invalid column, VARCHAR/DOUBLE/nullable key, foreign/null child, and impossible memory configuration raise DbError before child Init or consumption. Schema construction already enforces the supported width/column limits. QueryContext validates resources and requires at least eight BufferPool frames.

Construction owns a copy of the output Schema shared by reference with every run. `OutputSchema()` preserves names, positions, types, widths, nullability, and schema kind. `OutputOrdering()` is exactly `{true, column}`, regardless of child ordering.

`Init()` reserves bounded generation state, initializes the child once, generates runs, closes the child, performs required intermediate passes, and opens the final readers. It never materializes the complete final sorted relation. A small or empty input remains on the private-run path. `Next(out)` moves one owned tuple to the parent; repeated EOF is false and leaves `out` unchanged. Base Executor tracks initialization and observed EOF. The only retained output mode selects private-run iteration or streamed merge; generation and intermediate merging are synchronous Init work. The next-pass descriptor vector is local to MergePasses and dies before final output.

`Close()` is noexcept and idempotent. It closes the child, destroys heap/readers, releases row-vector capacity, drops descriptors and their capacities, and preserves the base EOF marker. It performs no per-page reclamation. Close then Init recomputes from the child in the same active query; old scratch pages remain charged. A second Init without Close is an error. Early cancellation uses sort.Close followed by query.Close. FinishQuery requires observed normal EOF and performs checked whole-query cleanup.

Init and Next unwind sort-owned state on execution failure. Model exhaustion, descriptor overflow, insufficient required fan-in, cumulative scratch exhaustion, and temporary I/O failure throw DbError, never EOF or truncated success. Allocation failures during execution are also translated to DbError. Callers must discard all output from a failed execution.

## Modeled private memory

M is **per heavyweight operator**, independent of BufferPool frames and other operators' allowances. Default M is **1,048,576 bytes**. Default cumulative scratch cap is **16,384 pages**. Neither is a whole-query RSS guarantee.

All row/scratch/arithmetic helpers are the shared execution-memory helpers in `work_budget.hpp`:

```text
P = 4096
Tinput = T(schema) = round_up_to_8(64 + 32*ncols + 2*max_encoded_row_bytes)
S = 2*P + 3*Tinput + 1024
D = min(4096, floor(M / (16*64)))         require D >= 1
R = 2*D*64                              two value-descriptor arrays; R <= M/8
A = M - S - R                           require M > S+R before subtraction
C_sort = floor(A / Tinput)              require C_sort >= 1
k = min(64, floor(A / (P+Tinput+128)))
```

Addition and multiplication are checked, and subtraction follows reserve validation. Vector size representability is checked before allocation. Merge fan-in must be at least two **when a second run is needed**; a single private run can succeed with k=0 or k=1. A configuration satisfying M >= 4P can still fail for a wide Schema. Maximum supported rows (16 columns, 2048 encoded bytes) have T=4672 and S=23232, already above 4P without descriptors.

S covers fixed control state, two staging-page allowances, and current/pending/output tuples. Each merge input additionally charges a private reader page, one decoded head, and 128 bytes of cursor/heap metadata. These are conservative educational charges, not measurements of object sizeof, allocator overhead, or RSS.

`ExternalSort::Memory()` exposes these derived values as `ExternalSortMemory`: `max_bytes`, `row_bytes`, `scratch_bytes`, `descriptor_capacity`, `descriptor_bytes`, `run_capacity`, and `merge_fan_in`.

Representative three-column Schema: INT64 id, VARCHAR(256) payload, INT64 key; max encoded width=278, T=720, S=11376.

| M | D | R | C_sort | k |
|---:|---:|---:|---:|---:|
| 24,576 | 24 | 3,072 | 14 | 2 |
| 32,768 | 32 | 4,096 | 24 | 3 |
| 131,072 | 128 | 16,384 | 143 | 20 |
| 1,048,576 (default) | 1,024 | 131,072 | 1,258 | 64 |

## Bounded run generation and the pending tuple

Reserve C_sort tuple slots once. Child tuples are owned. Fill the chunk without ever pushing beyond C_sort, then fetch one boundary tuple to distinguish exactly-full EOF from another run. When a full chunk is followed by a tuple, move that tuple into **one pending Tuple**, charged to S. Sort and spill the chunk, clear its elements, then start the next chunk by moving in pending. There is no child rewind or duplicate fetch at any boundary.

At exact capacity followed by EOF, the sorted chunk stays private and no empty run is fabricated. Empty input has zero runs/pages; one row needs no temporary pages. If the boundary tuple proves multiple runs are necessary but k<2, reject immediately. No short actual VARCHAR payload can increase C_sort: worst-case schema charge always governs capacity.

Sorting uses signed `as_int64()` comparisons with `<`, never serialized bytes or subtraction. Equal keys compare false in both directions. There is no tie RID, deduplication, or stability promise. One bounded std::sort is the intended algorithm.

Before each spill, check current run count < D. Create a local TempWriter borrowing the immutable schema, append the sorted tuples, and push the sealed value returned by Finish into vector<TempRun>. There is no per-run heap allocation, schema copy, page-ID list, or stable-address requirement. Debug builds assert chunk sortedness; tests independently decode all materialized run chains, including obsolete runs.

If the next initial run exceeds D, fail explicitly. No descriptor override is needed for tests: at M=24576, C_sort=14 and D=24, 337 rows exceed the cap. This finite input-run limit is intentional; generation does not merge early to bypass it.

Once generation enters external mode, destroy the chunk's **capacity** before allocating merge input pages. Keeping only `rows.clear()` at this transition would retain an entire phase's allocation; the implementation swaps with an empty vector. The two descriptor arrays are each deliberately reserved to D and reused.

## Multiway passes and final streaming

Each merge group has at most k TempReaders and one decoded head per nonempty reader. A standard `std::priority_queue` orders small `(signed key, input index)` entries. Each input owns the single Tuple head; the heap stores no duplicate Tuple. Pop the smallest entry, move its head to the caller/writer, advance only that reader, and push its replacement entry. Destroy an exhausted reader immediately. No input contributes two heap entries.

Readers use the existing private staging images: briefly fetch through BufferPool, copy page, release the guard, then decode rows. No sort state owns PageGuard. Fan-in depends on M, independently of F. Tests merge **11 readers with F=8**, observing peak pins=1.

While current run count > k, partition current descriptors into groups of up to k. Merge each multi-run group through a writer into a completed TempRun in the next array. A singleton group carries its descriptor forward without page I/O. Destroy each group’s readers and writer before processing the next group. At pass end, clear current and swap the two arrays. There is no collection per historical merge level.

Once run count <= k, open readers and initialize the final heap; **do not create a final writer or relation**. Every Next performs one merge step directly into the parent's output. No output buffer grows with result size. Empty final heap means EOF. The private path instead moves successive rows from its bounded sorted chunk.

Worked example for the three-column Schema, M=32768, C_sort=24, k=3, 97 input rows:

```text
initial: R0(24) R1(24) R2(24) R3(24) R4(1)
pass 1: R0,R1,R2 -> A(72)
        R3,R4    -> B(25)
final:  stream merge A,B -> parent, one tuple per Next
```

There is no C(97) final TempRun. A larger tested example uses M=24576 and 309 rows: **23 -> 12 -> 6 -> 3 -> 2** runs, four materialized passes, then a two-way streamed final merge. Singleton groups in the 23- and 3-run passes carry forward. There are 44 materialized runs total (23+11+6+3+1), not 45.

## Temporary lifetime and measurements

The 64-byte descriptor allowance bounds sizeof(TempRun). Each merge input’s P+128 allowance includes its reader’s copied descriptor, page, cursor, allocation bookkeeping, pointer, and heap entry; RowMemoryCharge separately covers the tuple head and its owned data. Compile-time size checks enforce these bounds. Fixed scratch covers the output writer and transient descriptor copies.

All runs share the query's one scratch file. Dropping obsolete descriptors removes value metadata only. Their pages are neither traversed for freeing nor recycled, and their PageIds remain charged against max_temp_pages. This includes incomplete output runs on failure and previous replay state. Close never shrinks the file. FinishQuery/checked query.Close discard temporary cache entries after pin/handle checks, then reset scratch through QueryContext cleanup.

The 309-row full-width fixture cumulatively allocates **107 pages**. A separate cap test fits five initial one-page runs under a six-page limit, allocates one intermediate output page, then throws when its next page would exceed the cap. Replay tests show two original pages remain charged: under cap=3, replay allocates the third page then fails on the fourth. No successful output prefix is reported in either failure case.

`Metrics()` exposes per-execution counters (reset at Init, preserved at Close):

| Field | Meaning |
|---|---|
| initial_runs | Sorted initial chunks, including one nonempty private run; empty input is zero |
| materialized_runs | Successfully sealed initial and intermediate output runs; singleton carry adds zero |
| intermediate_merge_passes | Completed materialized levels only; excludes final streamed merge |
| final_merge_fan_in | Actual number of final readers, zero on the private path |
| final_merge_streamed | Final external reader/heap state was successfully initialized |
| pending_boundary_tuples | Total boundary tuples retained one at a time, not simultaneous count |
| peak_retained_rows | Maximum normal chunk rows or merge heads; fixed scratch tuples excluded |
| peak_run_descriptors | Maximum size of either bounded descriptor vector |
| peak_merge_heads | Maximum heap heads, always <= k |
| modeled_peak_bytes | Maximum S+R plus C_sort*Tinput or merge inputs*(P+Tinput+128) |

The generation charge is `C_sort*Tinput`, even for a short chunk, because its vector capacity is reserved. The phases reuse M. No value claims actual RSS. TempStore's `allocated_temp_pages`, `peak_temp_extent_pages`, and `temp_relations_created` remain cumulative query metrics. Existing DiskManager counters expose physical file-page reads/writes and allocation initialization operations; BufferPool counters expose fetches/hits/misses/evictions/pins. There is no new generic metric framework or claim that these layers are interchangeable.

## Idealized page-data-pass model

Let N be input pages, assume all run levels have total size approximately N with equal packing, r=ceil(input rows/C_sort), and r>1, k>=2. Let t=ceil(log_k(r)) and L=t-1 completed materialized intermediate levels. Ignore allocation initialization, cache behavior, carried singleton savings, and downstream work for this idealization.

Counting the child input read as N, a completely materialized sort costs approximately `2N*(1+t)`: read input/write initial runs, then t read/write merge levels. This ends with the sorted relation **stored**, before a parent scans it. Our streamed version costs approximately `2N + 2NL + N = (2t+1)N`: input read, initial run write, L read/write intermediate levels, and one final input-run read. It saves approximately N final-write transfers on that convention. If the alternative also scans its final relation to deliver tuples, streaming saves another N final-read transfers, approximately 2N in total.

Counting **temporary traffic only**, subtract the original input read: streamed sort costs approximately `(2L+2)N`. A child need not itself perform N reads (it might be another executor). A private run has no temp transfers. Singleton carry-forward and partially consumed final output reduce work relative to full-level assumptions.

These are data-pass formulas, not measured file I/O equations. Every physical allocation initializes a zero page; DiskManager counts it in `temporary_allocations`, separately from `temporary_writes`. Report both rather than hiding the initialization cost. BufferPool hits may eliminate physical reads, and dirty eviction may cause writes during a nominal read phase. Per-run partially filled pages and slotted-page overhead change total pages per level. Early downstream Close avoids remaining final reads, but Init has already generated runs and intermediate passes. Actual savings depend on packing, cache residency, consumption, and whether downstream would otherwise require materialization. Raw-page test audits add their own reads and are performed after reported algorithm I/O samples.

## Invariants, verification, and limits

Every sealed run and final output is ascending by signed key. Normal retained rows <= C_sort; at most one pending boundary tuple exists. Each descriptor vector <= D. Each active input has at most one head, heap size <= active readers <= k. No temporary guard survives a call. Generation capacity is destroyed before merging, and group reader/writer capacity is destroyed between groups. Duplicates and complete payload bytes survive exactly. The complete final result is never materialized, and released scratch pages remain allocated until query end.

Tests cover private/empty/single/exact-capacity paths, pending rows with a nonrewindable child, signed extremes, identical duplicates, arbitrary VARCHAR bytes including NULs, maximum-width rows, nullable non-key payloads, independent sorted-run inspection, unique-key exact oracle order, multiset equality, fixed-seed randomized stress, multiple merge levels, minimum fan-in and descriptor failures, early Close/destruction/replay, cumulative/merge-time temp limits, injected child/write/read failures, pipeline metadata, and zero pins.

Bounded chunks plus std::sort keep generation straightforward and expose the memory/run-count tradeoff directly. Replacement selection would add a second substantial algorithm and more state under the 8,000-core-line budget; it is deliberately excluded. Other limits: one ascending INT64 key, no NULL key ordering, no stable tie order, no parallel sorting, finite run-descriptor cap, append-only scratch, and educational modeled memory rather than exact RSS. Nested-loop joins and [Grace Hash Join](joins.md) use the same temporary-storage and memory model. ExternalSort(GHJ(...)) uses independent local allowances and a shared cumulative scratch cap; [Aggregation](aggregation.md) also reuses this sorter for partial states and unordered raw input. [Optimizer selection](optimizer.md) shares these capacity formulas and models the streamed final merge.

## Source and self-check

The concrete state machine is [external_sort.cpp](../src/execution/external_sort.cpp): `ComputeSortMemory`, `GenerateRuns`, `SpillRun`, `MergePasses`, `OpenMerge`, `PullMerge`, and `CloseMerge`. Each initial chunk takes O(c log c) comparisons; across n input rows generation is O(n log C_sort). Each merged row costs O(log k) heap work per merge level. These are CPU bounds; the pass formulas above describe logical page transfers. Output remains streaming even if its volume exceeds M.

The external-sort tests include explicit boundary handoff, multi-pass merge, retained-capacity, descriptor-cap, and temporary-page-limit cases; the benchmark suite exercises memory and frame-count sweeps.
