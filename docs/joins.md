# Joins: nested loops and Grace hashing

`NestedLoopJoin` (NLJ) and `BlockNestedLoopJoin` (BNLJ) implement **inner equality join on one NONNULL signed INT64 key per input**. They preserve bag semantics: every matching logical pair appears exactly once, including identical duplicate rows. Grace Hash Join is described below. Aggregation and physical selection are implemented in aggregation/optimizer; multi-join enumeration, generic predicate engines, and SQL remain outside v1.

## Logical inputs and physical orientation

Construct either executor with logical **left, then right** children. `JoinPlan { left_key, right_key, outer=PhysicalOuter::Left }` refers to those children's output column positions. Force `PhysicalOuter::Right` to change execution orientation. Nested-loop join supports manual choice; optimizer [optimization](optimizer.md) evaluates both physical orientations.

The output Schema and every owned output Tuple always contain **all logical Left columns followed by all logical Right columns**, regardless of physical outer. Column names, types, VARCHAR bounds, and payload nullability are preserved. Right-as-outer maps the physical inner to logical Left before copying Values into the joined Tuple. Inputs may have different column counts, widths, and key positions. Neither join retains references into child tuples, page bytes, or temporary staging images.

Construction validates both nonnull children belong to this QueryContext, the side enum, both key indices/types/nullability, and the combined intermediate Schema. More than **16 columns** or more than **2048 maximum encoded bytes** raises DbError before either child's Init or Next. Exactly 16 columns/2048 bytes is supported. VARCHAR, DOUBLE, and nullable keys are rejected even if actual rows happen to contain non-NULL values. Those types remain valid in non-key intermediate payloads.

Equality uses `Value::as_int64()` and signed equality, without byte comparison, unsigned reinterpretation, subtraction, or endpoint arithmetic. INT64_MIN, negatives, zero, and INT64_MAX are supported. Both operators always advertise `Ordering{}` (unordered), including over an ordered IndexScan.

## API and replay contract

Public headers are `execution/nested_loop_join.hpp`, `execution/block_nested_loop_join.hpp`, and `execution/temp_scan.hpp` beneath `include/valencedb/`.

```cpp
BlockNestedLoopJoin join(query, JoinPlan{1, 0, PhysicalOuter::Right},
    std::make_unique<SeqScan>(query, SeqScanPlan{"left", true}),
    std::make_unique<SeqScan>(query, SeqScanPlan{"right", true}),
    32768); // Optional numeric local memory allowance in bytes.
join.Init();
Tuple row({});
while (join.Next(row)) {
    // Consume one owned logical Left+Right tuple.
}
query.FinishQuery(join);
```

`NestedLoopJoin` takes the same first four arguments without a memory allowance. BNLJ's final `optional<size_t> memory_bytes=nullopt` defaults independently to `QueryResources::work_memory_bytes`. Callers may pass a smaller positive allowance. Both own their two children through unique_ptr. No plan factory or algorithm registry is involved.

The physical **inner must obey the executor layer's Close -> Init replay contract**. Accepted existing inputs include SeqScan, IndexScan, Filter/Projection over replayable children, and TempScan. ExternalSort also obeys this contract by recomputing; repeated temporary allocations remain cumulative, and its private memory allowance is independent of the join's. The optimizer planner supplies automatic choice; there is no replay-capability type system. A custom nonrewindable executor is not a legal physical inner; callers must honor the established Executor contract. The outer is consumed once per execution, never replayed to recover a boundary row.

TempScan(QueryContext&, const TempRun&) copies a completed same-query descriptor. Its schema owner and context must outlive the scan; the source descriptor need not. Init constructs one TempReader, Close destroys it, and a later Init opens a new reader at the start. It returns owned tuples and unordered metadata, allocates no temporary pages, and inherits short-pin behavior. Two scans may read the same run independently. Foreign runs and closed queries fail clearly; unfinished runs cannot be passed because only Finish creates a descriptor.

Init starts only physical outer; it reads no rows and does not initialize inner prematurely. Next searches until one match or normal EOF. Repeated EOF is false and leaves output unchanged. Next before Init/after Close and repeated Init without Close are errors. Close is noexcept/idempotent, closes both children, releases tuples and BNLJ vector capacity, and preserves metrics/observed EOF. Close then Init resets counters and recomputes within the active query. Destructors close partial state. FinishQuery requires observed normal EOF; early cancellation uses Close followed by checked QueryContext::Close.

Execution errors propagate as DbError, never EOF. Join Init/Next close partial child/tuple state on exceptions; allocation failures are translated to DbError. A failure invalidates the entire result prefix. Close remains safe and QueryContext retains responsibility for checked scratch cleanup. No executor stores a PageGuard.

## Tuple-at-a-time NLJ

NLJ retains at most one current owned outer Tuple. For each outer row it initializes inner once, advances inner across successive Next calls, and emits one newly owned tuple on a match. After inner EOF it closes inner, discards outer, and fetches the next outer. Returning a match retains the same outer row and the inner cursor's continuation. There is no vector of matches or complete output.

`inner_scans` counts **every successful inner Init, including the first**, not just subsequent restarts. After full traversal it equals the physical outer row count. Empty outer yields zero scans. Empty inner is discovered once per outer row and terminates normally. NLJ memory stays bounded independently of result size; it does not allocate a work-memory-sized block or claim BNLJ's budget.

## BNLJ private memory and capacity

BNLJ groups decoded outer rows to reduce inner rescans. It reuses the executor layer's checked arithmetic and fixed schema charges:

```text
T(schema) = round_up_to_8(64 + 32*ncols + 2*max_encoded_row_bytes)
Touter = T(physical outer Schema)
Tmax = max(T(left), T(right), T(joined output))
P = 4096
S = 2*P + 3*Tmax + 1024
M = local override, or QueryResources.work_memory_bytes (default 1,048,576)
C_block = floor((M - S) / Touter)
```

Require **M > S before subtracting**, then require C_block >= 1 and a representable vector size. Invalid configuration fails before child Init/Next. BNLJ never silently falls back to NLJ. S includes two page allowances plus current/pending/output tuple scratch and fixed control state; using the joined output charge matters when it is much wider than either input. Shared helpers reject addition, multiplication, rounding, and subtraction overflow.

Init deliberately reserves C_block vector slots once; pushes never exceed that logical capacity. Capacity derives from the formula, not `vector.capacity()` or allocator inspection. Worst-case schema width governs even for empty/short actual VARCHAR values. Modeled peak is **S + C_block*Touter**, including reserved slots even for a short relation. This is an educational ownership model, not exact RSS, malloc accounting, or a global admission limit.

**F = shared BufferPool frames** and **M = private operator memory** are separate. Full queries require F >= 8, but C_block is independent of F. Each simultaneously live heavyweight operator receives its own numeric allowance. Nested-loop join does not reserve memory across a query tree.

## Blocks, pending rows, and continuation

BNLJ state consists of one bounded `vector<Tuple>`, at most one pending outer Tuple, at most one current inner Tuple, the next outer-block position, and small presence/EOF flags.

1. Start an empty block with pending, if present, moving it exactly once.
2. Fetch outer rows and append while the block has room. Once full, fetch one boundary row: keep it pending, outside the vector and charged to S. If that fetch instead sees EOF, mark outer exhausted. Never replay outer or discard the boundary row.
3. For a nonempty block, initialize inner **once**. An empty block at exhausted outer means join EOF without an inner Init.
4. Retain one inner tuple and compare it with each block row. Advance the block position before returning each match; the next Next resumes there with the same inner tuple.
5. After all positions are checked, discard the current inner tuple and fetch the next, resetting position to zero.
6. At inner EOF, close inner, clear/reuse the vector, and fill the next block. A final partial block gets one full inner scan.

The block never exceeds C_block and pending never exceeds one tuple. No inner/output collection exists. At C_block=1, the same state machine remains correct. A full block followed by EOF creates no extra empty block. All-equal A-by-B inputs emit exactly A*B outputs across any number of blocks.

Worked example with left physical outer, C_block=3:

```text
outer block = [1:A, 1:B, 2:C]
inner       = [1:X, 1:Y, 3:Z]

Next #1 -> A,X  (current inner X, next block position 1)
Next #2 -> B,X  (same inner X, next block position 2)
Next #3 -> A,Y  (C,X fails; advance to inner Y)
Next #4 -> B,Y  (same inner Y)
Next #5 -> EOF  (C,Y and all three pairs with Z fail)
```

This makes nine comparisons, four outputs, one block, and one inner scan. Another outer tuple already fetched after C would be pending and would start the next block; it is neither emitted twice nor lost. Right-as-outer uses the same traversal but assembles logical Left+Right output. Emission order is not a metadata promise.

## Metrics and representative evidence

Counters reset at Init and survive Close. They count operations to date on partial execution; formulas below describe full successful consumption.

| Counter | Meaning |
|---|---|
| outer_rows | Rows fetched from physical outer, including pending when present |
| inner_scans | Successful inner Init calls, including the first |
| candidate_comparisons | Key pairs compared; full consumption equals outer_rows * inner_rows |
| output_rows | Matching pairs returned to caller |
| outer_blocks (BNLJ) | Nonempty blocks whose inner Init succeeded; equals inner_scans |
| peak_block_rows (BNLJ) | Maximum populated vector size, excluding pending |
| pending_boundary_tuples (BNLJ) | Total boundary handoffs, retained one at a time |
| modeled_peak_bytes (BNLJ) | S + C_block*Touter after reserve; not RSS |

`NestedLoopJoin::Metrics()` returns `const NestedLoopJoinMetrics&`. BNLJ's accessor returns `const BlockNestedLoopJoinMetrics&`; `Memory()` returns `const BlockNestedLoopJoinMemory&` with `max_bytes`, `outer_row_bytes`, `scratch_bytes`, and `block_capacity`.

For fixed capacity, outer_blocks = ceil(outer_rows/C_block). An 11-row left and 8-row right, all keys equal, produce **88 rows** in both orientations: NLJ scans inner 11/8 times; BNLJ at capacity 3 scans it **4/3 times**, with pending counts 3/2 and comparisons 88 either way. Both match the independent encoded reference multiset. The join tests validate duplicate multiplicity, orientation, replay, bounded-memory state, fallback, failure handling, and pin cleanup.

## Logical scan cost versus measured I/O

Let R be physical outer and S_input physical inner (distinct from scratch reserve S above). The idealized logical scan models are:

```text
NLJ  = pages(R) + rows(R) * pages(S_input)
BNLJ = pages(R) + ceil(rows(R)/C_block) * pages(S_input)
```

Both compare every outer/inner pair. BNLJ reduces inner **rescans**, not equality comparisons. Complex children can do additional work; these formulas describe repeated relation scans.

The classic unified-buffer textbook formula is:

```text
pages(R) + ceil(pages(R)/(B-2)) * pages(S_input)
```

It assigns B-2 page buffers to outer blocks, with remaining buffers for inner/output. ValenceDB separates shared BufferPool frames F from private decoded operator memory M. Its block is charged in **decoded rows**, so it does not implement that exact classical B-page model.

Do not equate logical executor scan work, BufferPool fetches, BufferPool misses, DiskManager file reads, or OS/device I/O. The shared cache can satisfy repeated inner fetches; actual file reads can be lower than the logical scan formula. File reads themselves may hit the OS cache. IndexScan may perform index/heap accesses unlike sequential traversal. Tests measure file operations separately from scan counts.

Physical outer choice depends on outer row count, worst-case width, resulting C_block, and inner page count. Fewer rows or pages alone is insufficient. In the tested same-M example, **M=18,200, S=14,664** gives a 40-row narrow outer **C=29, 2 scans**, but a 5-row wide outer **C=2, 3 scans**. Both compare 200 pairs and produce the same 67 results. Nested-loop join exposes the choice; optimizer evaluates both orientations using this formula.

## Limits

Only inner equality joins with one NONNULL INT64 key are supported. No outer/semi/anti joins, composite keys, VARCHAR/DOUBLE keys, NULL-key semantics, inequality/arbitrary predicates, or useful output ordering is promised. The optimizer supplies automatic physical choice. Rewindable inner and an active single-query context are required. Output may be quadratic while private retained state is bounded. M is an educational per-operator model, separate from F and whole-query RSS. Grace Hash Join adds partitioned hashing without changing the nested-loop executor contracts.

## Grace Hash Join

`GraceHashJoin` avoids repeated full-input comparisons when partition pairs fit a hash build. Every execution **partitions both complete inputs into real TempRuns**, including small inputs. There is no in-memory-only shortcut. It supports the same inner equality on one NONNULL signed INT64 key and invariant logical Left+Right output as Nested-loop join, and always advertises unordered output.

```cpp
GraceHashJoin join(query, GraceHashJoinPlan{1, 0, 20000},
    std::make_unique<SeqScan>(query, SeqScanPlan{"left", true}),
    std::make_unique<SeqScan>(query, SeqScanPlan{"right", true}));
join.Init();
Tuple row({});
while (join.Next(row)) { /* consume owned logical Left+Right */ }
query.FinishQuery(join);
```

The public header is `execution/grace_hash_join.hpp`. `GraceHashJoinPlan` contains `left_key`, `right_key`, and `optional<size_t> memory_bytes=nullopt`. It does not reuse JoinPlan's caller-selected physical outer: GHJ chooses each pair's build side and, if necessary, fallback outer internally. Children are owned through unique_ptr. Construction validates same-context/non-null children, key indices/types/nullability, the joined 16-column/2048-byte limits, resource arithmetic and fallback feasibility, before either child starts.

### Partitioning and phase ownership

The retained phases are Closed, SelectPair, Probe, Fallback, and Done. Partition and Build are synchronous functions, not persistent states. Init creates p left writers, consumes left once to EOF, closes that child, and finishes every writer into a value descriptor. The local writer vector **and every writer object/private page are destroyed on return from Partition**, before the right-side call allocates its writers. It repeats exactly once for right using the same p. Both original children are closed before Init returns; Next never reopens them.

Each owned input tuple is appended once to `PartitionHash(key) % p`. PartitionHash converts signed INT64 to uint64_t before addition/mixing, so negative keys and both extremes have defined behavior. Its explicit integer mixer uses a fixed partition seed. The in-memory BuildHash uses the same mixer with a different fixed seed; no default implementation-dependent integer hash governs partition routing. Equal keys always route together. Neither hash is cryptographic.

The two vector<TempRun> arrays contain at most p sealed values each and remain bounded until Close. Descriptors borrow the operator’s immutable left/right schemas. Empty writers finish with zero pages. Each writer owns a private 4096-byte image; brief BufferPool guards copy/allocate pages. Writers are retained through unique_ptr for straightforward simultaneous partition construction. The complete writer vector dies before the opposite side or hash build begins; no reader/writer retains a guard across calls.

Init ends at SelectPair, with all partition writers gone and no hash state. Next processes corresponding `(Li,Ri)` only. It skips a pair with either side empty without constructing readers, a table, or fallback. Each finished pair's handles are destroyed before selecting the next.

### Local memory and side choice

All formulas reuse executor layer checked helpers; memory is modeled ownership, not exact allocator accounting or RSS:

```text
P = 4096
M = local override or QueryResources.work_memory_bytes
Tleft  = T(left schema)
Tright = T(right schema)
Toutput = T(joined schema)
T(schema) = round_up_to_8(64 + 32*ncols + 2*max_encoded_row_bytes)
S = 2*P + 3*max(Tleft, Tright, Toutput) + 1024
p = min(64, floor((M-S)/(P+256)))         require M>S and p>=2
Dghj = 128*p                            two run arrays; no diagnostic vector
A = M-S-Dghj                            require M>S+Dghj
Hside = Tside+96
build_capacity(side) = floor(A/Hside)
fallback_memory = M-Dghj
fallback_block_capacity(side) = floor(A/Tside)
```

Each partition reserves two 64-byte run allowances and P+128 for a writer, its pointer, and allocation overhead. Compile-time size checks protect those allowances. The planner calls the same capacity function as the executor.

Addition/multiplication are checked before use; every subtraction follows reserve validation. Vector/container size representability is checked. Both fallback outer capacities are derived at construction; at least one must be legal. Under current schema limits, a p-valid but row-infeasible fallback cannot occur: `M-S >= p*(P+256)` implies `A>=p*(P+128)>=8448`, whereas every legal T is at most 4672. Tests reject budgets just below p=2 and execute the minimum p=2 budget with maximum-width output. The explicit fallback guard remains, including representability checks.

For each nonempty pair compute checked `left_rows*(Tleft+96)` and `right_rows*(Tright+96)`. Choose the smaller **modeled byte footprint**, with ties choosing Left. A tested 10-row wide left has `10*816=8160` modeled bytes; a 12-row narrow right has `12*264=3168`, so Right builds despite having more rows. The mirrored case builds Left.

Before opening a build reader, compare the selected descriptor row count with its capacity. Too many rows go straight to BNLJ. Since both sides use the same A, if the smaller footprint cannot fit, neither can the larger. There is no partial hash output followed by a different strategy.

Representative two-INT64 schemas on both sides give Tleft=Tright=168, Toutput=264, S=10008. At M=20000, p=2, Dghj=256, both hash capacities are 36, fallback memory is 19744, and both BNLJ block capacities are 57. Tests cover selected build counts 35, 36, and 37: hash, hash, and fallback respectively.

### Bounded build and resumable probe

Each build constructs one `std::unordered_multimap<int64_t, Tuple, BuildHash>`. It sets max_load_factor(1.0f), deliberately reserves `min(partition_rows, build_capacity)`, and checks `table.size() < build_capacity` before **every new row insertion**. The smaller reserve is bounded by the published capacity; no allocator/bucket-size introspection determines admission. Every duplicate build tuple gets its own entry.

The entire build completes before any pair output. Then its reader and private staging page are destroyed and the opposite partition's probe reader opens. The table stays immutable throughout probe. State retains one owned probe Tuple and an equal_range iterator pair. Each Next copies logical Left+Right from one matching build entry and the probe tuple, advances the iterator, and returns. After the range ends, it replaces the probe tuple. Left build maps `(build,probe)` to `(Left,Right)`; Right build maps `(probe,build)` to `(Left,Right)`. A 2-by-3 duplicate key yields all six pairs, including identical payloads.

The runtime insertion check remains defensive even though sealed TempReader metadata normally makes it exact. If another build row would exceed capacity, close the build reader, destroy the entire hash object including buckets, destroy the local build tuple/page on return, and begin fallback before emitting. A small test friend directly calls Build with a reduced capacity to bypass the descriptor precheck: three rows retained, a fourth read triggers abandonment, zero hash outputs, and fallback emits the full 8-by-9=72 result. No fault scheduler or production corruption API was added. Detected temporary-page corruption still raises DbError under the executor layer; this fallback does not repair corrupt storage.

Probe EOF destroys the reader, table nodes/buckets, current tuple, and both pair handles. There is only one active hash table, never a table collection or output queue. Close likewise destroys all active state and both partition vector capacities, closes children, and preserves metric snapshots. The summary counters retain no partition records, relations, or tuples.

### Oversized pairs and skew fallback

Fallback creates a fresh Nested-loop join BlockNestedLoopJoin over **two TempScans of this pair**, passing the numeric `M-Dghj`. The descriptors remain live, and no hash allocation remains. BNLJ applies its existing S/Touter formula; there is no second full-M grant or generic sub-budget layer. Choose a legal physical outer giving fewer `ceil(rows/C_block)` inner scans; ties choose Left. The choice is local and deterministic, not optimizer join selection.

Next delegates directly to fallback.Next until EOF, then destroys the entire fallback before releasing the pair. TempScan replays the materialized inner partition, never an original child. The tested 120-by-130 all-key-7 pair exceeds both 36-row hash capacities and emits exactly **15600** rows through one fallback, with C_block=57 and three inner scans under M_fallback=19744. A wide-left/narrow-right fixture forces Right outer and matches standalone Nested-loop join BNLJ under the same reduced allowance.

There is deliberately no recursive repartitioning. BNLJ guarantees termination with bounded retained state, reuses Nested-loop join, and keeps the algorithm inspectable. Severe skew can make comparisons and output quadratic. Output cardinality never increases retained output state.

Worked example (tested at M=23300 with the two-INT64 schemas):

```text
p=3; Dghj=384; hash capacities=48; fallback C_block=76
Left:  L0(2 rows)  L1(90 rows)  L2(0)
Right: R0(3 rows)  R1(95 rows)  R2(0)
L0,R0 -> build Left, probe Right -> 6 streamed matches
L1,R1 -> BNLJ with M=22916      -> 8550 streamed matches
L2,R2 -> skip                   -> 0
Total: one hash pair, one fallback pair, one empty pair; 8556 rows
```

### Metrics, scratch, and logical transfers

`Memory()` returns GraceHashJoinMemory with the values in the formulas. `Metrics()` resets at Init and survives Close. Summary counters record hash/fallback/empty pairs, left/right successful builds and fallback outers, peak build rows, modeled peak build bytes (S+Dghj+reserved_rows*Hside), output rows, peak partition writers, and failed-build prefix rows. `completed_fallback_inner_scans` sums actual BNLJ inner scans only when each fallback reaches EOF; cancelled or failed partial pairs do not contribute.

Row/page counts remain solely in the sealed TempRuns. There is no per-partition metric vector or synthetic completed-read accounting. Tests decode partitions, check routing and tuple multisets, derive expected strategies and page scans from the descriptors, and compare expected transfers against BufferPool fetch deltas. Live hash-table and fallback block inspection verifies admission and reduced memory bounds. DiskManager temporary reads, writes, and allocations remain the source of completed physical file-I/O evidence, including work completed before a failure. Failed transfers propagate errors rather than incrementing these counters.

TempStore's existing `allocated_temp_pages`/`peak_temp_extent_pages` are cumulative **query** extent and include other operators. No subtraction on pair release occurs. Close and dropping descriptors perform **zero page-reclamation I/O**: no chain walk, reuse, or shrink. All pages, including released pairs, failed tails, and previous replay executions, count until checked FinishQuery or query cancellation resets scratch. TempScan/BNLJ fallback allocates no new temp pages. A downstream ExternalSort or a replay can exhaust the cap after old pair handles have been closed; tests cover both and propagate DbError, never truncated success.

With ideal equal packing, original input page counts N_R/N_S, and every pair fitting the second phase, classic GHJ costs `3*(N_R+N_S)`: read original inputs, write partitions, read partitions. It excludes output writing. ValenceDB's measurable logical approximation is:

```text
N_R + N_S + 2*(partition_pages_R + partition_pages_S)
```

This assumes the child scans cost those page reads and all partition pages participate. Empty-sided skipped pairs save their second-phase reads; per-partition tail waste changes packing. Logical temporary traffic is partition writes plus build/probe page scans or fallback rescans, including any abandoned build prefix. Physical file traffic must be measured separately because cached transfers need no file I/O.

For a fallback pair, **replace** the usual single read of both partitions with `pages(outer) + ceil(rows(outer)/C_block)*pages(inner)`. Do not also charge a nonexistent full hash scan. A defensive failed build adds its actually read prefix before the fallback; the separate row counter exposes this extra work, without claiming a page total for that prefix.

Algorithmic page-data passes differ from BufferPool fetches/hits/misses and DiskManager reads/writes. Physical allocation initializes pages and has separate counters. Cache hits can eliminate file reads, and file reads can hit the OS cache; neither ideal formulas nor BufferPool misses are physical device-I/O measurements.

### Lifecycle, composition, and limits

Close is noexcept/idempotent and releases all owned working allocations; no guard or raw frame reference escapes. Next after Close and repeated Init without Close reject under the executor layer. Close then Init repartitions replayed children once each; old pages remain charged. Repeated EOF leaves the output unchanged. Init/Next exceptions close working state and propagate DbError (allocation failures are translated); callers discard every output prefix. FinishQuery requires observed EOF; early cancellation uses Close then checked query.Close.

The integration `ExternalSort(GraceHashJoin(...))` consumes GHJ once, streams sorted output, and exercises external sort runs under an independent local allowance: tested GHJ M=65536, p=12 with F=8, and sort M=32768. Both use the same cumulative scratch cap. There is no whole-query scheduler or RSS cap, and no permission/admission machinery.

Limitations: inner join only; one NONNULL INT64 equality key; unordered output; no recursive repartitioning, Hybrid Hash Join, Bloom filters, or arbitrary predicates. The optimizer supplies automatic physical choice. Severe skew can be expensive through BNLJ. Temporary storage is append-only until query end. [Aggregation](aggregation.md) composes with GHJ, including a final ExternalSort, under independent local allowances. The join and integration tests exercise these composition and cleanup rules.

## Source and self-check

Read [NestedLoopJoin::Next](../src/execution/nested_loop_join.cpp), [BlockNestedLoopJoin::FillBlock/Next](../src/execution/block_nested_loop_join.cpp), and [GraceHashJoin::Partition/SelectPair/Build/BeginFallback/Next](../src/execution/grace_hash_join.cpp). [join_helpers.hpp](../src/execution/join_helpers.hpp) preserves logical Left+Right through physical swaps; [TempScan](../src/execution/temp_scan.cpp) gives fallback its rewindable inner.

NLJ and BNLJ both perform O(n_left*n_right) candidate comparisons, plus output construction; BNLJ changes scan multiplicity, not comparisons. Fitting GHJ has expected linear partition/build/probe work plus output under ordinary hash assumptions; skew fallback can restore quadratic comparisons/output. Memory remains bounded by each operator's model in all cases. No inner-match collection or complete-result vector is retained.

The tests exercise continuation state around emitted rows, pending-row ownership, partition-pair fallback, duplicate multiplicity, modeled side selection, and BNLJ fallback orientation.
