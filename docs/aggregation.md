# Aggregation

ValenceDB implements global aggregation and grouping on one NONNULL INT64 column. A single NONNULL INT64 measure supplies any requested combination of COUNT, SUM, MIN, MAX, and AVG. This is an executor API, with no SQL GROUP BY frontend. The optimizer supplies the physical algorithm choice.

## Configuration and results

Include `valencedb/execution/aggregate.hpp`. Both executors own one child and accept the same compact specification and optional numeric local memory override:

```cpp
AggregateSpec spec{0, 1, true, true, true, true, true};
HashAggregate aggregate(query, spec,
    std::make_unique<SeqScan>(query, SeqScanPlan{"input", true}), 32768);
aggregate.Init();
Tuple row({});
while (aggregate.Next(row)) { /* consume owned output */ }
query.FinishQuery(aggregate);
```

AggregateSpec fields, in order, are optional `group_column`, optional `measure_column`, then bool `count`, `sum`, `min`, `max`, `avg` (all flags default false). An absent grouping column means global aggregation. COUNT(*) is count=true without a measure. Supplying a measure for COUNT validates that column and expresses COUNT(measure); supported measures are nonnullable, so both counts agree. Other functions require a measure. At least one output is required.

Constructors validate child/context ownership, column existence, NONNULL INT64 key/measure types, output Schema, resource limits, and local memory arithmetic before child Init. Nullable, DOUBLE, and VARCHAR keys/measures reject even if actual rows would contain usable values. Unused input payload columns can have any legal intermediate type. Output construction reuses Schema's 16-column/2048-byte checks; the largest supported aggregate result is only six columns/52 bytes, so an oversized result cannot be expressed with AggregateSpec.

Output columns have deterministic names/order, including only requested fields:

```text
[group if grouped], count, sum, min, max, avg
```

The key, COUNT, SUM, MIN, and MAX are INT64. AVG is DOUBLE. Grouped results are NONNULL: every emitted group has at least one row. COUNT is always NONNULL. Global SUM/MIN/MAX/AVG columns are nullable, even when a particular execution is nonempty.

Empty grouped input produces zero rows. Empty global input produces exactly one row: COUNT=0, SUM/MIN/MAX typed INT64 NULL, AVG typed DOUBLE NULL. A later ExternalSort rejects nullable aggregate keys based on Schema metadata, and always rejects DOUBLE keys. No NULLS FIRST/LAST semantics are introduced. A grouped INT64 result can be a downstream sort key because its metadata proves it NONNULL.

## Raw state and arithmetic

AggregateState contains initialized `int64_t count, sum, min, max` and `bool has_value`. Its explicit `Update(value,spec)` and `Combine(partial,spec)` helpers share the selected-field rules:

| Requested output | Fields used |
|---|---|
| COUNT | count |
| SUM | sum, has_value |
| MIN | min, has_value |
| MAX | max, has_value |
| AVG | sum, count, has_value |

Overlapping functions reuse each field once. COUNT-only never reads or adds a measure. MIN-only never checks an unused sum/count. Two INT64_MAX measures therefore successfully produce COUNT=2 or MIN=INT64_MAX. Extrema initialize through explicit presence, rather than a sentinel that confuses empty input with a real extreme value.

Both updating raw input and combining partial states check signed INT64 addition **before** adding. Overflow throws DbError. State after an exception is discarded with the failed query. Helper tests exercise COUNT overflow directly, while production-row tests exercise SUM overflow during updates and spill recombination.

Checked partial sums make intermediate overflow depend on accumulation order. For example, MAX, 1, -1 overflows before cancellation, whereas MAX, -1, 1 succeeds. Hash spilling and sorting may alter combination order. ValenceDB promises equivalent results for successful nonoverflowing executions; it does not promise that every physical ordering of a cancellation sequence succeeds.

AVG is finalized once as `double(sum) / double(count)`. Partial state always retains integer sum/count. For key 7, an early batch with 10,20 has sum=30/count=2; a later batch with eight 100s has sum=800/count=8. Combine to sum=830/count=10 and return **83**, rather than averaging 15 and 100 to get 57.5. Another example, sum/count=100/2 and 100/8, yields 200/10=20, not 31.25. Tests check real uneven spilled batches and separately place MIN/MAX in different batches.

## Global execution

Both APIs use one state and consume their raw child once. They allocate no hash table, temporary relation, or sorter. HashAggregate consumes during Init, then its first Next finalizes one row. SortAggregate opens its child in Init and consumes/finalizes on its first Next. The following Next observes EOF. This remains constant state for arbitrarily many input rows, subject to checked arithmetic and child limits.

## Bounded HashAggregate

Grouped execution follows explicit phases: ConsumeHash, EmitHash if everything fits, or FlushFinal, SortPartials, CombinePartials; global execution has EmitGlobal, and lifecycle includes Done/Closed.

The group table is an ordinary `unordered_map<int64_t, AggregateState>`. One bounded reserve sets max_load_factor(1.0f) and reserves the derived capacity. Admission checks the logical number of groups, without querying buckets or allocator internals.

For every owned input row, first look up its key. Existing groups update in place, including when the map is full. A new group inserts if there is a slot. A new key at capacity triggers a flush:

1. On the first overflow, create one TempWriter borrowing the immutable partial schema.
2. Append every map entry as a raw partial-state row.
3. Count one completed nonempty spill batch, then clear entries while keeping the reserved buckets for this hash phase.
4. Insert/update the triggering input row exactly once and continue the original child.

The loop's owned Tuple remains alive throughout the flush, so no replay, duplicate fetch, or lost trigger is necessary. There is only one writer accumulating the partial page chain across all batches. Repeated keys reduce table pressure; row count alone never causes a spill. Tests fill the table exactly and continue updating existing keys, and also perform 20,000 updates across two keys without spilling.

After the final partial batch, Finish publishes one TempRun stored in optional<TempRun>. TempScan copies that descriptor, and the writer object is destroyed before sorting. The existing combine scratch allowance covers these small value copies and reader metadata; the separate page allowance covers TempScan staging.

If no overflow occurred, close the child and stream finalized rows directly from the stable bounded map. There is no separate collection of output rows and no temporary allocation.

If any overflow occurred, EOF requires flushing **every remaining map entry** as the final nonempty batch. Finish the writer, destroy the writer and its private page, close the consumed raw child, and destroy the **entire map including buckets**. The local input Tuple has also left scope. Only then create and initialize the internal ExternalSort over a TempScan of the completed partial run. A source assertion and test inspection check this ownership transition. Merely clearing the map would retain its large reservation and is insufficient here.

Worked example with capacity=2:

```text
raw groups: 1,2,3,1
1,2:        map={1,2}, full
3:          spill partial(1), partial(2); map={3}
1:          map={3,1}
EOF:        final flush partial(3), partial(1); finish
release:    writer and complete map destroyed
sort:       partial keys 1,1,2,3
combine:    one result for 1, one for 2, one for 3
metrics:    overflow_flushes=1, spill_batches=2, spill_records=4
```

The partial schema is fixed, nonnullable, and all INT64:

```text
group, count, sum, min, max, has_value (0 or 1)
```

Unused fields remain initialized zero and are ignored during combination. There is no partial DOUBLE average. The operator owns one immutable partial Schema that outlives its relation, TempScan/reader, and sorter.

The ExternalSort operator sorts these partial rows by column 0. It retains bounded runs, uses real temporary pages and intermediate merge passes, and streams its final merge. It never sees the raw child. Sorted equal keys combine through AggregateState::Combine. On a key boundary, retain one owned lookahead partial, emit the current finalized state, and resume that next key on the following Next. EOF emits the last group. No complete final result is materialized.

HashAggregate always advertises unordered output. Its spill path's internal sorted order is not an API promise.

## SortAggregate and lookahead

For grouped input, exact metadata `Ordering{true, group_column}` enables streaming adjacent-group aggregation. No ordering is inferred from heap layout or hash output. For example:

```text
IndexScan(group key) -> SortAggregate -> ascending groups, no sort runs
SeqScan             -> SortAggregate -> ExternalSort first, then ascending groups
```

This is the aggregate operator's required-order check; the optimizer shares the same Ordering proof while costing alternatives. Group key input column 1 still becomes output column 0. Sorting on a different input column does not qualify.

On unordered input, the constructor creates the existing ExternalSort with a reduced numeric allowance and moves the child into it. Init consumes raw input once through that sorter. The adjacent-group loop retains one current AggregateState and at most one lookahead Tuple, with no hash map.

For input 1:a,1:b,2:c, the first Next reads c to discover the boundary, saves that row, and emits group 1. The second Next starts group 2 from c exactly once. If EOF follows c, it still emits group 2. Repeated EOF returns false without modifying the caller's output. Ordered Init does not preconsume input; a single group may naturally require reading all its rows before that group's output is ready.

Grouped SortAggregate advertises `Ordering{true,0}` and emits strictly increasing distinct keys. Global aggregation has no useful key order. Both algorithms share small state/finalization and adjacent-group helpers, without a generic aggregate-function hierarchy or phase framework.

## Modeled memory and phase ownership

M is the numeric local override or QueryResources.work_memory_bytes. It is per heavyweight operator, separate from BufferPool F and other operators' allowances. M is not exact allocator use, process RSS, or whole-query admission.

```text
P = 4096
T(schema) = round_up_to_8(64 + 32*ncols + 2*max_encoded_row_bytes)
Tmax = max(Tinput, Toutput, Tpartial)    grouped HashAggregate
Tmax = max(Tinput, Toutput)             global / SortAggregate
S = 2*P + 3*Tmax + 1024
G = 192
Ccombine = 3*Tmax + 1024
hash_group_capacity = floor((M-S)/G)    require M>S and capacity>=1
hash internal Msort = M-Ccombine-P
sort aggregate Msort = M-Ccombine      unordered grouped input
```

Checked helpers precede addition/multiplication/subtraction. Global and ordered streaming require M>=Ccombine, without reserving unnecessary staging pages. Grouped HashAggregate uses S for current/trigger/output tuples, spill staging, and fixed state. Modeled peak hash bytes charge S plus the full reserved group capacity times G, not just live entries.

The hash-created TempScan's reader owns one private staging page outside the sorter's own model, explaining the extra P subtraction. That reader is destroyed when the sorter closes its input after run generation; the conservative reduced allowance remains unchanged through merging. Ordinary SortAggregate creates no extra external reader of its own. Its existing child has its own ownership; any future aggregate-owned reader would require another P reserve.

ExternalSort independently applies its existing descriptor, run-capacity, and required fan-in checks. Positive Msort alone does not guarantee feasibility. Hash sorting feasibility is checked only when spilling is needed, so a tiny valid one-group/no-spill query remains legal. At M=10488 with narrow input, capacity=1 succeeds for one key; a second key spills and explicitly fails for insufficient internal sort memory. Raw input is never replayed or switched to raw SortAggregate on error. The sorter's finite descriptor limit also bounds successful partial-record volume; no recursive aggregation or run-descriptor redesign is added.

Representative measured configuration for two INT64 input columns and all outputs:

| Value | Bytes or count |
|---|---:|
| M / Tmax / S / Ccombine | 32768 / 360 / 10296 / 2104 |
| Group capacity / peak live groups | 117 / 117 |
| Modeled peak hash bytes / internal Msort | 32760 / 26568 |
| Overflow flushes / all spill batches | 3 / 4 |
| Partial records / output groups | 355 / 350 |
| Partial sort initial runs / materialized passes | 10 / 3 |
| Cumulative temporary pages | 28 |

The final merge streams. With M=65536, the scale fixture has capacity=287 and writes 21 batches/5741 partials. Maximum-width input tests exercise Tinput=4672 dominating Tpartial=360: S=23232 and Ccombine=15040. Narrow encoded partial states still determine ExternalSort's own row charge.

## Lifetime, resources, and measurements

The executor lifecycle is unchanged: construction validates, Init starts one execution, Next returns one owned row or EOF only, and Close is noexcept/idempotent. Close then Init is a new whole execution, permitted for replayable children. Init twice without Close rejects without cancelling the active execution. Metrics reset at Init and survive Close. Early Close destroys map, writer, sort working allocations, and lookahead, and closes the child tree. SortAggregate retains its closed sorter object for replay; ExternalSort::Close releases that object's working capacities.

TempReader/Writer transfer page images through brief BufferPool guards. Aggregates own no PageGuard and retain none across child calls, output, or sort phases. All tests use F=8, with active pins=0 at checked boundaries and peak pins=1.

Spill and sort share the existing append-only scratch file. Dropping run descriptors releases their storage with **zero page-reclamation I/O**; it never walks chains, recycles PageIds, or reduces physical extent. Dropped runs, previous replay executions, and abandoned allocations count against max_temp_pages until checked query finish/cancellation. Tests exhaust the cap during partial writing and sorting, and demonstrate replay consuming an additional page before failing at the cumulative limit.

DbError propagates for overflow, resource exhaustion, and child/I/O errors; these never become EOF or successful partial results. Callers discard any emitted prefix, close the tree, and call checked QueryContext::Close on failure. Success requires observed EOF and successful FinishQuery cleanup. Structural I/O errors retain the existing abandon-instance policy; there is no recovery/retry subsystem.

Memory() exposes AggregateMemory's max_bytes, maximum_row_bytes, scratch_bytes, combine_bytes, hash_group_capacity, and sort_memory_bytes (zero when no internal sort allowance is needed). Metrics() exposes:

- `input_rows`: raw rows processed by hash/global aggregation, or sorted/raw rows fetched into the SortAggregate group loop, including one fetched boundary row. An early-closed unordered SortAggregate can have consumed fewer rows into this loop than its sorter already consumed from the original child.
- `peak_hash_groups`, `modeled_peak_hash_bytes`: bounded table occupancy and full reservation charge; zero outside grouped HashAggregate.
- `overflow_flushes`: completed overflow-triggered batches only.
- `spill_batches`: completed nonempty partial batches, **including final flush**.
- `spill_records`: successfully appended partial records, including a partial batch prefix if execution fails.
- `output_groups`: finalized rows emitted; global aggregation emits one.
- `used_internal_sort`, `input_order_reused`: actual physical path.
- `sort`: an ExternalSortMetrics snapshot after Init and at Close, including initial/materialized runs and completed merge passes.

TempStore reports cumulative pages separately. ExternalSort run counts, BufferPool fetches/misses, and DiskManager file reads/writes describe different work; they are not interchangeable "disk I/O" counts and do not measure physical device traffic.

The tested ExternalSort(HashAggregate(GraceHashJoin(...))) uses GHJ M=65536, aggregate M=32768, final sort M=32768, and F=8. GHJ emits 600 rows, the aggregate spills three batches and returns 300 groups, and the final sort creates six runs. Original children execute once; all operators retain independent local allowances, with one cumulative temp cap. No sum-of-allowances query admission is imposed.

## Complexity and tradeoffs

When groups fit, expected HashAggregate CPU is O(n) under ordinary unordered_map assumptions, with no temp I/O. Repeated keys can keep group-state pressure small even for large n. If it spills, work is one raw input pass, partial-state writes, ExternalSort over **partial records**, and streaming combination. Sort cost is over those partial records, not replayed raw rows. High cardinality or repeated reappearance after flush can create many partials, and output remains unordered.

Once ordered, SortAggregate performs O(n) adjacent grouping with tiny constant group state and naturally ordered output. It is particularly useful after a B+ tree IndexScan. Without order, its cost is ExternalSort over input plus O(n) grouping. The cost of establishing order is its main disadvantage. Physical algorithms are composed directly through the executor tree.

Intentional limits: one NONNULL INT64 group key, one NONNULL INT64 measure, no NULL grouping/count semantics, HAVING, DISTINCT aggregates, multiple measures/keys, windows, parallel aggregation, or recursive hash-partition aggregation. Checked integer sums can fail before cancellation; finite sort descriptors and append-only temp caps can reject large workloads. [Statistics, physical selection, and EXPLAIN](optimizer.md) reuse these algorithms and reduced internal budgets.

## Source and self-check

All aggregation code is in [aggregate.cpp](../src/execution/aggregate.cpp): `AggregateOutputSchema`, `AggregateState::Update/Combine`, `HashAggregate::ConsumeHash/FlushBatch/SortPartials`, and `SortAggregate::Next` through the local `NextGroup` helper. Header [aggregate.hpp](../include/valencedb/execution/aggregate.hpp) makes owned children, map, writer, partial relation, sorter and lookahead explicit. The partial Schema owner outlives every borrowed relation/reader.

The aggregation and integration tests cover triggering-row handoff, final flush, AVG sum/count recombination, selected-field overflow checks, ordered-input reuse, spilling across multiple batches, and cross-partition grouping.
