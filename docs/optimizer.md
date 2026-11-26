# Physical optimizer and EXPLAIN

ValenceDB has a small typed physical-plan selector, not a SQL optimizer. `ChoosePlan` considers existing algorithms for one or two base accesses, pushed INT64 filters, optional explicit access projections, at most one binary inner equijoin, optional global/single-key aggregation, one ascending NONNULL INT64 order requirement, and final projection. There is no SQL AST, multi-join search, histogram, memo, rule engine, or adaptive reoptimization.

## API and column positions

Include `valencedb/optimizer/optimizer.hpp`. The basic use is:

```cpp
db.Analyze("employees"); // Explicit, before starting the query.
QueryContext context(db, QueryResources{32768, 4096});
QuerySpec query;
query.left.table = "employees";
query.left.predicates = {{1, CompareOp::Greater, 20}};
query.aggregate = AggregateSpec{0, 1, true, true}; // group, measure, COUNT, SUM
query.order_column = 0;                         // aggregate's group output
query.projection = ProjectionPlan{{2, 0, 1}};    // sum, group, count
auto chosen = ChoosePlan(context, query);
std::cout << Explain(chosen);
chosen.root->Init();
Tuple row({});
while (chosen.root->Next(row)) { /* consume owned row */ }
context.FinishQuery(*chosen.root);
```

`BaseAccess` has `table`, `predicates` (AND), and optional `projection`. Predicates use **base-table** positions and execute before that projection. `EquiJoinSpec` keys use **access-output** positions. A join always produces logical Left followed by Right, regardless of physical outer. `AggregateSpec` uses this joined schema, or the single access schema. ORDER BY uses positions before the final projection; the final projection can retain/remap or remove that order. These positions are explicit rather than inferred from names. Invalid columns/types and inconsistent shapes throw `DbError`. Multiple joins, order/group keys, directions, and arbitrary expressions are not representable in the typed API.

Automatic projection pruning is omitted: extra payload survives unless the caller supplies a ProjectionPlan. `ProjectedSchema` and `ProjectedOrdering` are the shared execution helpers, so the first surviving occurrence of the ordered column is remapped; removing it loses the ordering proof. Filter preserves order; joins and HashAggregate are conservatively unordered. SortAggregate exposes ascending output column 0. Sorting nullable global SUM/MIN/MAX or DOUBLE AVG is rejected; there is no NULL comparator. COUNT(*) needs no measure. Global aggregation uses the existing single-state HashAggregate path, without group table or sort.

`OptimizedPlan` owns one `unique_ptr<Executor> root` and its inspectable `PhysicalPlan` descriptor. It carries no resource/options snapshot, rejection history or separate sort-elision metadata. Database outlives QueryContext, which outlives that root. Descriptors are inspection results, not a separate mutable executable-plan API. Candidates are small value trees; only the winner constructs executor objects, and planning never calls Init. SortAggregate retains its actual input as its child. `NeedsInternalSort()` derives the internal-sort requirement from the group key and that child's ordering; the executor constructs its sorter once with the reduced allowance. `Build()` recursively constructs each child and switches on the operator kind, without skipping nodes.

Each `PhysicalPlan` is one tagged value with schema/order, estimated rows, exactness,
conservative row bound, NDVs, local/subtree cost, real children, operator configuration,
and an optional **resolved** `memory_bytes`. There are no cached operator memory
layouts, explanation strings, cumulative temporary-page estimates, packed-page
counts, input-row totals, or replay counts. A small statistics-source enum on scans
preserves whether their estimates used fresh, stale, or missing samples. Capacity
layouts are local values computed by the existing shared helpers.

## Statistics and Analyze

`Database::Analyze(name)` performs one live heap scan and analyzes every base INT64 column. Each column retains exact signed min/max from that scan, a deterministic reservoir of at most **1024 values**, and `approximate_ndv`. VARCHAR slots remain empty. Analyze uses a fixed integer mixer of row position/column for replacement; no random device or table-sized distinct set exists. Sorting a copy of the bounded reservoir yields sample distinct count d and singleton count u. With n analyzed rows and sample size s:

```text
approximate NDV = clamp(d + u * (n-s)/s, 1, n)   nonempty INT64 column
empty: NDV=0, sample empty, min/max absent
```

Even a complete small sample is labeled approximate NDV. Exact min/max covers all live rows, including INT64_MIN/MAX. `Database::Statistics(name)` returns a nullable pointer to `TableStatistics { analyzed_rows, stale, columns }`. Absence means unanalyzed. Do not retain its column references across another Analyze. Insert, successful Delete, and CreateIndex mark the affected analyzed table stale. Re-Analyze replaces the snapshot and clears staleness. Analyze follows Database mutation exclusion during an active query.

Statistics are **in-memory only** and disappear on reopen. Reopened planning uses heuristics until explicit Analyze. Row count, heap page count, index existence/column and index height remain exact, maintained catalog/index metadata; planning never scans data to discover them. Deletes can leave heap pages allocated, so packed estimated output pages and exact physical heap pages are distinct quantities. Costing derives packed temporary pages from schema/rows; SeqScan local page cost preserves the exact heap-page charge.

Statistics may influence plan choice but never query correctness. Stale samples remain usable and are labeled `stale-sampled`; they never remove a predicate, substitute an empty executor, change index bounds, or choose a different comparison. Runtime predicates are retained even when estimated selectivity is zero. The stale-statistics test inserts a key outside the last analyzed range: the zero-row estimate still returns the row, agreeing with fresh stats and forced SeqScan.

## Selectivity and cardinality

`EstimateSelectivity` uses `1/NDV` for in-range equality and zero for a value outside sampled-snapshot exact min/max. Range estimates use an inclusive integer domain, `(max-min+1)`, with endpoint comparisons performed in INT64 and unsigned differences before floating conversion. This avoids signed overflow, division by zero, and cancellation for adjacent values near signed extremes. Clamp every fraction to [0,1]. Without useful min/max, equality uses 0.1 and inequalities use 1/3. Missing NDV uses sqrt(current rows), bounded to [1,rows]; empty is zero.

Filter conjunction estimates multiply the individual fractions. This assumes independence, including a deliberately crude treatment of overlapping predicates on one column. Index bounds use the minimum individual bound fraction; filter-prefix estimates are capped by access rows so the bounds are not applied twice. All filters still execute. Join rows use `left_rows*right_rows/max(1,left_NDV,right_NDV)`; group count uses the selected key NDV capped by estimated input rows. These assumptions can be wrong, especially for skew or correlated columns. Scan nodes identify their statistics source as fresh sampled, stale-sampled, or heuristic. Row exactness and conservative bounds remain in the descriptor for feasibility; EXPLAIN shows the estimated row count.

Costs/cardinalities use `long double`, including multiplication before any narrowing. No row-count product is computed in uint64_t. Exact raw counts also supply conservative row upper bounds (join Cartesian product, filters do not reduce the bound). Sampled selectivity never serves as a proof of memory feasibility.

## Candidates and deterministic selection

The implementation explicitly builds scan alternatives, then legal join algorithms/orientations, then aggregates, then required final sort and projection. Full candidate subtree scores decide the winner, including downstream ordering benefits. No index bonus is used.

Enumeration order breaks relative score ties within `1e-9 * max(1,best_score)`: access combinations use SeqScan before IndexScan; within each combination NLJ, BNLJ, GHJ, with Left before Right; aggregate alternatives use HashAggregate before SortAggregate. The first complete feasible candidate wins a tie. Tests repeat an exact NLJ/BNLJ and Left/Right tie ten times and compare EXPLAIN.

NLJ and BNLJ inner children must be replayable. QuerySpec admits only scans, filters, and projections below its sole join; all obey the Close→Init replay contract. Custom executor/subquery inputs are not accepted, so no capability framework is needed. GHJ consumes each original input once and selects per-partition build/fallback side at runtime, not through hypothetical build-side candidates.

## Cost units and child multiplicity

`ModeledCost { pages, cpu }` scores `pages + 0.0001*cpu`. Pages mean **estimated algorithmic logical page transfers**, not disk misses, device operations, milliseconds, or production calibration. CPU units are simple educational operation counts: Filter one per input; nested joins one per candidate pair; GHJ three per input; hash grouping two per input; streaming/global grouping one per input; sorting `n*log2(max(2,n))`; partial combination one per estimated partial. Projection is free in this model.

Each node exposes local and subtree costs. Ordinary one-pass subtree cost is child subtree plus local. Binary GHJ counts both children once. NLJ/BNLJ show children at their per-execution cost, and multiply the selected physical inner's **whole path cost**, including IndexScan and Filter CPU:

```text
NLJ  = outer_path + outer_rows * inner_path + comparisons
BNLJ = outer_path + ceil(outer_rows/C_block) * inner_path + comparisons
```

Join local cost excludes replayed child work: local page work is zero for nested loops; multiplicity appears in subtree cost. Replays are computed locally during costing, with no extra metadata or EXPLAIN worksheet. An empty outer executes the inner zero times. Do not sum displayed children once to reconstruct a nested-loop subtree.

For temporary data, `N(schema,n)=ceil(n/floor(4064/(max_encoded_bytes+4)))`. This packed worst-width model ignores per-run/partition fragmentation and singleton carry savings. The planner does not retain a cumulative temporary-extent estimate. Runtime TempStore enforces the append-only allocation cap; releasing runs does not reclaim extent. Allocation initialization and cache effects are omitted.

## Access paths

SeqScan local pages equal **exact table heap pages**. IndexScan uses persisted height h, physical leaf capacity L=254, and estimated matching tuples m:

```text
index local pages = (h-1) + max(1,ceil(m/L)) + m
```

The final term represents unclustered heap tuple fetches and can greatly exceed table pages. Empty lookup still descends to a leaf. Full traversal pays for all heap tuples but proves ascending order. Automatic IndexScan requires an indexed predicate, join key, group key, or single-table requested order key. Explicit forced IndexScan can request full traversal of the table's configured index. Predicate bounds are inclusive supersets; strict predicates retain their exact Filter, avoiding +/-1 endpoint overflow.

The tests show a 2,000-row/8-page table choosing a one-tuple IndexScan at 3 page units and choosing SeqScan for a full range. A 150-row wide table chooses full IndexScan at 151 units rather than SeqScan+sort at 330.108, because the index eliminates sorting.

## Shared local memory calculations

Planner and executors call the **same** small Compute helpers in their existing operator modules. Each heavyweight plan stores only the resolved allowance used in costing. `Build()` passes that exact byte allowance to the executor, including overrides; layouts are recalculated locally. EXPLAIN does not recompute capacities or validate resources. QueryContext validates query limits once; the shared helpers check the selected local memory allowance. No memory-grant or query-admission system is introduced. Checked reserve arithmetic, vector/container representability, and minimum capacities remain in those helpers. Here P=4096 and T is RowMemoryCharge.

| Operator | Derivation |
|---|---|
| BNLJ | `S=2P+3*max(Tleft,Tright,Toutput)+1024`; require M>S; `C_block=(M-S)/Touter >=1` |
| GHJ | same S; `p=min(64,(M-S)/(P+256)) >=2`; `Dghj=128p`; `build(side)=(M-S-Dghj)/(Tside+96)`; fallback M=`M-Dghj`, at least one legal fallback block |
| ExternalSort | `S=2P+3*Tinput+1024`; `D=min(4096,M/1024)`; `R=128D`; `C_sort=(M-S-R)/Tinput >=1`; `k=min(64,(M-S-R)/(P+Tinput+128))` |
| HashAggregate | `Tmax=max(Tinput,Toutput,Tpartial)`; `S=2P+3*Tmax+1024`; grouped capacity=`(M-S)/192 >=1` |
| Aggregate combine | `Ccombine=3*Tmax+1024`; global/ordered requires M>=Ccombine; hash internal Msort=`M-Ccombine-(P+128)`; unordered SortAggregate Msort=`M-Ccombine` |

GHJ local pages add two packed input volumes: partition writes and partition reads. Original inputs are paid by child costs exactly once. Skew is unknown; the GHJ executor retains runtime BNLJ overflow fallback. For 1,000 rows on both narrow sides at M=32768, GHJ wins at score 24.6, with p=5, Dghj=640, and build capacities 103/103. A separate forced GHJ test actually triggers one fallback pair without relabeling the plan.

For BNLJ at M=18200, S=14664, a 40-row narrow side has C=29 and two inner executions, while the five-row wide side has C=2 and three executions. Both mirrored query forms select the narrow outer, retaining logical output positions.

## Sort and aggregate work

`EstimateSort(schema, rows, row_upper_bound, exact_rows, resources, bytes)` is a pure helper returning local cost, initial runs, and merge levels. Explicit ExternalSort, SortAggregate internal sorting, and HashAggregate partial sorting all call it; estimation constructs no plan or executor.

Let r=ceil(estimated rows/C_sort). If r<=1, modeled temp work is zero. Otherwise let L be the number of repeated `r=ceil(r/k)` reductions until r<=k. ExternalSort local page work is `(2L+2)*N`: initial writes, L intermediate read/write levels, and final streamed reads. Child input is **not** charged again. There is no materialized final output. A 150-row wide forced SeqScan plan at M=32768 has C_sort=8, k=2, 19 initial runs, four materialized levels, local sort pages=300 and subtree pages=330.

If exact upper bounds permit more than one run, k must be at least two even when samples predict a private run. An exact known input exceeding D initial descriptors is rejected during planning. For uncertain filtered/join/spill volume, runtime retains the finite descriptor cap. Neither row estimates nor logical page costs guarantee successful execution within runtime caps.

Grouped HashAggregate compares estimated groups with capacity. Fit predicts no temp I/O. Spill conservatively models one partial per estimated raw input row: partial writes plus the TempScan read, ExternalSort **over partial states** using reduced Msort, and streaming combination. Local work adds `2*Npartial + sort_local`, not a second scan of raw input. Spill prediction is uncertain; HashAggregate handles actual spills irrespective of the estimate.

Whenever the exact input upper bound could exceed group capacity, the planner also checks the reduced partial-sort minimum/fan-in even if NDV predicts fit. Consequently it can reject a tiny-M grouped plan that would happen to fit because every actual key is equal. This intentional conservative local check avoids making sampled NDV a correctness requirement. Finite descriptor/temporary caps and allocation failures remain runtime limits.

SortAggregate reuses proven ascending group-key order with streaming CPU only. Otherwise the shared sort estimator uses M-Ccombine. Aggregation local cost includes group CPU plus that sort work; subtree cost adds the actual input subtree once. The child is never replaced by a synthetic ExternalSort. Partial-state costing likewise needs no synthetic SeqScan. Ordered output is column 0, so a matching final order costs nothing. Global aggregation skips this comparison entirely. Tests choose HashAggregate for three groups (capacity 117), SortAggregate for a high-NDV input, and ordered SortAggregate after selective IndexScan. Forced HashAggregate at M=65536 actually spills and matches forced SortAggregate results.

## Manual controls and resources

`PlanOptions` contains:

- `left_scan`, `right_scan`: `ScanChoice::{Auto,SeqScan,IndexScan}`.
- `join`: `JoinChoice::{Auto,NLJ,BNLJ,GHJ}`; `outer`: optional existing `PhysicalOuter::{Left,Right}`.
- `aggregate`: `AggregateChoice::{Auto,HashAggregate,SortAggregate}` for grouped aggregation.
- `scan_ring`: `ScanRingChoice::{Auto,On,Off}`, applied to SeqScan. Auto enables the existing ring when exact heap pages exceed F/4; On/Off are passed to SeqScanPlan. It has no effect on IndexScan.
- `sort_memory`, `join_memory`, `aggregate_memory`: positive optional numeric local overrides. Sort override controls final ORDER BY; aggregate internal sorts use the aggregate's reduced budget. Join override governs BNLJ/GHJ; NLJ has no work-memory reservation.

Forced choices are honored or rejected, never silently substituted. Candidate memory/fan-in failures skip that candidate; if no complete plan remains, ChoosePlan throws `no feasible physical plan`. Invalid public options/shapes have direct validation errors. No rejection strings are retained, and native allocation errors are not swallowed as candidate failures. Missing index, insufficient BNLJ/GHJ/hash/sort memory, outer without a join, GHJ plus outer, and forced grouped algorithms for global aggregation reject. With Auto join plus forced outer, only NLJ/BNLJ are eligible; GHJ has no physical outer setting. Supporting sorts remain permitted in forced plans. GHJ's own overflow fallback and HashAggregate's own spill remain their respective algorithms.

F is configured by Database's existing BufferPool constructor; M and max_temp_pages use the existing QueryContext QueryResources. Several live heavyweight operators each have their own M. They are algorithmic accounting allowances, **not a whole-query RSS cap**. The optimizer does not estimate cumulative temporary extent. Runtime cap errors propagate normally and invalidate the result prefix.

## EXPLAIN worked plan

`Explain(chosen)` returns a cost legend followed by one line per physical node.
Each line shows the operator/configuration, estimated rows, subtree score and
page/CPU components, local score, ordering, and resolved memory allowance when
applicable. Scan nodes include the statistics source. SortAggregate reports
`internal_sort=yes/no`, retaining its real input child; it does not invent a
synthetic ExternalSort node. A missing final sort and the child's ordering show
order reuse, even if the final projection subsequently drops that key.

The formatter only reads the selected descriptor. It never consults the current
database, computes capacities, initializes executors, or changes runtime metrics.
Resource worksheets, full schema dumps, forced-option echoes, uncertainty prose
and rejected alternatives are omitted. Consult `Memory()` on the actual operator
or the formulas above when studying capacities. M is per operator, not whole-query
RSS; sampled row/cost estimates cannot guarantee that runtime caps will suffice.

The `pipeline_explain` fixture runs filter → join → aggregate → sort → projection
against an independent reference. Its exact EXPLAIN output is recorded in the
[V1 verification report](v1_algorithm_first.md). The result has four groups;
the estimated five groups illustrate approximate join/group NDV. The final
projection remaps ordered group column 0 to output column 1. Equivalent automatic
and forced index variants match the same reference bag.

## Limits

The optimizer excludes SQL, more than one binary join, multiple group/order/join keys, histogram/correlation/skew models, NULL key ordering, and adaptive reoptimization. The benchmark harness exercises the available physical alternatives. Projection pruning is manual. Analyzed stats require re-Analyze after reopen. Estimates can choose a slower legal plan; finite descriptors and append-only temp extent still limit runtime. Aggregation equivalence applies to successful nonoverflowing inputs: checked partial accumulation can overflow before cancellation in an order-dependent way. These are ValenceDB-specific educational costs, not portable costs for PostgreSQL or other systems. Measurement-layer definitions are documented in [io_cost_models.md](io_cost_models.md).

## Source, search size, and self-check

Read [statistics.cpp](../src/optimizer/statistics.cpp) and [optimizer.cpp](../src/optimizer/optimizer.cpp). `Scans`, `Joins`, `Aggregates`, `EstimateSort`, `Sort`, and `Build` are local concrete helpers; `ChoosePlan` and `Explain` are the public free functions. Planning owns small value candidate trees and builds executors only for the winner. QueryContext must outlive the returned root.

At most two access alternatives per side, five unforced join alternatives (NLJ/BNLJ each orientation, one GHJ), and two grouped aggregate alternatives produce a small bounded enumeration for the fixed one-join shape. Predicate/projection processing is linear in the supplied lists per candidate; there is no factorial multi-join search. Analyze scans rows once and retains at most 1024 values per INT64 column plus bounded sorted-copy scratch. Planning itself performs no statistics scan and no temp execution.

For a surprising choice, compare local versus subtree terms, selected inner replay cost, width-derived capacities, and required final ordering before questioning arithmetic. Sampled cardinalities are advisory; independent exact upper bounds govern conservative feasibility. Forced algorithms reject impossible configurations. The optimizer tests cover replay costs, memory feasibility, deterministic tie-breaking, forced alternatives, aggregation choices, and EXPLAIN output.
