# ValenceDB V1: algorithm-first engine

## Phase 5 baseline and intended contract changes

The clean baseline is `839718704292c791a7a39fac41fbe0573586f327`, preserved
locally as `archive/v1-phase5-baseline-8397187`. The original pre-V1 engine is
also preserved at `archive/pre-v1-bplus-8fe8c9b`. No commit or push is requested.
Before implementation, the complete Debug build and all 13 CTest suites passed
(70.86 seconds). Logs are in ignored `benchmark_results/v1_phase5/`.

The Phase 5 contract changes recorded before implementation are:

- EXPLAIN becomes a compact physical tree: operator configuration, cardinality,
  local/subtree cost, page/CPU estimates, ordering and resolved memory allowance.
  Capacity worksheets, schema dumps, forced-option echoes, rejection histories
  and separate sort-elision annotations are removed.
- OptimizedPlan retains the selected descriptor and executor tree. Explain-only
  resource/options/history fields are removed. PhysicalPlan retains exactness,
  upper bounds and NDVs because feasibility and costing require them.
- Infeasible candidates are still skipped and alternatives considered. If none
  remain, planning throws a concise `no feasible physical plan` error rather than
  collecting every rejection. Invalid public query shapes remain errors.
- The DiskManager FailNext/FailNextIo testing API and its runtime state/branches
  are removed. Tests of one-shot injected failure/retry guarantees are revised;
  real file error detection, error propagation and cleanup remain required.
- Reserved byte/padding contents will no longer be audited by production readers.
  Writers keep the same zero-filled format; magic, version, identity, type, counts
  and access bounds remain checked. Catalog checks already implied by its trusted
  in-memory table list and validated schema, and the unused TableCount API, go.
- QueryResources are validated on QueryContext construction; capacity calculators
  resolve/check their memory allowance without revalidating unrelated temp limits.

No physical operator, storage format, supported query shape, memory/spill limit,
or clean close/reopen guarantee is removed. Unexpected I/O/allocation failures
may require abandoning the affected instance. V1 promises neither rollback nor
crash recovery, and a failed query invalidates its entire output prefix.

## V1 scope and architecture

V1 is an educational C++20, single-process, single-threaded relational engine
using real 4096-byte disk pages. The API is programmatic; there is no SQL parser.
One Database permits one active QueryContext. Storage-only operations can use a
one-frame pool; query execution requires at least eight shared buffer frames.
Base schemas contain 1–8 NONNULL INT64/VARCHAR columns, VARCHAR at most 256 bytes,
and encoded rows at most 1024 bytes. Intermediate schemas permit 1–16 columns,
DOUBLE and typed NULLs, with at most 2048 encoded bytes. The catalog holds at most
16 tables, each with at most one insertion-only INT64 B+ tree index.

Heap insertion, point fetch and scanning are supported. Heap deletion uses stable
slot tombstones; deletion from an indexed table is rejected before mutation.
Index creation scans existing rows and later inserts maintain the index. Clean
explicit Close/reopen preserves tables, RIDs, metadata and index contents.
Statistics are bounded samples in memory and require Analyze again after reopen.
V1 does not validate or repair arbitrary corrupt page graphs; unsupported file
corruption can require rebuilding even when local identity/bounds checks pass.

The optimizer handles one or two accesses, AND predicates, explicit projections,
at most one binary inner equijoin, optional global/single-key aggregation,
one ascending NONNULL INT64 order key, and final projection. Joins preserve
logical Left+Right output positions regardless of physical orientation.
COUNT, SUM, MIN, MAX and AVG are supported, with checked INT64 accumulation and
raw sum/count partials for AVG. Empty global aggregation returns one row; empty
grouped input returns none. SUM/MIN/MAX/AVG of an empty global input are NULL.

```text
QuerySpec + PlanOptions → ChoosePlan → PhysicalPlan + owning Executor tree
                                      ↓
Database → Catalog / HeapFile / BPlusTree      QueryContext → TempStore
                       ↓                         ↓
                       BufferPool + PageGuard + ScanRing
                                      ↓
                       DiskManager (persistent and scratch files)
```

Page codecs are separate from tuple/algorithm logic. B+ tree insertion decodes
one bounded node at a time; its ancestor path holds only page IDs and child
positions. Executors exchange owned Tuples, never references into evictable
pages. Temporary writers/readers own one private staging page each and touch the
same BufferPool as persistent data. TempRun is a sealed non-owning descriptor.
The context, schema and database must outlive their borrowers. Explicit query
completion closes the tree, discards temporary cache mappings and truncates the
scratch file; destructors release resources but do not certify success.

M is a conservative **per-operator algorithmic allowance**, independent of shared
buffer frames F. It is not allocator accounting, process RSS, or admission control
for a pipeline of simultaneously live operators. Every heavy operator has finite
row/group/head/descriptor capacities. Scratch is append-only until whole-query
cleanup and bounded by max_temp_pages; finishing a run does not reclaim pages.
Finite sort descriptors can reject a large input even when merging is otherwise
possible. These limits are intentional V1 boundaries, not incomplete results.

## Algorithms and cost models retained

- Slotted pages: append, stable slots, tombstone deletion and bounded page compaction.
- Buffer management: page table, dirty writeback, pinning, CLOCK with usage counts
  capped at five, and a small scan ring to reduce scan pollution.
- B+ tree: composite (key,RID) order, binary descent/lower bound, duplicate logical
  keys, leaf splits copying a separator upward, internal splits moving a separator
  upward, cascading splits/root growth, and streaming linked-leaf ranges.
- Volcano execution: Init/Next/Close, sequential/indexed scans, filters,
  projections and replayable temporary scans.
- External merge sort: bounded sorted runs, finite run descriptors, multi-pass
  priority-queue merge and streamed final output. Exactly one fitting run stays
  private; multiple runs require fan-in at least two.
- NLJ and BNLJ: full inner replay per outer row/block, both physical orientations,
  duplicate multiplicity and resumable output within each comparison loop.
- GHJ: one disk partitioning level, separate partition/table hashes, per-pair build
  selection by bytes, bounded hash multimap, and BNLJ for oversized/skewed pairs.
- Aggregation: one-state global accumulation; bounded hash grouping, partial-state
  spilling and external sort/recombination; streaming sorted grouping with one
  boundary row and reuse of proven input order.
- Optimization: deterministic sampled statistics, selectivity/cardinality/NDV
  estimates, full physical candidate enumeration for the supported shape,
  scan/join/side/aggregate choices, ordering propagation and unnecessary-sort
  avoidance, then construction of only the chosen executor tree.

No execution capacity formula changed in Phase 5. The same Compute*Memory helpers
serve execution constructors and planning. ResolveBudget no longer validates
unrelated query-wide temporary limits repeatedly; QueryContext still validates
those limits on construction. Direct capacity-helper callers must validate their
own QueryResources when they need query-wide checks.

Costs remain educational predictions: score = pages + 0.0001 × CPU operations.
SeqScan charges allocated heap pages. Unclustered IndexScan charges descent,
matching leaves and estimated heap tuple fetches. NLJ repeats the entire inner
path per outer row; BNLJ repeats it per ceil(rows/block_capacity). GHJ charges
both input paths once plus partition write/read traffic, assuming hashable pairs;
skew fallback remains a runtime algorithm, not a promised cost bound. Sort charges
initial writes, intermediate read/write passes and the streamed final reads.
Hash-spill cost uses partial-state widths, partial write/read, reduced sort memory
and combination work; SortAggregate reuses ordering or pays for its own reduced
sort allowance. Cache misses, allocation initialization, tail fragmentation and
skew are not fully predicted. No timing is inferred from these cost units.

Exact raw row counts and conservative bounds remain distinct from estimates.
Sampled zero selectivity never removes a real predicate or creates synthetic EOF.
Feasibility checks retain positive capacities, representable containers, GHJ
partition/fallback space, sort fan-in, exact-input descriptor limits and possible
hash-spill sorting. A candidate may be rejected even if a favorable sample would
happen to fit. Uncertain descriptor counts and cumulative scratch caps remain
runtime limits. The new 3000-byte tests demonstrate selection of IndexScan and
ordered SortAggregate after infeasible alternatives are skipped.

## Removed mechanisms and relaxed guarantees

The following inventory covers every Phase 5 production removal. Earlier V1
removals are separately identified below; this phase introduces no large
architectural change.

| Removal | Reason and resulting contract |
| --- | --- |
| Rejected helper, deduplication/search and candidate rejection string vectors | Diagnostic bookkeeping did not select plans. Three candidate catches still skip infeasible alternatives; no-plan failure is concise. |
| OptimizedPlan.resources, buffer_frames, rejected, options, elided_order_column | These were EXPLAIN-only snapshots. The plan and context/executor already expose the selected operator, order and actual resources. Forced options remain functional. |
| SortEstimate struct/returned initial_runs and merge_levels | Only EXPLAIN consumed these extra results. EstimateSort returns ModeledCost; local run/level calculations remain necessary for costs. |
| ExplainSort and memory recalculation in ExplainNode | A formatter should not repeat feasibility/capacity work. Operator Memory() and documented formulas retain educational capacity detail. |
| EXPLAIN schema/input/upper-bound/exactness/packing dumps, replay worksheets, capacity equations, predicted-spill/skew paragraphs, forcing echoes, rejection history and separate sort-elision message | Compact tree exposes choices, estimated rows, cost and order. Feasibility bounds and NDV remain on PhysicalPlan, and runtime algorithm metrics remain on executors. |
| Broad final-pipeline catch covering ORDER BY validation and projection | Invalid key/projection errors propagate directly. Only sort feasibility is caught at this stage; an ordered alternative can still win. |
| ResolveBudget's repeated QueryResources::Validate | QueryContext owns query validation; calculators still validate the selected positive allowance and checked arithmetic. |
| FailNext enum, FailNextIo API, fail_next_ state, Read/Write/ResetTemporary injection branches | Production disk code no longer supports artificial one-shot failures. Actual stream/filesystem errors still propagate. No injected retry/unchanged-state guarantee remains. |
| ReadRID/common-header/B+ fixed-header reserved-zero checks; catalog name-tail and column-reserved checks | Unused bytes have no algorithmic semantics. Encoders still zero them, with unchanged file format. Readers retain all meaningful identity/type/version/bounds checks. Corrupt padding is no longer diagnosed by production. |
| Catalog CreateTable metadata-page size recheck and cached/disk count comparison | Validated base schemas have at most eight 32-byte descriptors at offset 64; the single-threaded catalog owns its table list. No exhaustive audit of trusted internal state is needed. |
| Unused Catalog::TableCount accessor | No production, test or benchmark caller existed; TableNames remains available. |

All actual database operations supported at the Phase 5 baseline remain. Source
compatibility changes are limited to the listed diagnostic/fault APIs and fields.
The formatting and rejection-message contracts intentionally change. In addition,
unsupported corruption of ignored bytes is no longer rejected. No supported
operation is allowed to silently truncate output or convert an error to EOF.

Across the preceding phases, V1 also removed index deletion/redistribution/merge/
root contraction and retired pages; speculative split reservation and atomicity
expectations; exhaustive runtime B+ ordering/tree and slotted-page overlap audits;
repeated filesystem/allocation exception translation; redundant tuple encoding
and heap delete fetches; per-operator lifecycle/error wrappers; duplicated context
resource counters; temporary-reader row/page/tail reconciliation; redundant
extent/byte/failure counters; and Grace's failed-build retry machinery. Those
changes and the reasons for each individual removal are preserved in the
[Phase 1](v1_phase1_refactor.md), [Phase 2](v1_phase2_refactor.md),
[Phase 3](v1_phase3_refactor.md) and [Phase 4](v1_phase4_refactor.md) reports.
Their historical descriptions of fault hooks/EXPLAIN are superseded by this audit.

## Complete production audit and necessary complexity

All **58** .cpp/.hpp/.h files under src and include were read, including the private
join_helpers.hpp. The table covers every file pair by basename unless stated
otherwise. Category numbers follow the requested audit: **1** correctness,
**2** disk execution, **3** bounded memory, **4** ownership/cleanup,
**5** meaningful optimization, **6** unrelated defensive engineering (removed
above). CMake's valencedb library has no production sources outside these
directories; test and benchmark dependencies were reviewed separately.

| Files/component | Retained abstractions, state and branches; classification |
| --- | --- |
| common/error.hpp; common/version.cpp + version.h | DbError supplies one error type; ProjectName is the tiny library identity used by smoke tests. No error hierarchy/translation framework. **1** |
| types/schema, types/tuple | Column/Schema encode limits/types; Value's variant owns strings and its type tag distinguishes typed NULLs. Tuple owns its vector. Nullmask, count/width checks, bounded codec offsets and checked access prevent invalid values/access. **1,2,4** |
| storage/page_codec | PageRef distinguishes files; RID identifies stable slots; explicit little-endian scalar/page/RID codecs avoid struct-layout dependence. Magic/version/type/ID and span bounds retain format identity and safe access. **1,2** |
| storage/disk_manager | Two fstreams/path/page-count records own disk files; closed_ prevents use after Close. Allocated-ID/stream-offset/file-length checks and seek/read/write/flush/close errors remain. Success counters distinguish transfer and zero-page allocation work. No fault state remains. **1,2,4,5** |
| buffer/buffer_pool | Frame data/page/valid/dirty/pin/usage/generation; page table and clock hand implement cache identity, writeback and bounded CLOCK. Incoming-page staging prevents reading partial bytes into a resident page. Guards prevent eviction while borrowed; allocation before map replacement keeps ownership coherent without rollback machinery. Metrics distinguish cache/transfer behavior; InspectFrames supports tests/teaching. **1–5** |
| buffer/page_guard, buffer/scan_ring | Move-only pool pointer/frame index owns exactly one pin. Ring pool identity, occupied slots, generation and cursor avoid reusing another pool's or replaced frame's identity. Debug assertions document trusted invariants; empty guards/pinned invalidation still reject. **1,4,5** |
| storage/slotted_page | Header/slot/payload bounds precede access/copy. Tombstones preserve RIDs; one page scratch permits compaction without aliasing. Space checks handle directory exhaustion, empty/deleted slots and live-count underflow. Full overlap audits stay in tests. **1,2,3** |
| storage/catalog | Small table vector and fixed descriptors own names/schema/count/endpoints/index metadata. Open checks counts, names, duplicates, index key types and endpoint consistency; creates enforce the documented 16-table/base-schema limits. **1,2** |
| storage/heap_file | HeapFile borrows table metadata; HeapScan retains page/slot/completed-page count and captured mutation epoch. Owner checks stop cross-table reads; tail/page-count checks bound traversal and detect incomplete chains. Insert counters are checked for overflow. **1,2,4** |
| storage/database | Member order fixes disk→buffer→catalog/index lifetime. Stable unique index handles support borrowed cursors. closed_, query_active_ and epoch_ protect legal lifetime/mutation boundaries; statistics map/staleness serves costing. Index-build count/duplicate checks prevent publishing an incomplete index. **1,2,4,5** |
| index/bplus_page | Composite key, capacities and metadata structs directly describe disk layout. Bounds/count/key/child/height checks prevent invalid access; only count/identity checks run for ordinary node validation. **1,2,3** |
| index/bplus_tree | Leaf/Internal vectors, ancestor page/child positions and SplitResult express insertion propagation; metadata root/height and capacity govern disk access. Duplicate-pair no-op, empty-range, height and key checks are algorithmic. epoch_ protects cursors from structural mutation. **1,2,3,5** |
| index/bplus_iterator | Borrowed tree, leaf/offset, upper bound and epoch resume range traversal with no retained pin; empty leaf/end/high-bound branches implement normal EOF. **1,2,4** |
| execution/plan | Small operator configuration structs, predicate enum, Ordering and projection helpers describe executable operations. Public key/column/type checks and ordering remapping are semantic. **1,5** |
| execution/work_budget | QueryResources, checked size arithmetic and concrete Compute helpers express row, scratch, descriptor, writer, hash and merge capacities. Minimum capacity/container representability checks prevent unusable operators and overflows. **1,3** |
| execution/executor | Virtual interface is the single Volcano boundary; initialized_/eof_ enable replay and distinguish errors from EOF. QueryContext owns resources/temp store and succeeded_ records checked completion. Both Init/Next catches close and rethrow; destructor catch cannot report success. **1,4** |
| execution/basic_executors | Child ownership and optional scans/cursors/rings implement Init/Close; schemas and predicates/projections determine results. IndexScan checks that each RID maps to its live key. Null/cross-context children would violate ownership and remain rejected. **1,2,4,5** |
| execution/temp_relation | TempStore's active_/open_resources_ guard prevents truncation with live executors/readers/writers. TempRun stores exact counts/endpoints and borrowed schema/store. Writer store pointer closes on success/failure; reader page/slot/loaded/open state owns staging traversal. Increasing page links, caps, checked row count and page bounds retain safe bounded disk access. **1–4** |
| execution/temp_scan | A copied sealed descriptor and owned reader implement replay; cross-query run rejection prevents using the wrong reset domain. **1,2,4** |
| execution/join_helpers.hpp | Two concrete helpers check join schema/ownership and concatenate logical Left+Right tuples. No general join strategy framework. **1,4** |
| execution/nested_loop_join | Physical children/keys, retained outer tuple/have_outer_ and inner cursor represent the nested loops across Next. Scans/comparisons/output counters demonstrate complexity. **1,4,5** |
| execution/block_nested_loop_join | Bounded block, position_, retained inner/have_inner_, physical children/keys preserve the block loop across yields. Capacity drives filling; Close frees vector capacity. **1,3,4,5** |
| execution/external_sort | Run/row vectors, priority queue, owned input readers/heads and position_ implement run generation/merge. Descriptor/fan-in limits and phase releases enforce M; one initial lookahead distinguishes exact fit from required spilling. **1–5** |
| execution/grace_hash_join | Two bounded descriptor arrays, pair_/phase_/build_side_, one hash table, probe tuple/range iterators or one fallback resume partition-pair work. Exact build count bounds insertion; skew chooses BNLJ before building. Two hashes avoid routing/bucket correlation. **1–5** |
| execution/aggregate | Raw AggregateState and selected-field checks preserve overflow/NULL/weighted-average semantics. Hash table/sort ownership, iterator/phase_, global-emitted flag and one lookahead implement fit/spill/group boundaries. Groups/batches/spill/sort counters demonstrate work. AggregateTestAccess has no runtime branches/state and supports raw partial/ownership tests. **1–5** |
| optimizer/statistics | Per-column min/max, bounded reservoir and NDV; row count/stale flag and integer mixer support repeatable estimates. Endpoint/unsigned-distance arithmetic handles signed extremes. Samples stay advisory. **1,3,5** |
| optimizer/optimizer | Concrete tagged plan trees, schemas/order/NDVs/row bounds/cost/config/memory support bounded enumeration, feasibility and Build. Three narrow-purpose DbError catches skip infeasible candidates; public option validation and Build/I/O/allocation failures propagate. Explain-only histories/snapshots/worksheets were category **6**. Retained state is **1,3,5**. |

The remaining exception-handler inventory is small and intentional: three in
optimizer candidate loops (**1,3,5**); two in Executor Init/Next and two in
TempWriter Append/Finish that close and rethrow (**4**, also preventing failed
prefix publication); one nonthrowing QueryContext destructor catch (**4**).
No handler translates allocation/filesystem failures or records verbose rejection
text. Direct throws fall into public semantic validation, page identity/type/
bounds, checked arithmetic, actual I/O, ownership/lifecycle guards, or execution
capacity limits. Debug-only assertions cover local invariants, not exhaustive
runtime structural audits.

Phase 5 changes outside the optimizer are limited to this audit's identified
category-6 checks/hooks/accessor, their dependent tests, and current documentation.
Buffer replacement, storage algorithms, index insertion, execution algorithms,
capacity equations and benchmark workload definitions were not redesigned.

## Function-level learning order

Read each public header beside its implementation. The sequence below follows
increasing dependence; finish each component by reading its oracle/boundary tests.

| Component | Function order and what to follow |
| --- | --- |
| Values and schemas | Schema constructor → Value accessors → Tuple::value → EncodeTuple → DecodeTuple. Follow width/type/NULL handling before page storage. |
| Page representation | ReadU16/U32/U64 → WriteU16/U32/U64 → signed/double codecs → ReadRID/WriteRID → WritePageHeader/ValidatePageHeader. |
| Disk files | DiskManager::OpenFile → Allocate → Read/Write → Flush/Close → ResetTemporary. Observe page counts and real stream checks. |
| Buffer pool | Fetch → FindInvalid/ChooseFrame/SweepVictim → Install → Pin/Unpin → Flush/Invalidate/DiscardTemporaryPages. Then PageGuard move/Drop and ScanRing::Record. |
| Slotted pages | Initialize → CanFit → Insert → Fetch/Erase → Compact; then CheckBounds/CheckPayload. Trace offsets in the independent slotted-page validator. |
| Catalog and database | Catalog::DecodeMeta/EncodeMeta → constructor/CreateTable/Save → Database constructor/CreateTable/Insert/Delete/Close. Track TableInfo persistence and ownership. |
| Heap access | HeapFile::Insert → Fetch/Erase → HeapScan::Next/Reset. Trace tail growth and stable tombstones. |
| B+ pages | BPlusKey::operator< → leaf Count/Entry/Write → internal Child/Separator/Write → metadata Read/Write and capacities. |
| B+ search/insertion | Descend → LeafLowerBound → Contains/Start → ReadLeaf/ReadInternal → Insert → SplitLeaf → SplitInternal → SetRoot. Contrast copy-up with move-up. |
| Index integration | BPlusIterator::Next → Database::CreateIndex/TableIndex/Insert → IndexScan::DoNext. Verify logical duplicates and RID multiplicity through reopen. |
| Query protocol | QueryContext constructor → Executor::Init/Next/Close → FinishQuery/Close. Track ownership and EOF separately from success. |
| Basic operators | Int64Predicate::Matches → SeqScan and IndexScan DoInit/DoNext → Filter::DoNext → Projection::DoNext/OutputOrdering. |
| Temporary storage | TempStore::Allocate → TempWriter::Append/Publish/Finish → TempReader::Next/Rewind → TempScan → TempStore::Reset. |
| Memory accounting | RowMemoryCharge/FixedScratchCharge → ComputeSortMemory/ComputeBlockJoinMemory → ComputeGraceJoinMemory → aggregate Compute helpers. Draw live allocations per phase. |
| NLJ/BNLJ | JoinSchema/JoinedTuple → constructors (physical swap) → NestedLoopJoin::DoNext → BlockNestedLoopJoin::FillBlock/DoNext → DoClose. |
| External sort | DoInit → GenerateRuns/SpillRun → MergePasses → OpenMerge/PullMerge → DoNext → CloseMerge/DoClose. Trace one private run and then a multi-pass input. |
| Grace join | Partition → SelectPair → Build/Probe → BeginFallback → ReleasePair → DoNext/DoClose. Follow a uniform pair and an all-equal oversized pair. |
| Aggregation | AggregateOutputSchema → AggregateState::Update/Combine → Finalize/ConsumeGlobal → HashAggregate::BuildGroups/FlushBatch → DoInit/DoNext → NextGroup → SortAggregate constructor/DoNext. |
| Statistics | Database::Analyze → SampleHash/reservoir update → EstimateNDV → EstimateSelectivity. Follow repeated keys, stale min/max and signed extrema. |
| Optimizer | ChoosePlan/ValidateOptions → Scans → Unary/Project → Joins → Aggregates → EstimateSort/Sort → score comparison → Build → ExplainNode. Compare forced alternatives with automatic choices. |

## Code-size measurements

Counts include comments and declarations. Physical lines count splitlines;
nonblank lines exclude whitespace-only lines; bytes are UTF-8 file bytes. The
scope is every tracked production .cpp/.hpp/.h in src and include, including
join_helpers.hpp. Tests, benchmarks, build products and documentation are excluded.
These are executed counts against Git objects and the final files, not estimates.
The per-file inventory is `benchmark_results/v1_phase5/code-size.json`.

| Component | Physical lines before → after | Nonblank before → after | Bytes before → after |
| --- | ---: | ---: | ---: |
| Buffer | 447 → 447 | 380 → 380 | 14,108 → 14,108 |
| Common | 31 → 31 | 20 → 20 | 530 → 530 |
| Execution | 1,873 → 1,873 | 1,723 → 1,723 | 72,273 → 72,344 |
| Index | 584 → 577 | 509 → 503 | 22,903 → 22,684 |
| Optimizer and statistics | 724 → 613 | 710 → 599 | 39,650 → 31,773 |
| Storage | 1,166 → 1,142 | 1,018 → 994 | 44,324 → 43,016 |
| Types | 261 → 261 | 227 → 227 | 9,116 → 9,116 |
| Phase 5 total | **5,086 → 4,944** | **4,587 → 4,446** | **202,904 → 193,571** |

Phase 5 removes 142 physical lines (2.8%) and 9,333 bytes (4.6%). The optimizer
implementation itself goes from 519 to 413 lines; the optimizer/statistics module
loses 15.3% of its lines and 19.9% of its bytes. Execution's line count is unchanged
because a one-line redundant validation was replaced by a contract comment.

For the complete five-phase V1 effort, the pre-V1 commit
`8fe8c9b52212a206447474dc1dfa1c954915dfba` contained 5,542 physical lines,
5,041 nonblank lines and 225,029 bytes. The final 4,944/4,446/193,571 represents
598 fewer physical lines (10.8%) and 31,458 fewer bytes (14.0%). This includes the
intentional loss of index deletion; it is not a claim that equal functionality
was compressed by that amount.

## Test revisions and verification scope

All 13 CTest suites and all supported-operation/oracle workloads remain. The
optimizer has 14 groups (13 before), including the new tiny-memory feasible-order
cases. Old assertions about forcing/rejection prose, predicted-spill/capacity
worksheets and sort-elision snapshots were removed or rewritten to inspect actual
plan kinds, ordering, memory, cost and results. Rejection tests still require an
error; only their diagnostic-string expectations changed.

The disk-hook dependency search covered headers, sources, all tests, bench and
tools. Only tests used the API. Synthetic write failures in storage primitives,
buffer flush/eviction, heap flush, B+ insert, temp append, sort, GHJ, aggregation
and integration metrics were removed; these exercised the removed injection API.
Successful writes/persistence and real stream-error detection remain tested.
Synthetic incoming-read cases became physical truncation tests in buffer, B+
cursor, basic pipelines, temp writer Finish, temp reader, sort, BNLJ, GHJ
hash/fallback and aggregation. The artificial append-tail read-after-allocation
case was removed; bad tuple, temp-cap/orphan-allocation and failed-Finish tests
still require writers to close and refuse partial-run publication.

Reset tests preserve open-reader/pin guards, normal cleanup, extent/metric checks
and new-query reuse. Startup and post-EOF FinishQuery failures now move the
scratch path, require the native filesystem error, leave success false and
require abandoning the unavailable instance. One-shot recovery/retry assertions
were removed. Test-only child errors and native allocation/length exceptions
remain: they exercise common cleanup without production injection machinery.
Padding-only production rejection assertions now verify ignored padding; the
independent B+ validator still audits encoded padding and complete structure.

| Required workload | Retained executable evidence |
| --- | --- |
| Table creation/insertion and reopen | heap_catalog, index_integration and integration: exact rows/RIDs/schema/metadata after explicit close/reopen. |
| Sequential/index scans, index build and insert maintenance | bplus_insert randomized ordered-set oracles and raw-page validator; index_integration plus optimizer scan comparisons; indexing benchmarks. |
| External sort beyond M | external_sort: independent run decoding, sorted-bag oracle, multiple passes, exact-fit and k=0/1 boundaries, finite descriptors, early cancellation/replay. |
| NLJ/BNLJ/GHJ | nested_loop_join/grace_hash_join: empty/duplicate/extreme/randomized rows, both physical sides, block boundaries, result bags and replay. |
| Skewed partitions | grace_hash_join and optimizer GraceFallback: all-equal and mixed pairs, byte-based build side, exact-fit/one-over capacity, BNLJ fallback multiplicity. |
| In-memory/spilled aggregates | aggregation/optimizer: map oracles, all selected aggregates, empty/global results, weighted AVG, overflow, repeated keys across spills and ordered reuse. |
| Automatic selection/pipelines | optimizer/integration: seq/index choices, NLJ/BNLJ/GHJ and physical outer, hash/sort choice, filter→join→aggregate→sort→projection, reopened pipelines. |
| Memory and cleanup | execution/operator suites: row/group/head peaks vs shared capacities, bounded descriptor/temp caps, destruction/early-close/replay, zero pins and empty scratch after checked cleanup. |
| No false EOF/success | physical short reads, missing scratch paths, child/native exceptions, checked arithmetic and temp-limit prefix failures; failure propagates and invalidates every emitted row. |

## Executed results

Verification ran on Darwin arm64 with AppleClang 21.0.0.21000101, across
November 26–27, 2026 (local time). Every result below comes from an executed
command. Logs and JSON/TSV are retained locally under ignored
`benchmark_results/v1_phase5/`; this document is the tracked evidence summary.

| Run | Observed result |
| --- | --- |
| Baseline complete Debug build/regression | 13/13 suites, 70.86 seconds. |
| Focused optimizer after changes | 14/14 groups, including all feasible-alternative cases and exact pipeline reference. |
| Complete final Debug build/regression | 13/13 suites, 93.30 seconds. |
| Complete final Release build/regression | 13/13 suites, 61.97 seconds. |
| Complete final ASan/UBSan build/regression | 13/13 suites, 139.09 seconds; no ASan/UBSan diagnostics. |
| Release smoke benchmark | 63/63 successful, one repetition, seed 130013; independent references and cross-alternative equivalence checked. |
| Standalone Release B+ probe | All three configurations passed insertion, every exact lookup, full ordered range and independent raw-page validation. |

All complete builds and the standalone probe build emitted no compiler warnings
or errors. No sanitizer diagnostics were observed in the completed sanitizer run.
Final CTest logs are `verified-debug-tests.log`, `verified-release-tests.log` and
`verified-sanitized-tests.log`; associated verified build logs follow full
`final-*-build.log` builds. The earlier full regression also passed in all three
configurations before the final real post-EOF cleanup test was added, then the
complete suites were rebuilt/rerun to include it.

During implementation, the first build failed on a test fixture member typo
(`dir` versus `directory`); an exploratory regression started before that build
was fixed is not accepted as evidence. The next built regression passed 12/13,
with the optimizer failing only an erroneous expected EXPLAIN line count (10
instead of 9). Both test edits were corrected, then the focused and full final
runs above passed. Those unsuccessful logs remain available; they are not counted
as passing results.

### Benchmark observations

The smoke matrix contains 3 CLOCK, 4 point, 8 range, 1 index-insert, 6 sort,
20 join and 21 aggregate cases. Every successful record has zero active pins,
zero scratch extent, zero temporary cached pages and zero temporary pins after
cleanup. A separate JSON audit checked those fields, schema version 6 and the
reference-validation marker on all 63 records (`benchmark-audit.json`). The
structured benchmark schema is unchanged; its free-text EXPLAIN is now compact.

Representative measured cases (milliseconds are single observations, not medians
from repeated trials):

| Case | Results / measured algorithm behavior | Temp pages allocated | Temp file reads / writes | Elapsed ms |
| --- | --- | ---: | ---: | ---: |
| sort_m_24576 | 480 rows; capacity 23; 21 runs; 4 intermediate levels; streamed final fan-in 2 | 97 | 96 / 96 | 2683.346 |
| join_all_equal_nlj_left | 7,200 rows; 80 inner scans | 0 | 0 / 0 | 8.958 |
| join_all_equal_bnlj_left | Same 7,200 rows; capacity 51; 2 inner scans | 0 | 0 / 0 | 6.961 |
| join_all_equal_ghj_auto | Same 7,200 rows; 1 oversized fallback pair; 2 fallback scans | 4 | 0 / 0 | 68.395 |
| join_skew_ghj_auto | 8,697 rows; 4 hash pairs and 1 fallback pair | 12 | 0 / 0 | 206.735 |
| aggregate_few_hash_seq | 4 groups held in memory; no spill | 0 | 0 / 0 | 0.441 |
| aggregate_spill_hash_seq | 350 groups; peak 117; 6 batches/649 partials; 19 sort runs/4 levels | 64 | 31 / 50 | 1298.826 |
| aggregate_spill_sort_seq | Same 350 groups; 12 sort runs/2 levels | 32 | 17 / 23 | 637.903 |
| aggregate_few_sort_index | 4 groups; input order reused; no internal sorter | 0 | 0 / 0 | 0.398 |

Temporary allocation writes initialize real pages but are separate from the
reads/writes column. Small GHJ partition fixtures stay cached; the larger
sort-composition regression requires nonzero temporary file reads and writes.
This distinction prevents treating cache hits as evidence of absent disk storage.

The 1,200-row unique point batch returns four hits across eight hit/miss lookups:
forced SeqScan recorded **376** database-page reads (5.313 ms), versus **10**
for IndexScan (1.174 ms). Indexed insertion of 1,200 rows reached height 2,
allocated 55 persistent pages, read 3 and wrote 58 (7.389 ms); final flush is
included. These are observed page/file operations, not device I/O or portable
speedup claims.

The standalone B+ probe uses random insertion order, duplicate logical keys with
unique RIDs, and a raw-page oracle outside each timing interval. Production
capacity is leaf=254/internal=204; the small fixture uses 4/4.

| Frames / rows / capacity | Insert ms / reads / writes | Lookup ms / reads | Full range ms / reads |
| --- | --- | --- | --- |
| 1 / 8,000 / 254 | 97.027 / 15,536 / 7,837 | 40.765 / 16,000 | 0.491 / 46 |
| 16 / 8,000 / 254 | 49.976 / 2,375 / 2,422 | 18.266 / 5,213 | 0.502 / 46 |
| 1 / 350 / 4 | 12.185 / 1,926 / 703 | 5.861 / 2,100 | 0.320 / 124 |

Every probe ended each phase at zero active pins and peak pins of one. The first
two allocated 45 index pages during insertion; the small-capacity case allocated
180. Lower read/write counts with more frames demonstrate cache reuse; timings
are not a controlled comparison because verification processes overlapped. No
pre/post performance improvement or universal correctness proof is claimed.
The full-size benchmark matrix and repeated performance trials were not run.

### Actual compact EXPLAIN

The independently checked pipeline fixture prints:

```text
Estimated cost = logical page transfers + 0.0001 * tuple CPU operations
Projection columns=2,0,1 rows=5 cost=6.0497 (pages=6, cpu=496.61) local=0 order=asc(1)
  ExternalSort key=0 rows=5 cost=6.0497 (pages=6, cpu=496.61) local=0.001161 order=asc(0) M=24576
    HashAggregate group=0 rows=5 cost=6.0485 (pages=6, cpu=485) local=0.0128 order=none M=32768
      GHJ keys=0,0 rows=64 cost=6.0357 (pages=6, cpu=357) local=4.0252 order=none M=65536
        Filter column 1>20 rows=80 cost=1.01 (pages=1, cpu=100) local=0.01 order=none
          SeqScan table=employees scan_ring=off [sampled] rows=100 cost=1 (pages=1, cpu=0) local=1 order=none
        Filter column 0<4 rows=4 cost=1.0005 (pages=1, cpu=5) local=0.0005 order=none
          SeqScan table=departments scan_ring=off [sampled] rows=5 cost=1 (pages=1, cpu=0) local=1 order=none
```

This fixture returns four groups even though its estimated group count is five.
The `cost`/`pages`/`cpu` values are optimizer predictions, whereas the benchmark
counts above are measurements. Projection remaps the ordering to column 1.

### Reproduction and repository checks

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 2
ctest --test-dir build --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 2
ctest --test-dir build-release --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 2
ctest --test-dir build-sanitized --output-on-failure
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --output benchmark_results/v1_phase5/smoke.jsonl
c++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Iinclude -Itests \
  bench/bplus_tree_probe.cpp build-release/libvalencedb.a -o build-release/valencedb_bplus_probe
build-release/valencedb_bplus_probe 1 8000 254
build-release/valencedb_bplus_probe 16 8000 254
build-release/valencedb_bplus_probe 1 350 4
git diff --check
```

Dependency/stale-reference searches cover production headers/sources, tests,
bench, tools, README and current documentation. Removed APIs have no remaining
code callers. Historical phase reports remain historical rather than being
rewritten. Local Markdown file links resolve. Source hashes are recorded beside
per-file code counts. Git diff whitespace checks pass. HEAD and the new archive
branch remain at the baseline; changes are uncommitted and nothing was pushed.

## Deferred to V2

These are learning directions, not implemented or verified features: B+ tree
exact deletion/redistribution/merge/root contraction and reusable pages; richer
free-space management; SQL parsing/binding and more expressions/types; multiple
join/group/order keys and multi-join enumeration; histograms/correlation/skew
estimation; recursive hash partitioning; DISTINCT aggregates/windows; persistent
statistics and automatic projection pruning. Whole-query memory admission,
transactions/WAL/crash recovery, concurrency/locking/MVCC, parallelism and networking
would need explicit new contracts. They should be separate educational projects,
not defensive infrastructure added to V1 in anticipation.

