# Execution and temporary relations

The execution layer supplies SeqScan, IndexScan, Filter, Projection, query lifetime, temporary relations, and numeric memory configuration. [ExternalSort](external_sort.md), [NLJ/BNLJ and TempScan](joins.md), [Grace Hash Join](joins.md#grace-hash-join), and [HashAggregate/SortAggregate](aggregation.md) build on these contracts. There is no SQL parser, expression tree, operator registry, plan factory, scheduler, or generic optimizer framework. A [small physical selector](optimizer.md) composes the available operators. Basic configuration descriptors are ordinary structs in `execution/plan.hpp`; executor trees use direct composition with `std::unique_ptr<Executor>` children. ExternalSort's descriptor lives in `execution/external_sort.hpp` and carries the same optional numeric memory override.

## Executor lifecycle and ownership

`Executor` exposes `Init()`, `Next(Tuple&)`, `Close() noexcept`, `OutputSchema()`, and `OutputOrdering()`. The nonvirtual public entry points own lifecycle state and exception cleanup. Operators implement private `DoInit`, `DoNext`, and `DoClose` methods containing their algorithms and resource releases. Derived destructors call Close while their members are still alive. All operators use these semantics:

1. Construction validates configuration and resolves output metadata without starting execution.
2. `Next` before Init or after Close throws `DbError`.
3. Init starts a stream. A second Init before Close throws, including after EOF.
4. Next returns true for one **owned** tuple; false means normal EOF only. Repeated EOF returns false and leaves the output unchanged.
5. Close releases scan/cursor/child state, never throws, and is safe before Init or repeatedly. Derived destructors call Close. Children remain owned for replay.
6. Close followed by Init replays from the start within the same active QueryContext. There is no database mutation during that context. Init clears the EOF marker; Close preserves the marker so a caller can close an exhausted tree before FinishQuery.

`Exhausted()` records an observed normal EOF, not merely the delivery of the last row. Validation and stream I/O errors throw `DbError`; they are never converted to EOF. Allocation and length errors propagate as their original standard exception types. Filesystem errors from scratch reset during query startup/cleanup propagate as `std::filesystem::filesystem_error`; callers handling all storage failures catch `std::exception`. The common Init/Next boundary catches any execution exception, closes that subtree, and rethrows unchanged. As an exception propagates to the root, each parent closes its remaining children and owned scratch; local RAII objects have already unwound. Thus root Init/Next failure leaves the tree closed, including after partial initialization. Callers then close any separately owned temporary handles, call checked context Close, and discard the entire output prefix. No operator has an allocation-exception translation wrapper. Closed resources do not reopen automatically, and retry after an unexpected failure is not a V1 recovery guarantee. There is no partial-result commit or poisoned-query framework. Unexpected structural I/O failures can require abandoning the database under the existing storage contract.

Tuple and Value already own vectors and strings. A scan decodes owned bytes; Filter moves an owned matching tuple; Projection copies selected Values into a new tuple. No output references BufferPool frames, private staging pages, or a local child tuple. Output remains valid after another Next, eviction, child activity, or Close.

## Ordering and basic operators

`Ordering { bool ordered = false; size_t column = 0; }` represents unordered output or ascending order on one NONNULL INT64 output column. The column field is meaningful only when ordered is true. There are no descending, multicolumn, collation, or NULL-ordering rules.

| Operator | Configuration and streaming behavior | Ordering |
|---|---|---|
| SeqScan | `SeqScanPlan { table, use_scan_ring=false }`; delegates to HeapScan, skips tombstones, decodes one live row at a time | Unordered |
| IndexScan | `IndexScanPlan { table, column, low?, high? }`; streams a BPlusIterator then fetches the owned RID through Database | Ascending indexed column |
| Filter | `FilterPlan { Int64Predicate }`; streams a unique_ptr child and skips false rows | Preserves child metadata |
| Projection | `ProjectionPlan { vector<size_t> columns }`; copies selected positions into a new intermediate tuple | First surviving occurrence of the ordered source column, remapped to its output position; otherwise unordered |

SeqScan's explicit ring switch is deterministic. When enabled, it owns a default buffer manager ScanRing and passes it through `Database::Scan(name, ring)` to HeapScan. Disabled mode still uses ordinary `AccessMode::Scan` hit behavior, with no ring. HeapScan and BPlusIterator retain no guards between Next calls. Close destroys the scan before its ring. No ordering is inferred from heap layout.

IndexScan validates table existence, index presence, and the requested column against the actual table index. Absent bounds mean INT64_MIN/INT64_MAX; equal bounds implement equality; both bounds are inclusive; reversed bounds return empty. There is no endpoint arithmetic. Duplicates stream in physical `(key,RID)` order while metadata advertises only the logical key. Each cursor Next releases its leaf guard **before** Database::Fetch pins the heap. Missing/deleted RIDs, foreign heap owners, and mismatched row keys raise DbError instead of being skipped.

`Int64Predicate { column, CompareOp, int64_t constant }` supports Equal, Less, LessEqual, Greater, and GreaterEqual. Validate rejects invalid columns, nullable/non-INT64 targets, and unsupported enum values. Its typed constant is already an INT64. No VARCHAR predicates, expressions, arithmetic, OR, or SQL NULL semantics are implemented.

Projection accepts repeated positions and retains each source Column's name, type, VARCHAR bound, and nullability. It rejects empty output, invalid positions, more than 16 columns, and maximum encoded width above 2048. `IntermediateSchema(vector<Column>)` reuses the existing Schema validation for these limits and combined outputs. `ValidateInt64Key(schema,column)` centralizes the NONNULL INT64 restriction; it adds no sort/join/group algorithm.

## QueryContext and completion

The lifetime order is **Database → QueryContext → executors and temporary handles**. Objects on the left outlive objects on the right. QueryContext is noncopyable/nonmoving, references Database, owns QueryResources and one TempStore. TempStore holds the query-active state and one count of initialized executors plus open temporary readers/writers. This single reset guard is needed because independent executor trees and standalone handles can coexist. Opening a resource checks activity once; per-row calls check only their local open/EOF state. There are no transactions, locks, or sessions.

Construction validates positive resource limits and requires **at least eight BufferPool frames**. Storage-only tests and APIs remain legal with smaller pools. Database permits one active context. While active, Insert, Delete, CreateIndex, CreateTable, and Database::Close reject the operation before mutation. Successful checked query cleanup releases this restriction. Low-level Buffer/Disk access remains for storage internals and focused fixtures; applications must not bypass Database mutation or query scratch ownership through it.

Startup activates the query, discards all stale Temporary cache entries, and resets the existing DiskManager scratch file. The context owns this file's query lifecycle; DiskManager still owns its physical stream/path. Reset never opens an additional scratch file.

Successful completion uses `query.FinishQuery(root)`:

1. Require that the root belongs to this context and has observed EOF.
2. Close the root tree; reject if another initialized executor remains open.
3. Require that all temporary readers/writers have been closed or destroyed.
4. `BufferPool::DiscardTemporaryPages()` first rejects any pinned Temporary frame, then discards all Temporary cache mappings, including dirty pages without writes.
5. `DiskManager::ResetTemporary()` checks flush/close/truncate/reopen of the one scratch file.
6. Only after cleanup succeeds, deactivate the query and record `Succeeded() == true`.

For early Close, temp-only work, or execution failure, explicitly close the tree/temporary handles and call `query.Close()`. This checked cancellation/cleanup never records success. It is idempotent after successful cleanup. FinishQuery is for an active context; repeated FinishQuery after completion is invalid. Closed executors cannot restart after their context ends.

Open-handle and pinned-page rejection leaves scratch intact; close the handles/drop guards and retry the checked sequence. Reset failure propagates and leaves the Database query-active flag set, so another query cannot silently start. The reset path has no injection/retry hook; unexpected I/O failure may require abandonment. If startup fails after activation, no completed context exists to retry, so abandon that database instance. No retry state machine is provided. Destructors attempt cleanup without throwing; they cannot report checked success and must not replace explicit FinishQuery/Close. The lifetime requirements still apply on exception paths.

Truncating before cache discard would let a later dirty-frame flush recreate stale scratch contents. The implemented order prevents that. Discard is special whole-query disposal; ordinary in-query dirty temp eviction still writes the page so it can be read later.

## Memory configuration

`QueryResources` defaults to `work_memory_bytes = 1,048,576` (1 MiB) and `max_temp_pages = 16,384` (64 MiB of scratch extent). Both are positive; the page cap must also fit the physical page-count representation.

`ResolveBudget(resources, optional_override_bytes)` returns the numeric `size_t` allowance. All sort, BNLJ, Grace, and aggregate capacity calculations live in `work_budget.cpp`; optimizer and operators call these same functions. Each heavyweight operator independently receives the query default or its positive local override. Two operators can each retain M simultaneously. M is **per heavyweight operator**, not a global query cap or an exact process-RSS guarantee. Basic operators allocate no M-sized buffer; they retain cursors, bounded descriptors, and a few owned tuples. A positive but tiny M is legal configuration; an algorithm must reject it if its required scratch/capacity cannot fit.

BufferPool memory is the shared page cache. Operator memory contains private owned tuples, algorithm state, and reader/writer staging images. Work memory is not additional BufferPool frames. The centralized educational charges are:

```text
T(schema) = round_up_to_8(64 + 32 * ncols + 2 * max_encoded_row_bytes)
P = 4096
Tmax = maximum T across relevant input/output/internal schemas
S = 2*P + 3*Tmax + 1024
H(build_schema) = T(build_schema) + 96
G = 192
```

`RowMemoryCharge`, `FixedScratchCharge`, `HashBuildRowCharge`, and `kAggregateGroupCharge` expose these formulas. S models two private pages, a few tuples, and fixed state; further simultaneously live pages cost P each. The charges approximate ownership and vector/value/string overhead, not `sizeof(Tuple)` or allocator internals. Grace Hash Join uses the hash row charge for bounded builds; aggregation uses G=192 for bounded groups.

`CheckedAdd` and `CheckedMultiply` reject size overflow, including round-up overflow. `RemainingMemory(M,reserve)` verifies M >= reserve before subtraction. A zero remainder is safe arithmetic; each algorithm still checks its minimum useful capacity. There are no memory grants, query-tree reservations, or generic pin/resource schedulers.

## Temporary relations and handles

TempRun is a small copyable sealed descriptor: store and immutable-schema pointers, first/last PageIds, and exact row/page counts. It has no sealing flag, release state, writer ownership, per-run handle count, or heap allocation. Only TempWriter::Finish() constructs a run. Both the store and immutable schema owner must outlive every descriptor copy, reader, and scan. TempWriter rejects rvalue schemas at compile time; keeping named schemas alive and unchanged remains the caller's responsibility, as with storage cursors.

Construct TempWriter(store, schema), append tuples, then obtain a TempRun value from Finish(). The writer owns all unfinished metadata. Construction allocates no page; finishing an empty writer returns invalid endpoints and zero counts. Finish publishes the last page before returning the value, then unregisters the writer. Any Append or Finish exception closes the writer, so a failed or abandoned prefix cannot later be published as a valid run. Append/Finish after completion or closure throw; repeated Close is safe.

Each reader/writer owns one private PageBytes (4096 bytes), even when empty. Close decrements the shared reset guard exactly once; the fixed array remains until object destruction. Count closed-but-retained arrays in operator budgets. TempStore refuses query cleanup while any handle is registered.

Append encodes and validates one tuple, checks row-count arithmetic, and inserts into the staging page. If a tail is full, allocate the next Temporary PageId before linking/publishing the old tail; copy the old image into a briefly fetched frame, MarkDirty, release the guard, and initialize the new private page. Finish publishes the final tail with an invalid next link. Empty Finish publishes nothing.

TempReader copies the completed descriptor, so source descriptor moves, assignments, vector reallocation/reordering, and destruction cannot affect it. It fetches one Temporary frame, copies its page image, releases the guard, validates the slotted header/owner/bounds, and decodes owned tuples with accessed-payload bounds checks. As with heap pages, it no longer performs exhaustive overlap or slot/live-count audits on each page load; see [storage.md](storage.md). It requires strictly increasing links, preventing infinite traversal in append-only scratch; BufferPool/DiskManager also reject unallocated page IDs. It does not recount rows/pages or reconcile the final page with the descriptor. Exact descriptor counts come from TempWriter, with independent structural/count audits in tests. Arbitrary scratch corruption, such as an early terminal link or an altered row count, is not comprehensively diagnosed. Rewind resets its private cursor; repeated EOF stays false. Next/Rewind after Close and opening a reader after query cleanup throw.

Temporary pages use `PageRef{FileKind::Temporary,id}` and the existing **Heap slotted-page layout/type**, with `owner_meta_page = kInvalidPageId`. No separate tuple format is introduced. Database page 0 and Temporary page 0 are distinct. All page contents move through BufferPool; TempStore consults DiskManager only for extent and checked scratch lifecycle. There is no direct DiskManager Read/Write in execution code.

The tradeoff is a 4096-byte copy per page transfer and one private page per live handle. In return, no TempReader/Writer guard crosses a Next/Append/Finish return or child call. Page-at-a-time external I/O stays visible and later algorithms need not coordinate long-lived buffer pins.

## Append-only release and limits

Dropping a run value performs no I/O, page-chain walk, truncation, or page reclamation. Readers and scans keep their own copies; no descriptor address has to remain stable. Close/destroy all handles before checked query cleanup, which discards cached temporary pages before resetting scratch. Descriptor values may remain in scope through cleanup, but cannot open readers afterward.

Every temporary allocation during the context counts toward max_temp_pages, including abandoned tails, dropped runs, earlier replay state, and pages physically allocated by BufferPool::NewPage before it fails to acquire a frame. Check the physical extent **before** extending the file. Cap errors throw DbError. No page recycling, free list, mid-query truncation, or eviction of scratch extent is provided. PageIds can restart only after the previous query has completed checked cleanup.

`TempMetrics` retains historical `allocated_temp_pages` (successfully installed NewPage calls) and `temp_relations_created` (writer constructions, including empty/abandoned writers). No failure-path metric repair is performed. On successful append-only execution allocated pages also give the peak extent; after a failed installation the physical extent may be larger. The authoritative current extent and page-cap input is `DiskManager::PageCount(Temporary)`, which becomes zero after successful reset. Both historical counters survive cleanup; each new QueryContext starts fresh metrics. DiskManager and BufferPool retain page-I/O and cache metrics. See [io_cost_models.md](io_cost_models.md).

Intentional limits remain single process/thread/active query, narrow INT64 predicates and ordering, caller-managed reference lifetimes, append-only scratch, no whole-query RSS enforcement, no SQL, concurrency, WAL, transactions, or crash recovery. ExternalSort, NLJ/BNLJ, Grace Hash Join, and aggregation preserve these contracts; the [optimizer](optimizer.md) reuses the same lifecycle and memory rules.

## Source and self-check

[executor.cpp](../src/execution/executor.cpp) implements lifecycle/checked completion; [basic_executors.cpp](../src/execution/basic_executors.cpp) implements the streaming operators; [temp_relation.cpp](../src/execution/temp_relation.cpp) implements staged append/read/reset; [work_budget.cpp](../src/execution/work_budget.cpp) contains checked charges. Basic filters/projections do linear tuple work with bounded per-row state; IndexScan adds tree and heap access. Temp transfer work is linear in relation pages plus row decoding.

The execution and integration tests cover guard scope around child calls, scratch extent/reset, replay, failure propagation, prefix invalidation, successful FinishQuery, and cleanup after cancellation.
