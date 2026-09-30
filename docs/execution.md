# Execution and temporary relations

The execution layer supplies SeqScan, IndexScan, Filter, Projection, query lifetime, temporary relations, and numeric memory configuration. [ExternalSort](external_sort.md), [NLJ/BNLJ and TempScan](joins.md), [Grace Hash Join](joins.md#grace-hash-join), and [HashAggregate/SortAggregate](aggregation.md) build on these contracts. There is no SQL parser, expression tree, operator registry, plan factory, scheduler, or generic optimizer framework. A [small physical selector](optimizer.md) composes the available operators. Basic configuration descriptors are ordinary structs in `execution/plan.hpp`; executor trees use direct composition with `std::unique_ptr<Executor>` children. ExternalSort's descriptor lives in `execution/external_sort.hpp` and carries the same optional numeric memory override.

## Executor lifecycle and ownership

`Executor` exposes `Init()`, `Next(Tuple&)`, `Close() noexcept`, `OutputSchema()`, and `OutputOrdering()`. All current operators use these semantics:

1. Construction validates configuration and resolves output metadata without starting execution.
2. `Next` before Init or after Close throws `DbError`.
3. Init starts a stream. A second Init before Close throws, including after EOF.
4. Next returns true for one **owned** tuple; false means normal EOF only. Repeated EOF returns false and leaves the output unchanged.
5. Close releases scan/cursor/child state, never throws, and is safe before Init or repeatedly. Derived destructors call Close. Children remain owned for replay.
6. Close followed by Init replays from the start within the same active QueryContext. There is no database mutation during that context. Init clears the EOF marker; Close preserves the marker so a caller can close an exhausted tree before FinishQuery.

`Exhausted()` records an observed normal EOF, not merely the delivery of the last row. Constructor/Init/Next validation and storage errors throw `DbError`; they are never converted to EOF. A failed Init closes partially opened children. After an execution error, callers stop consuming, close/destroy the tree, close temporary handles, call checked context Close, and discard the entire output prefix. There is no partial-result commit or poisoned-query framework. Unexpected structural I/O failures can require abandoning the database under the existing storage contract.

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

The lifetime order is **Database → QueryContext → executors/temp descriptors → temporary readers/writers**. Objects on the left outlive objects on the right. QueryContext is noncopyable/nonmoving, references Database, owns QueryResources and one TempStore, and counts initialized executors. There are no transactions, locks, or sessions.

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

Open-handle and pinned-page rejection leaves scratch intact; close the handles/drop guards and retry the checked sequence. Reset failure propagates and leaves the Database query-active flag set, so another query cannot silently start. A deterministic one-shot reset hook may be retried through ordinary cleanup; genuine structural I/O failure may require abandonment. If startup fails after activation, no completed context exists to retry, so abandon that database instance. No retry state machine is provided. Destructors attempt cleanup without throwing; they cannot report checked success and must not replace explicit FinishQuery/Close. The lifetime requirements still apply on exception paths.

Truncating before cache discard would let a later dirty-frame flush recreate stale scratch contents. The implemented order prevents that. Discard is special whole-query disposal; ordinary in-query dirty temp eviction still writes the page so it can be read later.

## Memory configuration

`QueryResources` defaults to `work_memory_bytes = 1,048,576` (1 MiB) and `max_temp_pages = 16,384` (64 MiB of scratch extent). Both are positive; the page cap must also fit the physical page-count representation.

`ResolveBudget(resources, optional_override_bytes)` returns only `OperatorMemoryBudget { max_bytes }`. Each heavyweight operator independently receives the query default or its positive local override. Two operators can each retain M simultaneously. M is **per heavyweight operator**, not a global query cap or an exact process-RSS guarantee. Basic operators allocate no M-sized buffer; they retain cursors, bounded descriptors, and a few owned tuples. A positive but tiny M is legal configuration; an algorithm must reject it if its required scratch/capacity cannot fit.

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

`TempRelation(store, const Schema&)` is a noncopyable/nonmoving descriptor holding references to the TempStore and immutable schema, first/last PageIds, row/page counts, total modeled row bytes, and small lifecycle fields. It has no row/RID/page-ID vector and never deep-copies Schema. Both the store and the immutable schema owner must outlive the relation and its handles. Construction from an rvalue Schema is deleted to prevent the common dangling-reference case. Longer-lived references remain the caller's responsibility, as with storage cursors.

Construction allocates no page. Empty endpoints are INVALID and counts are zero. Exactly one TempWriter may ever open the descriptor. A closed unsealed writer abandons its tail; it cannot be resumed by a new writer. TempReader opens only a sealed, live relation. Independent readers each have their own page, slot, and row/page counters. Rewind restores a reader to the first page. Next on a closed reader and Append/Seal on a closed writer throw. Repeated Close is safe. Seal closes the writer; a second Seal throws.

Each reader/writer owns one private `PageBytes` (4096 bytes), even when empty. Close drops its relation access/active-handle registration; the fixed array's storage lasts until object destruction. Count these arrays in their owning operator's private memory, including closed-but-retained handle objects if an operator keeps them. This explicit cost buys short pins and simpler later external operators.

TempWriter Append encodes and validates one tuple, checks row-charge arithmetic, and copies it into its private slotted page. The first physical page is allocated only for the first successful append attempt requiring space. If a tail is full, allocate the next Temporary PageId **before** linking/publishing the old tail; copy the old image to a briefly fetched frame, MarkDirty, release the guard, and initialize the new private page. Seal publishes the final tail with an invalid next link, marks the descriptor sealed, and closes the writer. Empty Seal publishes nothing.

TempReader fetches one Temporary frame, copies the full page into its private image, releases the guard, validates the slotted layout, and decodes owned tuples from that image. It follows forward links and skips tombstones if present. It checks forward progress and descriptor row/page/tail agreement instead of treating corruption as EOF. Rewind and new independent readers do not share a mutable cursor.

Temporary pages use `PageRef{FileKind::Temporary,id}` and the existing **Heap slotted-page layout/type**, with `owner_meta_page = kInvalidPageId`. No separate tuple format is introduced. Database page 0 and Temporary page 0 are distinct. All page contents move through BufferPool; TempStore consults DiskManager only for extent and checked scratch lifecycle. There is no direct DiskManager Read/Write in execution code.

The tradeoff is a 4096-byte copy per page transfer and one private page per live handle. In return, no TempReader/Writer guard crosses a Next/Append/Seal return or child call. Page-at-a-time external I/O stays visible and later algorithms need not coordinate long-lived buffer pins.

## Append-only release and limits

To release a relation, first Close/destroy every reader/writer, then call `relation.Release()` and drop the descriptor when convenient. Release rejects open handles, preventing dangling active access; it clears descriptor counts/endpoints and can be repeated. It never walks a page chain, fetches a page, truncates the file, or returns a PageId for reuse. A released descriptor cannot open another reader/writer. Descriptors may remain in scope through successful query cleanup, but are unusable afterward.

Every temporary allocation during the context counts toward max_temp_pages, including abandoned tails, released relations, earlier replay state, and pages physically allocated by BufferPool::NewPage before it fails to acquire a frame. Check the physical extent **before** extending the file. Cap errors throw DbError. No page recycling, free list, mid-query truncation, or eviction of scratch extent is provided. PageIds can restart only after the previous query has completed checked cleanup.

`TempMetrics` tracks cumulative `allocated_temp_pages`, `peak_temp_extent_pages`, and `temp_relations_created`, plus the current gauge `current_temp_extent_pages`. Allocated/current/peak pages agree during append-only allocation, including physically allocated pages from failed NewPage installations. Successful cleanup sets only current extent to zero; cumulative fields remain historical. Each QueryContext starts fresh metrics. [io_cost_models.md](io_cost_models.md) defines all layers and snapshot boundaries. `ModeledRowBytes()` sums T(schema) per appended row. DiskManager and BufferPool retain their existing physical-I/O and logical-cache metrics. No wider observability framework is added.

Intentional limits remain single process/thread/active query, narrow INT64 predicates and ordering, caller-managed reference lifetimes, append-only scratch, no whole-query RSS enforcement, no SQL, concurrency, WAL, transactions, or crash recovery. ExternalSort, NLJ/BNLJ, Grace Hash Join, and aggregation preserve these contracts; the [optimizer](optimizer.md) reuses the same lifecycle and memory rules.

## Source and self-check

[executor.cpp](../src/execution/executor.cpp) implements lifecycle/checked completion; [basic_executors.cpp](../src/execution/basic_executors.cpp) implements the streaming operators; [temp_relation.cpp](../src/execution/temp_relation.cpp) implements staged append/read/reset; [work_budget.cpp](../src/execution/work_budget.cpp) contains checked charges. Basic filters/projections do linear tuple work with bounded per-row state; IndexScan adds tree and heap access. Temp transfer work is linear in relation pages plus row decoding.

The execution and integration tests cover guard scope around child calls, scratch extent/reset, replay, failure propagation, prefix invalidation, successful FinishQuery, and cleanup after cancellation.
