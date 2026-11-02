# I/O metrics and cost boundaries

ValenceDB reports algorithmic work, buffer activity, and file operations separately. None measures storage-device traffic or elapsed execution time. File operations may be served by the OS page cache. Metrics are observational; they do not determine query success or plan semantics.

## Scopes and snapshots

- `BufferPool::metrics()` (also `Database::Metrics()`) starts at pool construction and accumulates through queries and checked database close. Reopening constructs a new pool. `active_pins` is a current gauge; `peak_pins` is a lifetime high-water mark.
- `DiskManager::counters()` starts at DiskManager construction. Scratch reset does not reset these counters. Reopening creates new counters.
- `QueryContext::Temps().Metrics()` starts with each context. Allocated pages, peak extent, and relation creations remain historical after cleanup. Current extent becomes zero only after successful scratch reset.
- Operator `Metrics()` resets at each `Init()` that starts an execution and survives `Close()`. Rejected duplicate Init does not reset the live execution. Aggregate internal-sort metrics are snapshots after sort Init and at aggregate Close.

Accessors return const references to live counters. Copy the small struct for a stable snapshot; subtract cumulative counters for interval work. Do not subtract peaks to infer an interval peak. There is no global reset or generic telemetry registry.

## Buffer layer

| Counter | Exact boundary |
|---|---|
| `buffer_fetches` | Fetch attempts after scan-ring ownership/mode validation; NewPage is separate |
| `buffer_hits`, `buffer_misses` | Exactly one cache lookup outcome per counted Fetch, including subsequent I/O/exhaustion failures; hits + misses = fetches |
| `new_pages` | Physical allocations successfully returned to NewPage, even if frame installation then fails |
| `evictions`, `dirty_evictions` | Completed replacement of valid frames, with the latter a dirty subset |
| `explicit_flush_writes` | Successful DiskManager Write calls from Flush, including FlushAll and dirty Invalidate; a later file Flush error does not erase the completed write |
| `dirty_eviction_writes` | Successful DiskManager Write calls for dirty victims; an installation allocation failure afterward still leaves this write counted |
| `sweep_inspections` | Each CLOCK frame visit, including pinned frames |
| `sweep_decrements` | Each decrement of an unpinned positive usage count |
| `ring_reuses` | Completed installations that reused an eligible ring handle |
| `ring_fallbacks` | Ring misses selecting a free frame or rejecting a handle and using CLOCK; counted even if later I/O fails |
| `active_pins`, `peak_pins` | Current total guard pins and lifetime maximum, including multiple guards on one frame |

Dirty eviction counts completed replacements; dirty-eviction writes count transfers. These can differ on failed installation. The two write-cause counters each cover both file namespaces. They partition BufferPool-issued writes; they are not extra file writes to add to DiskManager totals. Direct low-level DiskManager writes appear only in its counters. Clean/no-op Flush and temporary discard add no write. Ring hits consult the global mapping first and add neither ring reuse nor fallback.

## File layer and allocation

`database_reads` / `temporary_reads` count completed 4096-byte Read calls. `database_writes` / `temporary_writes` count completed 4096-byte Write calls. `failed_reads` / `failed_writes` count rejected/failed calls, including invalid page IDs and the one-shot test hook; they are not completed transfers. Flush/close/reset and allocation failures are checked errors, not read/write-counter events.

`database_allocations` / `temporary_allocations` count successful append-and-zero-initialize operations. Initialization writes one full page and flushes the stream, but is counted in allocations **instead of** ordinary writes. Therefore the file layer's successful full-page write operations, including initialization, are `writes + allocations` for each file. This is not an fsync or device-write count. Failed or partial physical transfers are not claimed as successful operations.

Textbook pass formulas and optimizer estimates omit allocation initialization. Keep actual counters intact when comparing them with the ideal model. Normal database/temp page contents use BufferPool; physical zero allocation and narrowly scoped storage tests are the exceptions.

## Query scratch extent

`allocated_temp_pages` is the cumulative physical allocation count for this query, including NewPage allocations whose frame acquisition failed. `current_temp_extent_pages` is the current append-only extent at allocation/reset boundaries. `peak_temp_extent_pages` is its historical maximum. During a query all three agree; after checked reset current becomes zero and the historical fields survive. `temp_relations_created` counts writer constructions, including empty and abandoned runs; descriptor copies add nothing.

Empty relations allocate no pages. Releasing a relation drops RAM handles without walking the chain, freeing pages, reusing IDs, performing reclamation I/O, or reducing any extent counter. Repeated Close→Init in one context adds to the same extent. The physical extent governs `max_temp_pages`, not the number of live descriptors. Low-level Buffer/Disk APIs must not bypass query-owned scratch allocation/reset.

A TempReader stages each visited page with one BufferPool Fetch; decoding multiple tuples and observing EOF on that page add no logical page transfers. TempWriter publishes each page through one Fetch and marks it dirty. Its separate NewPage allocation is not another Fetch. A logical transfer can hit the cache with no file read; a dirty frame may later be written by replacement. Tests assert these boundaries without claiming that tuple-at-a-time heap scans perform only one Fetch per page.

Successful teardown closes executors and handles, rejects temporary pins, discards all temporary cache mappings (including dirty frames), and only then checks scratch reset. A reset error means FinishQuery failed and no success is recorded; the Database remains query-active. Genuine structural/reset failure can require abandoning the instance. The focused one-shot reset test allows ordinary checked cancellation afterward; this is not a recovery guarantee.

## Algorithmic work and optimizer costs

The existing specialized counters remain authoritative: sort initial/materialized runs, intermediate passes and streamed final fan-in; BNLJ blocks/scans/comparisons; GHJ partition-pair strategies/build peaks/failed-build rows; and aggregate group peaks/overflow flushes/spill batches/records/output groups. Their exact partial-execution boundaries are in [external_sort.md](external_sort.md), [joins.md](joins.md), and [aggregation.md](aggregation.md). `spill_batches` includes the final nonempty map flush. GHJ completed-page counters omit unfinished scans, with failed build-prefix rows exposed separately.

The optimizer's `ModeledCost` uses logical pages plus `0.0001 * cpu`, not cache misses or elapsed time. Let N be estimated packed pages, r initial runs, k fan-in, and L materialized intermediate levels (repeated ceil(r/k) reductions until r<=k). Local/subtree accounting is:

| Operator | Subtree cost rule |
|---|---|
| NLJ | Outer selected path once + outer rows × the complete selected inner path + local comparisons |
| BNLJ | Outer selected path once + ceil(outer rows/C_block) × the complete selected inner path + local comparisons |
| GHJ | Both original child paths once + local partition writes and reads (ideal 2 × packed input pages) |
| ExternalSort | Child once + initial writes, intermediate read/write levels, final streamed reads: local `(2L+2)N` when external; no final output write |
| HashAggregate, fit | Child once + local hash CPU |
| HashAggregate, spill | Child once + partial writes + partial TempScan reads + reduced-budget partial ExternalSort work + combine CPU |
| SortAggregate | Ordered child once + group CPU; otherwise internal reduced-budget ExternalSort subtree once + group CPU |

An indexed/filter/project inner is charged at its selected subtree execution cost, not replaced with raw table pages. Adding all displayed children once to an NLJ/BNLJ subtree would misrepresent replay. A final projection does not add page work. [optimizer.md](optimizer.md) gives exact estimates and feasibility formulas.

Costs assume clean completion and approximate packing. Allocation/initialization, cache effects, partition/run tail waste, skew fallback and abandoned build/retry work can differ from these ideal estimates. For GHJ fallback, replace that pair's hash read with outer pages plus block-count times inner pages; any failed build prefix is extra. No reclamation cost is subtracted when descriptors die. Statistics affect choices only; fresh, stale and forced plans retain the same executable predicates.
