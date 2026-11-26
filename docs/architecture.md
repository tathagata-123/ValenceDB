# ValenceDB v1 architecture

ValenceDB connects a page-oriented row store to owned-tuple executors and a small physical-plan selector. The design keeps ownership, page lifetimes, temporary storage, and modeled memory explicit so each algorithm can be reasoned about from storage through execution.

```text
                      QuerySpec + PlanOptions
                                |
                   ChoosePlan / Explain (optimizer)
                                |
                    owning Executor tree, Next(Tuple&)
                       /                      \
          Database / heap / B+ tree        TempRuns
                       \                      /
                         shared BufferPool (F)
                                |
                           DiskManager
                  persistent file + scratch file
```

From the bottom up, `DiskManager` transfers 4096-byte pages, `BufferPool` caches them, heap/catalog/index helpers interpret their explicit formats, and `Database` coordinates persistent mutation and clean reopen. Executors consume owned tuples; external algorithms use the same pool through staged temporary pages. `ChoosePlan` enumerates existing physical alternatives and constructs the winning executor tree without executing it. It and `Explain` are free functions, not an `Optimizer` class.

The optimizer pipeline remains explicit: generate scans → enumerate joins and
physical orientations → enumerate aggregates → apply required sort/projection →
select the cheapest feasible complete plan → build executors. One tagged
`PhysicalPlan` describes each actual operator and carries one resolved memory
allowance when needed. Shared memory helpers in work_budget.cpp derive capacities for both optimizer and execution. A pure
sort estimator charges explicit sorting, aggregate input sorting, and partial-state
sorting without creating synthetic nodes. SortAggregate owns its internal sorter;
its plan child is the real input, and its local cost includes the internal sort.
EXPLAIN formats selected facts after planning rather than storing prose in candidates.

## Ownership and lifetimes

```text
Database
  ├─ DiskManager (physical database and scratch file streams)
  ├─ BufferPool (borrows DiskManager; owns frames/page table/CLOCK)
  ├─ Catalog
  ├─ stable BPlusTree handles, one per indexed table
  └─ in-memory statistics

caller scope
  ├─ QueryContext (borrows Database)
  │    ├─ QueryResources value
  │    └─ TempStore (logical scratch lifetime/cap/resource count)
  └─ OptimizedPlan or caller-owned root Executor
       └─ uniquely owned children and operator working state

PageGuard -> one BufferPool pin
TempReader / TempWriter -> one private PageBytes each
TempRun -> sealed copyable value borrowing TempStore and one immutable Schema
BPlusIterator -> borrows a stable tree; stores epoch/page/offset, no retained pin
```

Database outlives the context, pool users and tree cursors. Context outlives **all** executors and temporary handles; it does not own the root. Construct the context before the plan so destruction reverses that order. Parent executors own children using `unique_ptr`; descriptors are inspection values, not executable ownership. Operator-owned schemas outlive their run/partition/partial descriptors and readers. `PageGuard::Drop` releases exactly one pin; no byte reference may outlive that pin. Catalog metadata and index handles do not retain page guards.

Database physically opens the scratch file, while each QueryContext resets and exclusively uses it. There is one scratch stream for the active query, not one file per operator. Dropping a TempRun value needs no cleanup; readers and scans own descriptor copies. Append-only IDs and extent remain charged until query cleanup.

## A representative pipeline

The `PartitionedGroupPipeline` fixture in `tests/integration_test.cpp` composes:

```text
SeqScan(left) + SeqScan(right)
        -> GraceHashJoin -> HashAggregate spill -> ExternalSort -> Projection
```

IndexScan can replace a legal base access: `IndexScan::Next` obtains an owned `(key,RID)` from `BPlusIterator::Next`, which drops its leaf pin, then fetches the heap tuple. Each tuple owns decoded values, including VARCHAR bytes. Parent activity may evict either page safely.

GHJ consumes each original child once into matching temporary partitions. Its left writer pages die before right writers allocate; all writers die before build. Each pair either skips, completely builds then probes, or uses BNLJ over TempScans before emitting anything for that pair. Output always uses logical Left+Right columns.

HashAggregate consumes that stream into bounded selected-field states. A new group at capacity flushes partial states; the triggering row is processed once. After any spill the final map is flushed too. The map and writer are destroyed before an internal ExternalSort combines adjacent partials. AVG combines sum/count. A final ExternalSort is still needed when required because HashAggregate promises no order. Its bounded run vector dies before merge readers allocate, and its final merge streams directly into Projection.

All temporary readers/writers own private page images. Transfer is `brief Fetch guard -> full-page copy -> guard release`. No guard survives a child call, Append return, or Next return. Persistent and temporary PageRefs have separate identities but compete for the same F frames.

## Memory and I/O boundaries

F is the shared frame count; complete queries require F ≥ 8. M defaults independently for **each heavyweight operator** through `QueryResources::work_memory_bytes`. A live join, aggregate, and final sort may each retain their own modeled budget. There is no query-tree admission or exact RSS bound.

```text
T(schema) = round_up_to_8(64 + 32*ncols + 2*max_encoded_row_bytes)
S = 2*4096 + 3*Tmax + 1024
```

`RowMemoryCharge`, `FixedScratchCharge`, and each `Compute*Memory` helper validate arithmetic before reserve subtraction. Hash aggregate's internal sort gets M minus combine scratch and its separately owned TempScan page and handle allowance; unordered SortAggregate subtracts combine scratch. These are educational charges, not allocator inspection. Phase changes destroy large allocations; `clear()` is used only when reuse stays inside the same phase.

[ExternalSort](external_sort.md), [joins](joins.md), and [aggregation](aggregation.md) derive the capacities. [I/O cost models](io_cost_models.md) separates logical passes, BufferPool requests, misses, file reads/writes, initialization, and unmeasured device activity. Relation release performs no reclamation I/O and never subtracts historical scratch extent.

## Completion and failures

`QueryContext::FinishQuery` requires observed root EOF, closes the tree, and calls checked context cleanup. `TempStore::Reset` rejects any initialized executor or open reader/writer using one shared resource count, `DiscardTemporaryPages` checks pins and discards mappings, and only then does `DiskManager::ResetTemporary` truncate/reopen scratch. Success is recorded after all these steps succeed. Closed executor objects may still exist; their working handles must be gone.

For cancellation, close the tree/handles then call `QueryContext::Close`; cancellation never records success. `Next()==false` means EOF, never error. Common Executor Init/Next entry points close failed subtrees and propagate original exceptions, including native allocation/length errors. Any execution error invalidates the whole emitted prefix. Unexpected structural I/O failure may leave heap/index state uncertain: propagate the error and abandon the instance (stream errors are `DbError`; filesystem operations may throw `std::filesystem::filesystem_error`). There is no WAL, transaction rollback, poisoned-handle framework, or crash recovery.

## Metrics, benchmarks, and reading route

Buffer/file counters belong to their pool/disk instance; TempMetrics belongs to one query; operator metrics reset on Init and survive Close. Copy metrics before taking deltas. The benchmark harness times planning, Init, Next through EOF, bounded result consumption, and FinishQuery; exact reference checks occur separately. The benchmark methodology is described in [benchmarks.md](benchmarks.md).

A useful reading order is [storage](storage.md) → [buffer manager](buffer_manager.md) → [B+ tree](bplus_tree.md) → [execution](execution.md) → external algorithms → [optimizer](optimizer.md). Production extensions such as transactions, concurrency, recovery, and a SQL frontend are intentionally outside the current scope.
