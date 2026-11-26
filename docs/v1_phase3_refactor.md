# V1 Phase 3: common query execution infrastructure

## Baseline and intended contract changes

Baseline: `37b6c7b6c59308c342262ea9b7fe27b5b990ec05`, preserved before
implementation on local branch `archive/pre-v1-execution-37b6c7b`.
Working branch: `v1/algorithm-first`; starting worktree clean. No AGENTS.md
was found in the repository or its ancestors. No commit or push is requested.

Dependency inspection covered Executor and all eleven concrete operators,
QueryContext/Database query exclusion, temporary handles and descriptors,
optimizer capacity/cost callers, execution/integration tests, and benchmark
emission/validation. Children are owned by their operator; standalone temporary
handles and multiple independent trees can also coexist in one context.

Contracts decided before implementation:

- Keep public Init/Next/Close/schema/ordering. Central Executor entry points
  manage lifecycle and close partially initialized or failed subtrees before
  propagating the original exception. Operator implementations supply the
  algorithm and resource cleanup only. Native bad_alloc and length_error are
  no longer translated to DbError. Errors never become EOF.
- Combine initialized-executor and temporary-handle counts into one reset guard.
  Retain this guard because independently owned trees/handles can coexist.
  Check query activity when opening resources, not for every tuple. Checked
  query completion/cancellation still resets scratch only after all resources
  close; failed reset leaves Database unavailable for mutation/new queries.
- Keep TempWriter failure closure: standalone writers need to prevent publishing
  a failed prefix. Remove reader row/page reconciliation and tail auditing;
  keep page identity/type/bounds and strictly increasing links to bound traversal.
  Writer row/page counts remain exact for supported operations, including Grace
  partition fit decisions. Remove redundant total modeled row bytes.
- Keep one historical successful temporary-page allocation counter; use the
  physical DiskManager extent for current extent and the page cap. Remove duplicate
  peak/current counters and failure-path metric repair. Failed-operation metrics
  are not complete accounting of the physical extent.
- Return size_t from ResolveBudget. Centralize operator capacity formulas in
  work_budget.cpp, shared with optimizer. Preserve algorithm capacities except
  any necessary correction for simultaneously retained staging overhead.
- Preserve algorithms, supported operations, disk spilling, resource limits,
  persistence, and single-active-query/mutation exclusion. No rollback, recovery,
  or exact process-wide memory guarantee is added.

## Removed mechanisms and replacements

| Removed mechanism | Reason and replacement |
|---|---|
| Per-operator Start/Ready/End/CloseState calls and Init/Next catch wrappers | Executor owns the iterator protocol in one implementation. Operator bodies now show tuple processing and resource release directly. |
| Eighteen bad_alloc/length_error translation handlers | Native exceptions already describe the failure category. The common boundary closes the subtree and rethrows unchanged. |
| Separate QueryContext open-executor count and TempStore handle count | One open-resource count guards reset for both; no executor registry, root pointer, call-depth tracking, or new session object is introduced. |
| QueryContext's duplicate active flag | TempStore holds context activity; Database retains its separate query-exclusion flag. |
| Query activity checks on every Executor::Next, TempReader::Next/Rewind, TempWriter::Append/Finish, and TempStore allocation | Resources check activity when opened. The reset guard prevents their context from closing while they remain usable. Local open checks remain to prevent invalid access after Close. |
| TempReader rows_read/pages_read, descriptor row/page reconciliation, and final-tail verification | Sealed descriptors come from the writer; exhaustive scratch consistency auditing is outside V1. Strictly increasing links prevent cycles; fetched pages and accessed payloads remain bounded. |
| TempRun/TempWriter accumulated modeled row bytes and ModeledRowBytes accessor | The quantity was only rows times the schema charge, used by tests rather than admission decisions. Exact row/page counts remain. |
| TempMetrics current/peak extent fields and exception-path counter updates | DiskManager owns current physical extent. Successful append-only allocations provide historical algorithmic work without duplicate counters. |
| Grace failed_build_rows instrumentation | Failure-prefix diagnostics do not determine the strategy or enforce the memory bound. The runtime build cap, fallback, build peaks and result tests remain. |
| OperatorMemoryBudget | ResolveBudget returns size_t. All existing Compute*Memory formulas now live in work_budget.cpp and remain shared by optimizer/execution. |
| Benchmark pre-reset extent sum, peak extent, and failed-build fields | Redundant/failure-only diagnostics are removed in schema version 5. Cleanup evidence, page I/O, buffer activity and algorithm counters remain. |

No executable database operation, algorithm, storage format, test scenario, suite,
CMake target, or fault hook was removed. Join/sort/aggregate code changes are
common-protocol adaptation, moving capacity calculations, one accounting correction,
and the removed diagnostic; their algorithms are unchanged.

## Resulting lifecycle and cleanup

Construct the Database, then QueryContext, then executors/temporary handles; destroy
in reverse order. The public Volcano methods remain Init, Next and Close, alongside
schema and ordering. Private DoInit/DoNext/DoClose hooks implement each operator.
Derived destructors still call Close while their members exist. Init marks the
executor open before entering its algorithm, so partial initialization is closable.
Next checks initialization, returns repeated EOF without calling the algorithm,
and records EOF only after a normal false return. Close is idempotent, releases the
subtree, then drops its reset guard; it preserves observed EOF for FinishQuery.
Close followed by Init replays, including NLJ/BNLJ inner-child restarts.

The public Executor entry points are the shared execution error boundary. Every
Init/Next exception closes that subtree before propagating unchanged. Parent entry
points close remaining siblings as the exception unwinds to the caller's root.
Local vectors, writers and readers unwind through RAII before those catches.
There is no new query runner, callback abstraction, or per-operator error wrapper.
After a root failure, callers discard the emitted prefix, close independently owned
temporary handles, and use checked QueryContext::Close. That call cannot reset
scratch while any initialized executor, reader or writer remains. On normal EOF,
FinishQuery closes the root and follows the same checked reset sequence before
recording success.

The reset guard is intentionally retained: unique child ownership does not cover
multiple independent trees or standalone handles in the same query. Cache discard
still checks temporary pins, then drops dirty scratch mappings without writing,
then DiskManager checks scratch reset. A reset error leaves the database active;
new queries and database mutation remain prohibited. Destructors provide best-effort
nonthrowing cleanup, not checked success. Database::BeginQuery/CheckMutable and its
query-active flag needed no changes. No rollback/recovery guarantee is added.

TempWriter retains its two local catch-and-close blocks. A writer is also usable
outside an executor tree, so common Executor cleanup alone cannot prevent publishing
a failed prefix. TempReader/Writer retain one private staging page each; no guard
survives a tuple call. TempStore, TempRun, TempReader, TempWriter and TempScan remain.

## Memory and remaining complexity

The shared model retains checked size arithmetic, fixed scratch, row charges,
finite descriptors, bounded vectors/hash tables, and phase-specific staging:

- BNLJ: block capacity from the outer-row charge after maximum input/output scratch.
- Sort: two descriptor arrays, bounded run vector, and merge fan-in charging each
  reader page/handle plus its owned tuple head. Run capacity dies before merge.
- Grace: both partition descriptor arrays coexist with one side's writer pages;
  build capacity charges hash nodes/buckets; fallback subtracts descriptors before
  calling the ordinary BNLJ model. Exact partition RowCount still controls fit.
- Hash aggregation: bounded groups coexist with spill scratch, then die before
  sorting partials. Its internal sort now receives M minus combine scratch minus
  P+128, accounting for both the separate TempScan page and handle allowance.
  For the 32768-byte fixture this changes Msort from 26568 to 26440, with group
  capacity still 117. The optimizer calls the same corrected function.
- Sort aggregation: ordered/global combination needs only combine scratch;
  unordered input uses the existing sorter with its reduced allowance.

The 64-byte run and P+128 handle allowances remain conservative, with compile-time
size checks. No capacity is increased merely because a descriptor shrank. Each
heavyweight operator has its own allowance; simultaneously live operators may each
retain M. These are educational modeled allocations, not exact allocator usage,
process RSS, or a whole-query memory guarantee.

Essential complexity remains in per-executor initialized/EOF bits (including EOF
surviving Close), algorithm positions/phases, borrowed schema lifetime, one reset
resource count, writer failure invalidation, checked arithmetic, page identity/type
and payload bounds, forward links, real I/O errors, and physical scratch caps. These
support correctness, replay, bounded memory, cleanup and safe access rather than
hypothetical production recovery.

## Compatibility and relaxed guarantees

Supported query results, ordering, clean persistence, replay, spilling and database
mutation exclusion are preserved. Custom Executor subclasses must implement
DoInit/DoNext/DoClose rather than overriding the public entry points. Direct callers
of scans/filters/projections now also get automatic closure after execution failure;
continuing Next on the failed stream is rejected. Native allocation/length exception
types replace DbError translation. Callers handling all execution failures should
catch std::exception (and discard the prefix); no failed result is normal EOF.

Temporary scratch is trusted writer output, not an arbitrary-corruption recovery
format. A valid-looking early terminal link, extra/missing rows, incorrect descriptor
counts or tail mismatch are no longer comprehensively diagnosed. Identity, type,
owner, header and accessed-payload bounds remain checked; backward/self links throw.
Failed I/O still propagates. Exact counts are constructed on every successful run
and independently audited in tests; they are not estimates.

The removed fields/accessors are source API changes. TempMetrics counts successful
NewPage returns, so after failed frame installation it can be lower than physical
extent. The orphan still consumes max_temp_pages because that cap reads DiskManager.
Benchmark records use version 5; old reports/results remain historical. Compare
common fields across versions. No timing improvement is claimed.

## Tests and verification

Baseline configure, complete Debug build, and full regression were executed before
implementation: 13/13 suites passed in 78.33 seconds. The first implementation build
found two stale test references (slot-header constant and removed modeled-row-byte
accessor); both were fixed. The first focused regression then found two old hash
aggregate allowance expectations; both were updated for the P+128 correction.
Those initial failures are preserved in the logs.

All original suites and test scenarios remain (the orphan-allocation fixture was renamed to describe physical cap consumption). Tests changed only to adopt the
central lifecycle hooks, removed diagnostics, corrected capacity, or removed
exhaustive scratch audits. Two scratch row-count-corruption cases now exercise
retained payload offset/length bounds. Exact writer row/page counts, interleaved
runs, randomized algorithm oracles, persistence, disk spilling, small-memory
boundaries, replay, failure propagation and integration coverage remain. The orphan
allocation fixture still proves the physical cap holds after failed installation.

A new common-boundary fixture covers all four combinations of Init/Next failure
and native bad_alloc/length_error. It throws through a parent Projection with an
unfinished writer or active reader, verifies native exception propagation and
resource release, rejects FinishQuery/Next, then checks scratch reset and database
mutation without caller-side root.Close.

Executed complete Debug regression: 13/13 passed, 92.79 seconds.
Executed complete Release regression: 13/13 passed, 52.29 seconds.
Release benchmark smoke: 63/63 successful reference-validated cases, including
sort, joins, aggregation, CLOCK and B+ tree integration. A separate recursive JSON audit confirmed schema version 5 and absence of every
removed field in all 63 records.
Executed complete ASan/UBSan regression: 13/13 passed, 143.98 seconds.
All three complete build logs contain zero compiler warning/error diagnostics. Builds and benchmark work sometimes overlapped; timings
are verification durations, not comparative performance measurements.

Commands and raw evidence are in benchmark_results/v1_phase3/ (ignored by Git):

```sh
cmake -S . -B build
cmake --build build -j 4
ctest --test-dir build --output-on-failure
ctest --test-dir build -R 'execution|external_sort|nested_loop_join|grace_hash_join|aggregation|optimizer|integration' --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 4
ctest --test-dir build-release --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 4
ctest --test-dir build-sanitized --output-on-failure
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --output benchmark_results/v1_phase3/smoke.jsonl
git diff --check
```

## Code-size measurements

Physical and nonblank lines include comments. Each component includes its public
header and source. The comparison uses git show of the preserved baseline against
the working tree, so moving formulas into work_budget is counted on both sides.
Per-file membership and counts are in benchmark_results/v1_phase3/code-size.json.

| Component | Before lines | After lines | Before nonblank | After nonblank |
|---|---:|---:|---:|---:|
| Executor / QueryContext | 106 | 110 | 98 | 102 |
| Basic executors | 164 | 152 | 151 | 139 |
| Temporary relations | 253 | 226 | 242 | 215 |
| TempScan | 48 | 42 | 41 | 35 |
| Work budget | 79 | 188 | 72 | 175 |
| External sort | 296 | 261 | 276 | 241 |
| Nested loop join | 106 | 92 | 95 | 81 |
| Block nested loop join | 177 | 144 | 164 | 131 |
| Grace hash join | 322 | 268 | 302 | 248 |
| Aggregation | 407 | 354 | 387 | 334 |
| **Total** | **1958** | **1837** | **1828** | **1701** |

Production scope shrinks by 121 physical lines (6.2%) and 127 nonblank lines (6.9%).
The principal improvement is visibility: common lifecycle/error/capacity work is
out of the algorithm bodies. The common Executor file grows to own those rules;
work_budget grows because formulas move there. Database/storage/index algorithms
and optimizer implementation are unchanged.


The seven changed test files plus benchmark executable source and runner total
6333 lines before and 6377 after. These supporting changes are separate from the
production totals; no coverage reduction is used to make the production count smaller.

The final stale-reference audit covered headers, implementations, test subclasses,
optimizer callers, benchmarks/runner, README and current documentation. Removed
API names remain only in migration explanations and historical reports.
`git diff --check` passes. HEAD and the archive branch still identify the baseline;
no commit or push was performed.
