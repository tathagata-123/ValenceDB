# V1 Phase 2: storage and buffer management

## Baseline and scope

Baseline: `7e9f97f7103a55ea286eb12ddff88eb7bba22450`.
Preserved before code changes on local branch `archive/pre-v1-storage-7e9f97f`.
The working branch is `v1/algorithm-first`; the starting worktree was clean.
No repository or ancestor AGENTS.md was present. No commit or push is requested.

Before editing code, `cmake -S . -B build`, `cmake --build build -j 4`, and
`ctest --test-dir build --output-on-failure` succeeded: 13/13 suites, 90.92 seconds.
The raw test log is in `benchmark_results/v1_phase2/baseline-ctest.log` (ignored).

Inspected all eleven requested component implementations and headers, their
storage/index/execution tests, temporary-storage callers, benchmark emission and
runner validation, and current storage/buffer/I/O documentation. Persistent and
temporary file contents flow through BufferPool; HeapFile validates page ownership;
Database persists updated TableInfo and maintains indexes; executors use the same
buffer, temporary namespace, counters, and fault hooks.

## Contract changes decided before implementation

- DiskManager will propagate native filesystem exceptions instead of wrapping
  them in DbError. Explicit stream I/O and page validation failures still throw
  DbError. No error becomes success or EOF.
- Remove failed_reads/failed_writes from IoCounters and benchmark output. Keep
  successful physical read/write/allocation counters for algorithmic I/O costs.
  Version the changed benchmark record and adapt its runner.
- Read directly into the caller's page buffer: its contents are unspecified after
  a failed physical read. BufferPool retains its existing separate incoming page.
- Let fstream RAII close files on destruction; checked explicit Close remains.
- SlottedPage::Validate will check identity, owner, and header/free-space bounds
  in constant time. Accessed payloads and compaction destinations stay bounded.
  Full slot/live-count/overlap audits move to an independent test validator.
- Heap insertion and deletion will avoid duplicate validation/encoding/fetching
  already performed by their storage callee. Foreign RID rejection, stable
  tombstones, scan epochs, metadata persistence, and index maintenance remain.
- Keep page formats and all documented supported operations unchanged. Unexpected
  I/O/allocation failure may require abandoning or rebuilding the instance; V1
  promises no rollback or recovery from partial structural changes.

## Fault-hook dependency audit

No fault hook is removed in this phase. Read/Write hooks are used by
storage_primitives, buffer_pool, heap_catalog (Write), bplus_insert,
nested_loop_join (Read), external_sort, grace_hash_join, aggregation, execution,
and integration tests. ResetTemporary is used by execution and integration tests
for checked query cleanup. These tests still verify propagation, no false EOF,
and resource cleanup in components awaiting later phases. There are no production
callers of FailNextIo. Removing the two failure counters needs only assertion and
benchmark integration edits, not removal of these test scenarios.

## Removed mechanisms and reasons

| Removed mechanism | Reason / replacement |
|---|---|
| Three filesystem exception wrappers (constructor, OpenFile, ResetTemporary) | Native filesystem errors already describe the failed operation and path. They propagate without repeated translation. |
| failed_reads / failed_writes and two catch-and-rethrow blocks | Failed-operation accounting does not illustrate page I/O costs. Successful reads, writes, and allocations remain counted; failures still throw. |
| DiskManager's private read staging page and full-page copy | Read transfers directly to its supplied buffer. BufferPool already stages incoming bytes separately from its resident frame. |
| Handwritten DiskManager destructor | The two fstream members already own and close their files. Explicit checked Flush/Close remains necessary to report errors. |
| SlottedPage::Validate's all-slot traversal, range vector, sort, overlap check, live-count reconciliation, tombstone-offset audit | These are exhaustive structural audits rather than ordinary access requirements. Moved to tests/slotted_page_validator.hpp. |
| Insert's second CanFit call after compaction | The preceding live-byte capacity calculation and checked compaction establish the required contiguous space. |
| FetchHeap's invalid-ID check | DiskManager rejects unallocated/invalid page IDs; page identity/type/owner validation remains in FetchHeap. |
| Database::Insert's first EncodeTuple call | HeapFile already encodes and validates before changing pages. Encoding once preserves schema/type rejection before mutation. |
| Database::Delete's preliminary Fetch/decode, duplicate row-count check, and disappeared-row check | HeapFile::Erase checks ownership and returns the tombstone result directly in this single-threaded engine. |
| HeapFile::Erase's payload Fetch/copy | SlottedPage::Erase accesses the slot directly; deletion needs no payload allocation or decoding. Table row-count underflow remains checked. |
| Failure-counter assertions and benchmark fields | The API fields are removed. Error-propagation, EOF, cleanup, and successful-I/O assertions remain. Benchmark schema advances from 3 to 4. |
| Failed-read output-preservation assertion | DiskManager no longer promises untouched output on a partial read. A real short-read test verifies propagation and no successful-read count. |

No test function, suite, CMake target, supported database operation, or page-format
field was removed. Implementation edits outside storage are limited to necessary
counter consumers in tests and benchmark emission/version validation.

## Resulting algorithms and safety boundaries

SlottedPage::Validate checks the common header, table owner, slot count, exact
directory end, free-space boundaries, and live_count <= slot_count. These checks
are O(1), with no allocation. Fetch checks the directory and its selected live
payload before constructing an owned byte vector. It does not inspect other slots.

Insert either appends directly into contiguous free space or sums live lengths
to decide whether compaction can make room. Compact walks slots once, checks each
source range, and checks remaining destination capacity before subtraction/copy.
The latter check matters even when several individually bounded source ranges
overlap. One fixed page-sized scratch image prevents source/destination aliasing;
slot numbers and tombstones stay stable. Erase checks the slot and live-count
underflow, then marks its length zero without decoding tuple data.

HeapFile still inserts only into the tail, appends/links new pages when needed,
and checks page ownership on reads and erases. Database saves updated metadata
and maintains the insertion-only index exactly as before. HeapScan still validates
each visited page, skips tombstones, checks chain length/endpoints, releases pins
between calls, and rejects use after a mutation epoch change.

## Intentionally unchanged components and remaining complexity

| Component | Why it remains unchanged |
|---|---|
| BufferPool (header and source) | Frame array, page table, dirty bit, pin count, bounded CLOCK usage/sweep, all-pinned rejection, and dirty writeback already expose the algorithm. Existing counters demonstrate caching, CLOCK, write causes, scan-ring reuse, and bounded pin use. |
| PageGuard | Its move-only RAII ownership is the pin lifetime rule. Empty-guard checks prevent invalid access; small debug assertions enforce trusted invariants without a verification framework. |
| ScanRing | Borrowed frame identities and generations prevent stale handles from replacing a different page. The small ring and eligibility rules protect hot/pinned pages without owning extra page buffers. |
| Catalog | Its bounded directory, fixed schema encoding, metadata identity checks, and explicit Save are already short and needed for schema compatibility and clean reopen. |
| PageCodec | Explicit endian serialization and field bounds preserve the disk format and avoid unsafe pointer access. No new serialization abstraction is needed. |
| Schema | Supported types, column limits, and maximum tuple size are core compatibility and bounds rules. |
| Tuple / Value | Owned values, explicit type checks, length/null-mask checks, and complete decode consumption are necessary for valid tuple access. |
| HeapScan and storage public APIs other than the documented DiskManager counters/read-error contract | Epochs, chain endpoints/counts, and table ownership support correct scans and prevent foreign RIDs. No new scan state or lifecycle framework is introduced. |

BufferPool's incoming page staging and map insertion before frame replacement
remain: they are a small, readable sequence already covered by fault tests, not a
rollback framework. Generation tracking, query-active checks, explicit cleanup,
and pin accounting remain necessary for current callers. Disk page-address and
file-length checks, namespace separation, stream seek/read/write/flush/close
checks, and checked temporary reset are retained.

Physical processing is still disk based. The buffer uses F page frames; heap
compaction uses one scratch page; HeapScan returns one owned tuple at a time.
External execution memory limits and spilling implementations are unchanged.

## Lost functionality and relaxed guarantees

No supported storage operation or persistence format is lost. Non-indexed deletes,
insertion-only index integration, and clean close/reopen remain supported.

The removed diagnostic fields are a source API change to IoCounters and a benchmark
record change (version 4). Consumers of version 3 must compare common fields or
adapt; old evidence is retained unchanged. Filesystem failures may now be native
filesystem_error rather than DbError; both derive from std::exception. Allocation
failures continue to propagate.

After a failed DiskManager::Read, its output may contain a partial transfer. The
unchanged BufferPool still protects its resident frame by reading into its own
incoming page. There is no general retry, rollback, or recovery guarantee.

Ordinary heap and temporary-page accesses no longer reject every overlap, inaccurate live count,
invalid unused/tombstone offset, or corrupt tuple in another slot. Deletion no
longer decodes the erased payload, so it does not diagnose invalid tuple encoding.
Header/identity/owner, accessed memory bounds, and arithmetic checks remain. If
invalid metadata or I/O causes a mutating operation to fail, some bytes may already
have changed (including a tombstone before a table-count underflow is detected).
Abandon or rebuild that instance. These checks are not transactional atomicity.
TempReader shares the slotted-page implementation; its independent descriptor
row/page/tail and forward-progress checks remain unchanged.

## Test changes and coverage

- Existing deterministic CLOCK, dirty writeback, all-pinned, pin-move/RAII,
  scan-ring ownership/eligibility/generation, temporary discard, and buffer
  failure scenarios are unchanged.
- The independent slotted-page validator audits every slot, payload overlap,
  tombstone offset, and exact live count in layout, compaction, directory-limit,
  heap-chain, and new randomized tests.
- Three fixed seeds each run 600 slotted-page insert/delete/compact operations.
  Every step checks the full structure and every historical slot against an owned
  payload/tombstone oracle, retaining stable slot identity.
- Corrupt header, out-of-page payload, payload into the directory, truncated
  payload, live-count underflow, and oversized directory tests exercise direct
  access checks. A fixture with overlapping 3,000-byte ranges verifies compaction
  cannot overflow its destination. Full overlap/live-count errors are verified
  by the test validator rather than the ordinary Validate entry point.
- Existing randomized heap rows, page/directory exhaustion, tombstones, ownership,
  catalog limits, schema persistence, and database reopen remain covered. A foreign
  RID with an absent slot additionally checks ownership precedes not-found.
- A real file truncation after opening produces a short-read error, with no
  successful-read increment. A moved scratch path makes ResetTemporary propagate
  its native filesystem exception. Destruction still closes the owned streams.
- Counter-only assertions are removed from storage_primitives, bplus_insert,
  nested_loop_join, and integration. Their injected failure, no-EOF, pin-release,
  cleanup, and successful-I/O checks remain. No test scenario was deleted.

## Code-size measurements

Counts compare `git show 7e9f97f:<path>` with the working tree, including comments
and blank lines. Each component below includes its public header and source.
Machine-readable per-file measurements are in
`benchmark_results/v1_phase2/code-size.json` (ignored).

| Component | Before lines | After lines | Before nonblank | After nonblank |
|---|---:|---:|---:|---:|
| DiskManager | 261 | 231 | 230 | 201 |
| SlottedPage | 149 | 148 | 132 | 129 |
| HeapFile / HeapScan | 157 | 155 | 140 | 138 |
| Catalog | 201 | 201 | 181 | 181 |
| Database | 234 | 230 | 207 | 203 |
| PageCodec | 201 | 201 | 166 | 166 |
| BufferPool | 301 | 301 | 266 | 266 |
| PageGuard | 92 | 92 | 72 | 72 |
| ScanRing | 54 | 54 | 42 | 42 |
| Schema | 82 | 82 | 68 | 68 |
| Tuple / Value | 179 | 179 | 159 | 159 |
| **Total** | **1911** | **1874** | **1663** | **1625** |

Production scope shrinks by 37 physical lines (1.9%) and 38 nonblank lines (2.3%).
SlottedPage's major improvement is removing O(s log s) audit work from every row
access, not shortening the file: local memory-safety checks replace the removed
full audit. No timing speedup is claimed.

Supporting changes are counted separately so production simplification is not
confused with coverage removal:

| File | Before lines | After lines |
|---|---:|---:|
| tests/storage_primitives_test.cpp | 273 | 300 |
| tests/heap_catalog_test.cpp | 347 | 411 |
| tests/slotted_page_validator.hpp | 0 | 44 |
| tests/bplus_insert_test.cpp | 629 | 627 |
| tests/nested_loop_join_test.cpp | 717 | 715 |
| tests/integration_test.cpp | 460 | 459 |
| bench/valencedb_bench.cpp | 732 | 732 |
| tools/run_benchmarks.py | 309 | 309 |

## Verification

All commands below were actually executed; logs and benchmark JSONL are saved
under `benchmark_results/v1_phase2/` (ignored by Git).

- Baseline complete Debug build and regression: 13/13 suites, 90.92 seconds.
- Focused storage_primitives/buffer_pool/heap_catalog: 3/3 suites, 4.82 seconds.
- Final complete Debug build and regression: 13/13 suites, 90.72 seconds.
- Final complete Release build and regression: 13/13 suites, 53.23 seconds.
- Final complete ASan/UBSan build and regression: 13/13 suites, 138.52 seconds.
- Release smoke: 63/63 successful reference-validated cases covering CLOCK,
  B+ point/range/insertion, sort, joins, and aggregation. Every JSONL record uses
  schema version 4 and none contains the removed failure counters. The runner
  completed its counter, resource-cleanup, plan, and result-equivalence checks.
- All three complete build logs have zero compiler warning/error diagnostics.

Executed command forms (output redirected to the logs above):

```sh
cmake -S . -B build
cmake --build build -j 4
ctest --test-dir build --output-on-failure
ctest --test-dir build -R 'storage_primitives|buffer_pool|heap_catalog' --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 4
ctest --test-dir build-release --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 4
ctest --test-dir build-sanitized --output-on-failure
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --output benchmark_results/v1_phase2/smoke.jsonl
git diff --check
```

Full-suite execution covers buffer replacement/writeback/pins/rings, heap insertion
and non-indexed deletion, compaction, catalog persistence, tuple encoding, reopen,
index integration, and all bounded-memory execution algorithms. Tests in different
build configurations ran sequentially because some fixtures share temporary file
names. Builds and benchmark work sometimes overlapped, so these timings are
verification durations, not comparative performance measurements.

The stale-reference audit covers headers, implementations, temporary callers,
tests, benchmarks, runner, README, and current documentation. Removed counter
names remain only in explanations of the change; old refactor reports remain
historical. Storage, execution, architecture, joins, and I/O documentation now
describe the applicable validation/error boundaries. `git diff --check` is clean.
HEAD and the archive branch both still identify the baseline commit; no commit
or push was performed.
