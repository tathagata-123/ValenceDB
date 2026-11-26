# V1 Phase 1: algorithm-first B+ tree

## Baseline and scope

Baseline: `8fe8c9b52212a206447474dc1dfa1c954915dfba` (Phase 4: Simplify query optimizer).
Preserved before editing on local branch `archive/pre-v1-bplus-8fe8c9b`.
The working branch is `v1/algorithm-first`; no commit or push is part of this task.
The starting worktree was clean. No repository AGENTS.md was present.

Before code changes, `cmake --build build -j 4` succeeded and
`ctest --test-dir build --output-on-failure` passed all 13 suites (79.66 seconds).
The baseline CTest log is saved locally under
`benchmark_results/v1_phase1/baseline-ctest.log`.

Inspected the three index implementations and headers, Database index ownership
and mutation, IndexScan, optimizer access selection/costing, raw-page validator,
insertion/deletion and integration tests, both benchmark paths, and documentation.
IndexScan uses the existing range cursor; the optimizer uses index metadata,
height, and ordering. Neither needs an implementation change.

## Contract changes decided before implementation

- The tree becomes insertion-only. Remove exact erase, sibling redistribution,
  separator rotations, merging, underflow propagation, root contraction, and
  tree-specific retired-page bookkeeping. No lazy deletion replaces them.
- Database::Delete throws for every indexed table, including absent/invalid RIDs,
  immediately after table resolution and before heap access or mutation. Deletion
  of non-indexed rows retains its existing behavior.
- Allocate each right page when its node splits, and a new root only when the
  split reaches above the old root. Remove cascade prediction and reservation.
  Allocation/I/O failure may leave partial mutations and unreachable pages;
  abandon or rebuild the affected instance. No rollback/retry guarantee remains.
- Production page checks retain identity, type, capacity, and bounds. Full node
  ordering, cross-page structure, and the full metadata reserved-byte scan move
  to the independent test validator. Individual accessed fields remain checked.
- Preserve the page format, composite ordering, binary search, bounded node
  vectors, configurable capacities, PageGuard/BufferPool integration, streaming
  cursors, cursor epochs, and clean close/reopen persistence.

## Removed mechanisms and reasons

| Removed mechanism | Reason / replacement |
|---|---|
| Public `Erase` and its exact-pair removal path | Deletion is outside the V1 index contract. Database rejects indexed deletion before heap access. |
| `RepairLeaf` | Left/right redistribution and merges are deletion-only algorithms. |
| `RepairInternal` | Separator rotations, internal borrowing, and internal merging serve deletion underflow. |
| `InternalNode::RemoveRightChild` | Parent child/separator removal is needed only by merges. |
| Underflow propagation and root contraction | Insertion grows nodes and height; it never creates an underfull node. |
| Retired-page vector, tree-specific `Invalidate` calls, validator retired-ID argument | Insertion retires no pages. General BufferPool invalidation is unchanged. |
| Cascade prediction, `needed`/`grows_root`, reserved-page vector, `used` counter, reservation assertions | Allocate a right page within each actual split and a root only when propagation exhausts the ancestor path. |
| `Ancestor::child_count` and related assertions | Only page ID and chosen child index are needed to insert a promoted separator. |
| Separate inline leaf decoder in `Insert` | `ReadLeaf` owns the one bounded decode; insertion uses `std::lower_bound`. |
| `Start`'s leaf-advance loop | `Next` already handles an end-of-leaf offset. |
| `CheckOrdered` and full leaf/internal validation loops | Algorithms maintain sorted keys; raw-page tests audit ordering and all child references. Lookup remains binary search with constant-size header validation. |
| Full IndexMeta reserved-area scan on read | Unused tail bytes need no runtime audit. The independent validator retains it. |
| `SetRoot`'s full metadata re-decode | Check page identity/type, then encode the new metadata using the cached key column. |
| Database's index erase/key-extraction/missing-pair delete path | Indexed deletes are rejected before heap access. Non-indexed heap deletion is unchanged. |
| Deletion-only test fixtures, contraction counters, erase benchmark phase | They exercise removed operations; useful index lifecycle/oracle checks were retained and adapted. |
| Split-reservation failure atomicity/retry assertions | Partial structural mutation after allocation/I/O failure is permitted in V1. Error propagation and pin-release checks remain. |

No exception translation, general storage failure hooks, shared buffer counters,
PageGuard ownership, or unrelated executor/optimizer implementation was changed.
No lazy-deletion markers or alternate deletion path were added.

## Resulting algorithm and function map

Insertion descends once, retaining unpinned ancestor IDs and child indices. It
binary-searches a bounded leaf vector, rejects an exact duplicate, inserts the
pair, and writes a fitting leaf. An overflowing leaf is split at its midpoint;
the first right key is copied upward and the leaf chain is updated. Each parent
receives one separator/right child. An overflowing internal node moves its
boundary separator upward, leaving that separator out of both resulting nodes.
An empty ancestor path creates a new two-child root and persists its ID/height.

The [function-by-function guide](bplus_tree.md#function-guide) covers every tree
operation, page helper group, cursor operation, and small codec/check helper.
The [worked leaf and internal examples](bplus_tree.md#insertion-and-splits) show
the different separator rules. `bplus_iterator.cpp` itself is unchanged: its
existing epoch and guard ownership already provide the needed streaming contract.

## Test changes and retained coverage

- `bplus_insert_test.cpp` retains empty/single-leaf, worked leaf/internal splits,
  middle-leaf links, production capacity boundaries, ascending/descending
  cascades, root growth, composite duplicates, exact duplicate no-ops, signed
  extrema, inclusive/reversed ranges, cursor invalidation, and reopen tests.
- Randomized insertion/oracle coverage now uses four seeds and 4,000 iterations
  each, plus explicit duplicate attempts, with `{4,4}`, `{5,5}`, and minimum
  `{2,3}` capacities. It incorporates the useful minimum-capacity/randomized
  coverage from the removed mixed-deletion test. Creation uses one buffer frame;
  reopen uses two and then one. Multiple page evictions and zero retained pins
  are asserted.
- Full ordering and structural checks stay in the independent raw-page validator:
  occupancy, root exceptions, separator bounds, uniform depth, unique reachability,
  allocated page IDs, leaf chain, and metadata. Negative fixtures cover malformed
  fences, duplicate internal separators, repeated/cyclic children, broken/cyclic
  leaf links, underfull leaves, duplicate entries, types, metadata, and padding.
  Individual page-access bounds and identity tests remain in production tests.
- The old deletion suite becomes `index_integration_test.cpp`. It preserves index
  creation before/after population, prior heap tombstones, empty and populated
  reopen, duplicate/extreme keys, tuple/schema rejection, index metadata validation,
  build-publication checks, and a 3,000-operation randomized heap/index oracle.
  Delete operations now assert rejection and unchanged contents/cursors.
- The explicit indexed-delete boundary test snapshots every persistent page,
  compares heap/index contents and row counts, and verifies heap/index cursors
  survive rejection. Invalid/absent/foreign RIDs and an empty indexed table are
  rejected too. Non-indexed delete and its persistence remain tested here and in
  the unchanged heap/catalog suite.
- The full query pipeline deletes rows before creating indexes, retaining the
  same tombstones, indexed inserts, reopen, joins, aggregation, sorting, and
  optimizer comparisons. Execution and optimizer implementation/tests otherwise
  remain unchanged.
- The existing storage fault hook checks that cursor read errors throw rather than
  signal EOF and insertion write errors propagate without pins. It no longer
  asserts unchanged bytes, allocation counts, valid cursors, or successful retry
  after a failed structural mutation.
- The standalone B+ probe retains insert/contains/range phases and raw-page oracle
  validation. The main query benchmark harness required no implementation change.

## Lost functionality and relaxed guarantees

There is no B+ erase, shrinking, redistribution, merge, or root contraction.
An indexed table cannot be deleted from through Database, even when the requested
RID is absent; this is a deliberate API change from false/success to DbError.
Non-indexed table deletion continues unchanged.

There is no pre-mutation guarantee for a split-page allocation failure. An earlier
split may already have changed pages when a later allocation/write fails; the
whole affected instance must be abandoned or rebuilt. There is no claim of safe
retry, rollback, atomicity, crash recovery, or page reclamation. Errors still
propagate, and guards release resources. Runtime Validate no longer diagnoses
all ordering corruption or unused metadata-tail corruption; tests audit them.
Low-level page Write callers must supply sorted entries.

The physical page format, capacities, duplicate semantics, lookup/range APIs,
clean persistence, small-pool support, and successful-insertion cursor epoch
behavior are preserved. Stable lower fences remain legal on reopen. Existing
append-only allocation was not converted to a free-page system.

## Remaining complexity and tradeoffs

- Composite `(key,RID)` ordering is necessary to route duplicate keys across
  leaves and reject only exact duplicates.
- Separate leaf/internal splits are necessary because leaves copy a key upward,
  while internal nodes move a separator upward. Explicit vectors/spans make
  those boundaries visible without introducing a framework.
- The O(height) ancestor path avoids retaining pins or persistent parent pointers.
  One bounded leaf vector and one bounded parent representation permit a
  one-frame pool, including during cascading splits.
- Page codecs and local identity/type/count/bounds checks preserve the disk
  format and prevent invalid access. Actual I/O errors remain storage errors.
- The mutation epoch prevents a cursor from reusing offsets after a split or
  insertion; removing it would make cursor correctness harder to express.
- Root metadata and stable Database-owned handles are needed for reopen and
  cursor lifetime. RAII remains essential for releasing page pins.
- Insertion now reads an owned leaf and writes through `WriteLeaf`, adding one
  cache fetch for a fitting leaf compared with writing through the decode guard.
  This keeps algorithm and page ownership separate. `Contains` and range
  initialization still binary-search guarded bytes without decoding node vectors.
  No runtime full-node ordering pass remains on either path or each cursor step.
- General heap/catalog validation, buffer fault hooks, query lifecycle checks,
  and executor/optimizer complexity are outside this phase and remain unchanged.

## Code-size measurements

Counts are physical source lines, including comments and blank lines, compared
with `git show 8fe8c9b:<path>`. Nonblank counts are provided separately; these are
not token, binary-size, or algorithm-complexity measurements.

| File | Before lines | After lines | Before nonblank | After nonblank |
|---|---:|---:|---:|---:|
| `src/index/bplus_tree.cpp` | 423 | 224 | 394 | 203 |
| `src/index/bplus_page.cpp` | 184 | 159 | 158 | 135 |
| `src/index/bplus_iterator.cpp` | 37 | 37 | 33 | 33 |
| `include/valencedb/index/bplus_tree.hpp` | 70 | 71 | 61 | 62 |
| `include/valencedb/index/bplus_page.hpp` | 65 | 66 | 55 | 56 |
| `include/valencedb/index/bplus_iterator.hpp` | 27 | 27 | 20 | 20 |
| **Index total** | **806** | **584** | **721** | **509** |
| `src/storage/database.cpp` | 177 | 176 | 157 | 156 |
| `include/valencedb/storage/database.hpp` | 58 | 58 | 51 | 51 |

The main tree implementation shrank by 199 lines (47.0%). The six index files
shrank by 222 lines (27.5%); nonblank lines fell by 212 (29.4%). The tree header
adds explicit split result/helper declarations while removing deletion declarations.
Machine-readable measurements are saved locally in
`benchmark_results/v1_phase1/code-size.json`.

The supporting test/probe line counts are shown separately so deletion-only
coverage removal is not confused with production simplification:

| Test/probe | Before | After |
|---|---:|---:|
| `bplus_insert_test.cpp` | 612 | 629 |
| `bplus_delete_test.cpp` → `index_integration_test.cpp` | 821 | 413 |
| `bplus_validator.hpp` | 146 | 143 |
| `bplus_tree_probe.cpp` | 75 | 73 |

## Final verification

All commands below were executed locally. Generated build/test/benchmark logs
are in `benchmark_results/v1_phase1/`, which is ignored by Git. The full raw-page
and randomized output is also in each build's `Testing/Temporary/LastTest.log`.

- Baseline: complete Debug build; 13/13 CTest suites passed (79.66 seconds).
- Initial focused Debug run: 6/6 suites passed (64.27 seconds).
- Final complete Debug build and regression: 13/13 suites passed (101.70 seconds).
- Final insertion suite: 10 groups, maximum height 10, peak pins 1, final pins 0.
  Index integration: 5 groups, peak pins 1, final pins 0.
- B+ query benchmark smoke: all 13 point/range/insertion runs succeeded, including
  SeqScan/IndexScan equivalence, duplicate keys, and post-insertion validation.
- Standalone Release probe: all insert/contains/range phases and independent
  structure checks passed at `(frames, rows, leaf capacity)` values
  `(1,8000,254)`, `(16,8000,254)`, and `(1,350,4)`, each with peak pins 1.

- Final complete Release build and regression: 13/13 suites passed (56.89 seconds).
- Final complete ASan/UBSan build and regression: 13/13 suites passed (205.32 seconds).
- All three build logs contain no compiler warnings or errors.

Executed command forms:

```sh
cmake --build build -j 4
ctest --test-dir build --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 4
ctest --test-dir build-sanitized --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 4
ctest --test-dir build-release --output-on-failure
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --family btree_point --family btree_range --family btree_insert \
  --output benchmark_results/v1_phase1/btree-smoke.jsonl
c++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Iinclude -Itests \
  bench/bplus_tree_probe.cpp build-release/libvalencedb.a -o build-release/valencedb_bplus_probe
build-release/valencedb_bplus_probe 1 8000 254
build-release/valencedb_bplus_probe 16 8000 254
build-release/valencedb_bplus_probe 1 350 4
```

The timing figures describe these verification runs, not performance comparisons:
the randomized workload grew and builds/tests sometimes ran concurrently. No
before/after speedup claim is made. Broader non-index benchmark families were
not rerun; their algorithms were unchanged and remain covered by full CTest.

A final stale-reference audit covers headers, implementation, Database callers,
IndexScan, optimizer, tests, CMake targets, benchmark sources, README, and current
documentation. Old deletion/reservation descriptions remain only as explicitly
historical material in the prior refactor reports, or as descriptions of what
V1 removed. `git diff --check` reports no whitespace errors. No commit or push
was performed.
