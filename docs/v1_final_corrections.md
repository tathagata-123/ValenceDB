# ValenceDB V1 final corrections

The correction pass started on `v1/algorithm-first` at `c7ff156ec95f352bd0e6f1cbe46f851a8dff6636` with a clean working tree. The original baseline, `8fe8c9b52212a206447474dc1dfa1c954915dfba`, is an ancestor. The complete starting Debug suite passed before the first edit.

## Findings and regression evidence

| Finding | Implementation and reason | Focused regression |
| --- | --- | --- |
| A1 · Stale catalog directory | `Catalog::CreateTable` validates directory page 1 and compares its persisted count at byte 16 with the handle's cached table count before allocating the metadata page. A mismatch rejects the append. On success it drops the directory guard, creates and releases the metadata page, then fetches the directory again to publish the entry. | `StaleCatalogCreation` opens two handles on one frame. After the first creates and populates `alpha`, the second fails to create `bravo` with the same persistent page count. The current handle creates `charlie`. Checked reopen finds exactly those two tables and fetches the original row by RID. Peak pins during creation were one. |
| A2 · Immutable schema | `Catalog::Save` matches the table name and metadata page ID, then compares schema kind, column count, and every column's name, type, VARCHAR limit, and nullability in order. Only validated creation calls `EncodeMeta` to write the schema descriptors. This closes the path through which a replacement name could reach the fixed-width encoder. | `ImmutableCatalogSchema` rejects a valid column rename, a different type, VARCHAR limit, column count or schema kind, a nullable replacement, and changes to either identity field. Each rejected save leaves the entire page and cached metadata intact. A 23-byte name, two live rows at their original RIDs, a tombstone, and a published index survive checked reopen on one frame. |
| A3 · Global COUNT order | A global result always has one row. When COUNT is present, output column 0 is a NONNULL INT64 and already satisfies ascending order. `Aggregates` and both executors advertise that property. A grouped hash result is unordered and a grouped sort result orders by its group key. | `GlobalCountOrdering` checks COUNT of zero and four at `QueryResources{1500, 4096}`. The chosen plan has no ExternalSort. Its executor and a directly constructed global SortAggregate both replay after Close → Init with zero scratch pages and zero active pins. COUNT plus SUM returns `(0, NULL)` or `(4, 6)`. Nullable SUM/MIN/MAX and DOUBLE AVG remain invalid ORDER BY keys. |
| A4 · Fractional cardinality | `EstimateNDV` returns zero for nonpositive rows, returns the row estimate itself between zero and one, and otherwise uses the existing clamp. Known-range equality uses `1 / max(1, NDV)` inside the range and zero outside. This gives the clamp valid bounds and keeps selectivity at most one. | `FractionalCardinality` checks rows 0, 0.25, 1, and 100 with sampled statistics and without statistics. At 0.25 rows both NDVs are 0.25. In-range equality is 1 with the sample and 0.1 with the fallback. The test checks values below and above the sample range, numerical expectations, both bounds, and a negative row estimate. |
| B1 · Equijoin key NDVs | `Joins` caps all output NDVs by estimated rows, then assigns the smaller capped NDV to both key positions. The right position is relative to the projected left schema's width. Equality requires both output keys to have the same distinct count. The minimum estimates the size of their intersection. | `EquijoinKeyNDV` forces GHJ at 32,768 bytes on 1,000 zeros joined to 0–999. Either group key and either input orientation produce NDVs `[1, 1]`, one estimated group, HashAggregate, and exactly `(0, 1000)`. The raw join emits 1,000 copies of `(0, 0)`. Projected keys at positions 1 and 3 yield `[12, 1, 3, 1]`. Empty inputs on either side yield zero join rows, groups, and NDVs. |

### Persistent fields and memory boundaries

Previously `Save` called `EncodeMeta(guard.MutableData(), info)` and assigned `table = info`. It now calls `WriteMutableMeta(guard.MutableData(), info)` and copies six scalar fields into the cached entry. The writer uses the existing offsets.

| Field | First heap page | Last heap page | Live rows | Heap pages | Indexed column | Index metadata page |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Byte offset | 16 | 20 | 24 | 32 | 36 | 40 |

The column count at byte 38 and descriptors starting at byte 64 belong to creation. The storage test marks an ignored descriptor padding byte and checks that it survives real inserts, deletion, and index publication. This detects a full-page rewrite even if all meaningful schema fields would encode identically.

The existing `DatabaseRoundTrip` test also checks both ends of a multi-page heap and reads the 120 surviving rows at their original RIDs after 140 inserts and 20 deletions. The index integration suite reopens the persisted tree and compares heap and index contents.

The one-column COUNT fixture needs 1,384 combine bytes. Adding SUM makes the output row charge 168 bytes and the required allowance `3 × 168 + 1024 = 1528` bytes. The regression checks that the mixed aggregate rejects the 1,500-byte default, then runs it with the existing aggregate-only override of 1,528 bytes. The query default and sort allowance stay at 1,500 bytes in both cases.

## Files and functions

| File | Changed code or documentation |
| --- | --- |
| `src/storage/catalog.cpp` | `Catalog::CreateTable`, `Catalog::Save`, `Catalog::EncodeMeta`, and file-local `SameSchema` and `WriteMutableMeta`. |
| `include/valencedb/storage/catalog.hpp` | Marks the mutable fields in `TableInfo`. The public `Save(const TableInfo&)` interface is preserved. |
| `src/optimizer/optimizer.cpp` | `Aggregates` sets output ordering. `Joins` synchronizes the equijoin keys after the existing NDV cap. |
| `include/valencedb/execution/aggregate.hpp` | `HashAggregate::OutputOrdering` and `SortAggregate::OutputOrdering`. |
| `src/optimizer/statistics.cpp`, `include/valencedb/optimizer/statistics.hpp` | `EstimateNDV` and `EstimateSelectivity`, with the fractional-cardinality convention beside their declarations. |
| `tests/heap_catalog_test.cpp` | Adds `StaleCatalogCreation` and `ImmutableCatalogSchema` to `main`. |
| `tests/optimizer_test.cpp` | Adds `FractionalCardinality`, `GlobalCountOrdering`, and `EquijoinKeyNDV` to `main`. Extends `GlobalAndValidation` for invalid aggregate keys and `AggregateChoices` for grouped planner/executor ordering agreement. |
| `tests/aggregation_test.cpp` | Updates `Run` to verify ordering across the global and grouped hash/sort result-oracle cases. |
| `docs/bplus_tree.md` | Removes `CheckReserved` from the function guide and identifies the production RID and metadata checks. |
| `docs/joins.md` | Distinguishes per-pair working state released by `ReleasePair` from the partition descriptor vectors released by `Close`. |
| `docs/aggregation.md` | Describes the final flush, map destruction, writer lifetime, and sorter construction in execution order. Documents global COUNT ordering. |
| `docs/optimizer.md` | Documents `EstimateSort` returning `ModeledCost` with local run/level calculations, O(p) predicate evaluation, and O(p²) filter-chain copying. Updates the cardinality and ordering rules and the worked plan's estimate of four groups. |
| `docs/v1_final_corrections.md` | Records this pass and its verification. Earlier phase reports retain their historical measurements. |

## Production size

The same 58 tracked `.cpp`, `.hpp`, and `.h` files under `src/` and `include/` were measured from the starting Git objects and the corrected working files. Physical lines use `splitlines()` and include declarations, comments, and blank lines. Nonblank counts exclude whitespace-only lines.

| Measure | Starting commit | Corrected tree | Difference |
| --- | ---: | ---: | ---: |
| Physical lines | 4,944 | 4,977 | +33 |
| Nonblank lines | 4,446 | 4,475 | +29 |
| File bytes | 193,571 | 195,270 | +1,699 |

## Executed verification

The builds used CMake 4.3.1 and AppleClang 21.0.0 on Darwin 25.5.0 arm64. Before the fixes, the new checks observed stale creation succeeding, a fractional NDV outside its allowed interval, inconsistent join-key estimates, a COUNT query rejected for lack of a feasible plan, and incorrect executor ordering. With A1 applied in isolation, the valid column rename still succeeded. The final runs include all corrections together.

| Run | Result | Wall time |
| --- | --- | ---: |
| Starting Debug suite | 13/13 | 31.45 s |
| Focused Debug heap/catalog, index integration, optimizer, aggregation | 4/4 | 9.72 s |
| Complete Debug suite | 13/13 | 32.25 s |
| Complete Release suite | 13/13 | 27.06 s |
| Complete ASan/UBSan suite | 13/13 | 51.54 s |

The full suites include 17 optimizer groups, 15 aggregation groups, and five index integration groups. The execution and operator suites exercise ordering, early close, replay, scratch cleanup, and physical I/O errors. The new equijoin fixture retains the 1,000-row join estimate and the 1,000,000-row Cartesian upper bound.

### Sanitizer coverage

The final sanitized build used `-fsanitize=address,undefined` for compilation and linking. Both sanitizers were set to halt on errors. The complete CTest log contained no AddressSanitizer or UBSan diagnostics.

AppleClang's runtime aborted the smoke executable at startup with `detect_leaks is not supported on this platform` when invoked with `ASAN_OPTIONS=detect_leaks=1`. Its compiler also rejected `-fsanitize=leak` for arm64 Darwin. A separate Homebrew GCC 16 address-sanitized empty program stalled at LeakSanitizer's exit scan, including outside the sandbox, and was stopped. Its standalone `-fsanitize=leak` control reported no leak for a deliberately lost allocation. LeakSanitizer coverage was unavailable on this host.

### Release smoke matrix

All **63/63** cases passed with one repetition and seed 130013. The matrix covered three CLOCK cases, four point lookups, eight range scans, one indexed insertion, six sorts, 20 joins, and 21 aggregates. The runner validated canonical reference results for 60 cases and exact page access schedules for the three CLOCK cases. It checked result digests across physical alternatives and compared ordered digests for sorts and sort-based grouped aggregates.

A separate read of the saved JSONL confirmed 63 unique successful cases in 21 equivalence groups. Every record ended with zero active pins, zero temporary pins, zero cached temporary pages, and zero scratch extent. The 480-row external sort allocated 97 temporary pages. The spilling hash aggregate returned the same 350 groups as sort aggregation, using 64 and 32 temporary pages respectively. The raw records are in the ignored local file `benchmark_results/v1_final_corrections/smoke.jsonl`.

### Commands

The starting Debug baseline used the clean commit.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 4
ctest --test-dir build --output-on-failure -j 4
```

The new regression checks were first run against the starting implementation. The last two commands below isolated the schema failure after the stale-directory correction.

```sh
cmake --build build -j 4 --target valencedb_heap_catalog_tests valencedb_optimizer_tests valencedb_aggregation_tests
build/valencedb_heap_catalog_tests
build/valencedb_optimizer_tests fractional_cardinality
build/valencedb_optimizer_tests equijoin_key_ndv
build/valencedb_optimizer_tests global_count_ordering
build/valencedb_aggregation_tests semantics
cmake --build build -j 4 --target valencedb_heap_catalog_tests
build/valencedb_heap_catalog_tests
```

The corrected tree was built and verified with these commands.

```sh
cmake --build build -j 4
ctest --test-dir build --output-on-failure -j 4 -R '^valencedb_(heap_catalog|index_integration|optimizer|aggregation)$'
ctest --test-dir build --output-on-failure -j 4
build/valencedb_optimizer_tests pipeline_explain

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 3
ctest --test-dir build-release --output-on-failure -j 4

cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 3
env ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir build-sanitized --output-on-failure -j 4

python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --seed 130013 --output benchmark_results/v1_final_corrections/smoke.jsonl

git diff --check
git diff --stat
git status
```

## Finalization

All five audit findings are resolved. V1 retains its on-disk format, one-frame storage operations, per-operator memory allowances, insertion-only B+ tree, disk-based sort and spill paths, and existing scan, join, and aggregation algorithms. The diff review covers six production files, three test files, four current guides, and this new report. The whitespace check passed. All changes are uncommitted on the starting branch.

**ValenceDB V1 is ready to be finalized for the stated educational scope.** LeakSanitizer is the sole outstanding verification limitation.
