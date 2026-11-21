# Phase 4: optimizer and physical planning

Branch: `refactor/phase3-btree-buffer`. The initial ten changed/added/deleted files
matched the Phase 3 report and were inspected and checkpointed as local commit
`1c7fe3c`. Phase 1 (`092b80b`) and Phase 2 (`21c452a`) remain intact. No unrelated
work was found. Phase 4 remains uncommitted for review; nothing was pushed or
merged. This is an implementation report, not the separate independent audit.

## A. Files changed

Modified:

- `include/valencedb/optimizer/optimizer.hpp`
- `src/optimizer/optimizer.cpp`
- `tests/optimizer_test.cpp`
- `bench/valencedb_bench.cpp`, `tools/run_benchmarks.py`
- `README.md`
- `docs/optimizer.md`, `docs/io_cost_models.md`, `docs/architecture.md`, `docs/benchmarks.md`

Added: `docs/phase4_refactor.md`. No Phase 4 files deleted. Statistics, storage,
B+ tree, buffer, and execution sources/headers are unchanged. Generated evidence
is retained under the existing ignored `benchmark_results/` directory.

## B. Structural simplifications

PhysicalPlan no longer caches `BlockNestedLoopJoinMemory`, `GraceHashJoinMemory`,
`ExternalSortMemory`, or `AggregateMemory`. One optional resolved `memory_bytes`
replaces all four. The shared Compute helpers remain in their operator modules;
capacity, reserve, descriptor, partition, fallback, and combine layouts are local
values during costing, formatting, or executor construction.

Removed fields: `details`, `confidence`, `pages`, `input_rows`, `temp_pages`, and
`inner_executions`. Candidate creation no longer formats prose or propagates
cumulative scratch diagnostics. Packed pages, input rows, and replay counts are
derived when needed. A small statistics-source enum remains on scans to label
fresh/stale/missing samples without consulting mutable database state later.
Requested options are retained once on OptimizedPlan for forcing information.
The eliminated ORDER BY position is also retained once: a final projection can
remove the ordering key, so that decision cannot always be recovered from the
result schema/order alone.

SortAggregate's synthetic ExternalSort child is gone, as are temporary SeqScan
and ExternalSort plan objects formerly used to cost hash partials. Build no longer
skips a grandchild. No execution ownership changes were needed: the Phase 2
SortAggregate still owns one input pointer and wraps it exactly once when needed.

## C. Before-and-after architecture

Before, each candidate mixed configuration, estimates, cached capacity layouts,
diagnostic accumulators, narrative strings, and real/synthetic children. After,
one tagged value retains configuration, schema/order, cardinality/exactness/upper
bound, NDVs, local/subtree costs, actual children, and the resolved allowance.
`NeedsInternalSort()` derives the aggregate property from its group key and the
actual input ordering, avoiding a second stored flag that could disagree.

`EstimateSort` takes schema, estimated rows, conservative bound, exactness,
resources, and bytes. It returns local page/CPU cost, initial runs, and merge
levels; it creates no plan or executor. Explicit final sorting, unordered
SortAggregate, and HashAggregate partial sorting use the same function.

Scans → joins/orientations → aggregates → required sort/projection → cheapest
feasible candidate remains visible in ChoosePlan and its concrete helpers.
Only the winner reaches Build. Build recursively builds actual children and
switches over PlanKind, passing the node's resolved allowance unchanged.
ExplainNode/ExplainSort format selected facts and locally derived capacities.

## D. Algorithm preservation and four-phase integration review

The cost constant `pages + 0.0001*cpu`, scan/index formulas, conjunctive selectivity,
join NDV/cardinality estimate, inner-path replay multiplicity, candidate order,
relative tie threshold, and forced-choice rejection rules are preserved.

Sort page work remains `(2L+2)*N` for multiple runs, including initial writes,
intermediate read/write passes, and streamed final reads. Private runs cost no
temporary page transfers. Sorting CPU remains `n*log2(max(2,n))`. SortAggregate
local work is grouping CPU plus internal sorting when unordered; subtree work
adds the real input once. Ordered input pays grouping CPU only. Hash spill work
still charges partial writes and TempScan reads, partial sorting, and combination,
with at most one modeled partial per raw row. It does not scan the original
input twice. Partial cardinality remains uncertain even for exact raw input.

Conservative upper bounds still control possible multi-run fan-in and possible
hash-spill feasibility, even when sampled NDV predicts fit or zero rows. Exact
initial-run descriptor overflow rejects during planning; uncertain overflow is
still runtime-checked. Every runtime predicate survives. Deterministic 1024-value
reservoir sampling, singleton-based NDV, signed endpoint arithmetic, staleness,
reanalysis, and reopen behavior are unchanged.

Integration review followed DiskManager/BufferPool, database/heap/catalog/index,
TempStore/TempRun, external sort, NLJ/BNLJ/GHJ, and both aggregates:

- The B+ tree still uses the Phase 3 decoder/repair structure, with raw-page
  invariant tests unchanged. CLOCK still advances BufferPool's hand directly.
- Persistent and temporary pages still share the pool. DiskManager remains the
  physical transfer boundary; no disk-backed algorithm became an in-memory substitute.
- Sealed runs still borrow stable schemas/stores. Staged readers/writers release
  guards before child calls. Checked query cleanup closes handles, discards
  temporary mappings, then truncates scratch; runtime cumulative caps remain.
- ExternalSort retains bounded run/descriptor arrays, multi-pass merging, and
  streaming final output. GHJ retains Phase 2 descriptor accounting, bounded
  build/probe, defensive build abandonment, and BNLJ skew fallback.
- HashAggregate destroys build allocations before partial sorting and combines
  raw sum/count states. SortAggregate retains one owning input and replay/cleanup.
- Per-operator budgets, reduced aggregate sorts, and physical join orientation
  use the same shared formulas as execution. No new interface or ownership layer
  was introduced. Stale references to GHJ per-pair records and completed-page
  counters were corrected to the surviving summary/inner-scan metrics.

## E. Verification

Commands ran from the repository root. Configure/build/test logs are retained in
`benchmark_results/phase4_logs/`.

```sh
# Phase 3 baseline, before optimizer edits:
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure

# After sort/tree changes, then after memory/EXPLAIN changes (each followed a build):
ctest --test-dir build --output-on-failure \
  -R 'valencedb_(optimizer|integration|aggregation|external_sort|grace_hash_join|execution)$'

# After new plan-structure assertions and schema update:
cmake --build build -j2
ctest --test-dir build --output-on-failure -R 'valencedb_(optimizer|integration)$'

# Final Debug:
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure

# Final ASan/UBSan:
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j2
ctest --test-dir build-sanitized --output-on-failure

# Release and comparison:
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 \
  --output benchmark_results/phase4_smoke.jsonl
python3 benchmark_results/phase4_logs/compare.py
shasum -a 256 -c benchmark_results/phase4_logs/phase3-artifacts.sha256
git diff --check
git status --short
```

- Phase 3 baseline: **13/13 Debug suites**, 79.04 seconds.
- Sort/tree focused checks: **6/6 suites**, 67.53 seconds.
- Memory/EXPLAIN focused checks: **6/6 suites**, 65.71 seconds.
- Final optimizer/integration focused checks: **2/2 suites**.
- Final Debug: **13/13 suites**, 91.47 seconds (other configurations built concurrently).
- Final ASan/UBSan: **13/13 suites**, 189.59 seconds, no sanitizer diagnostics.
- Release smoke: **63/63 reference-validated cases**, zero failures.
- Alternating Phase 3/4 timing checks: **42/42 reference-validated runs**,
  seven cases × three repeats × two versions; paired resources/results match.
- Configure/build logs contain no compiler warnings/errors. Phase 3 checksums
  match. Complete Phase 4 diff reviewed; `git diff --check` passed.

Optimizer now has 13 test groups (one added); integration retains all five.
Existing scan, forced-alternative, orientation, replay-cost, spill, tiny-memory,
stale/zero-estimate, signed-extreme, reference-result and deterministic-tie checks
remain. Assertions against removed caches were replaced by executor-budget and
cost assertions, not discarded. The new group independently checks sort page/CPU
formulas, a genuine final sort versus aggregate local sort work, reduced override
budgets, actual input nodes, unchanged planning I/O, zero initialization/temporary
allocation during planning, two executions of a multi-pass internal sort, run
counts, and exactly one sort's writer count. Ordered input runs with only 3000
bytes, below any sorter allowance, through Filter/Projection. Final projection
drops the key while EXPLAIN still accurately reports ORDER BY elimination.

## F. Performance and resource comparison

All 63 smoke cases have identical result counts, diagnostic/ordered digests,
physical file I/O, BufferPool counters, scratch metrics, algorithm fields,
cleanup state, and reference validation. Actual selected operator trees match
after normalizing the removed internal-sort display node. Published total cost
scores match in every query. No selection, memory feasibility, algorithm path,
or physical I/O change was observed.

Representative unchanged counters:

| Case | DB reads | Temp reads | Temp writes | Temp allocations | Algorithm evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| sort_m_24576 | 19 | 96 | 96 | 97 | 21 initial runs, 4 intermediate passes, streamed fan-in 2 |
| join_balanced_ghj_auto | 4 | 0 | 0 | 10 | 5 hash pairs, no fallback |
| join_skew_ghj_auto | 4 | 0 | 0 | 12 | 4 hash pairs, 1 fallback, 2 completed inner scans |
| aggregate_spill_hash_seq | 11 | 33 | 50 | 64 | 5 overflow flushes, 6 batches, 649 partials |
| aggregate_spill_sort_seq | 11 | 17 | 23 | 32 | 12 initial runs, 2 intermediate passes, 350 groups |
| aggregate_few_sort_index | 16 | 0 | 0 | 0 | Input order reused, no sorter |

Writes exclude separately counted allocation initialization. Hash aggregation
at M=32768 still has group capacity 117, modeled peak 32760, and partial-sort
allowance 26568. SortAggregate's comparable allowance remains 30664. GHJ at
M=32768 retains p=5, descriptor charge 640, build capacities 58/58, and fallback
allowance 32128. Temporary pins/mappings/current and physical extents are zero
after query cleanup. Approximate fit predictions can still be wrong: the hash
spill fixture's unchanged heuristic score is 11.144 despite actual spilling;
its conservative spill feasibility check and runtime spill path remain essential.

The historical smoke median Phase 4 / Phase 3 timing ratio is 0.9888. Contemporary
paired runs alternate executable order and include independent reference checks.
Their median timings are:

| Case | Phase 3 ms | Phase 4 ms | Ratio |
| --- | ---: | ---: | ---: |
| point_unique_index | 0.950 | 0.870 | 0.9156 |
| sort_m_24576 | 1213.197 | 1256.753 | 1.0359 |
| join_balanced_ghj_auto | 78.855 | 78.425 | 0.9946 |
| join_skew_ghj_auto | 102.022 | 100.489 | 0.9850 |
| aggregate_spill_hash_seq | 740.943 | 727.433 | 0.9818 |
| aggregate_spill_sort_seq | 359.556 | 376.444 | 1.0470 |
| aggregate_few_sort_index | 2.757 | 2.760 | 1.0011 |

The sort and unordered aggregate medians rose 3.6% and 4.7% in these samples;
other cases moved in both directions. All paired I/O, path, memory, result, and
published cost checks match. These limited timings establish neither a general
speedup nor a regression; no algorithmic/resource regression was observed.

Schema 3 intentionally removes the synthetic sort child and folds its local
cost into SortAggregate. New fields expose resolved memory, CPU terms, subtree
score, and the internal-sort property. EXPLAIN no longer reports cumulative
estimated scratch extent. No execution counter changed or disappeared. The
comparison script normalizes only this documented tree change and compares
results/resources independently. Old score comparisons use Phase 3's published
five-significant-digit EXPLAIN precision; schema 3 also emits numeric score/CPU.

Evidence retained:

- Original `phase3_smoke.jsonl`, summary, timing, point/probe and comparison files;
  checksums recorded before editing and checked afterward.
- `/private/tmp/valencedb-phase3-bench`, copied before rebuilding Release.
- `benchmark_results/phase4_smoke.jsonl`, `.summary.json`, `.stderr.log`.
- `benchmark_results/phase4_comparison.json`.
- `benchmark_results/phase4_timing_recheck.jsonl` (every exact command) and
  `phase4_timing_summary.json`.
- `benchmark_results/phase4_logs/`: baseline/focused/final logs, comparison script,
  checksum manifest, and final diff evidence.

## G. Readability improvement

A reader can follow scan enumeration and predicates in Scans, the algorithm/side
loops and replay formulas in Joins, and fit/spill/order choices in Aggregates.
ChoosePlan still shows the complete pipeline and tie-breaking rule. Estimation
has one sort formula and no disposable execution-shaped trees. Each child now
means an actual input; internal aggregate work belongs to the aggregate. Build
has one recursive construction rule and one concrete switch.

The improvement is removal of concepts: four cached layout variants, two synthetic
planning constructions, a child-skipping exception, repeated explanatory strings,
and propagated diagnostic totals. Formatting is longer because it now derives
facts explicitly, but it is outside candidate construction. There is no registry,
visitor, inheritance hierarchy, generic estimator, or ownership framework to learn.

## H. Remaining issues

No unresolved correctness failure is known from this implementation verification.
Statistics and logical costs remain approximate; finite descriptors, append-only
scratch, checked aggregate overflow, and modeled rather than measured memory
remain existing limits. Timing samples do not establish general speedups or
regressions. The historical Phase 3 deletion I/O/RSS tradeoffs remain documented
in its report and were not changed here. Benchmark consumers must understand
schema 3. The separate independent repository audit remains outstanding.
