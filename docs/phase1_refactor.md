# Phase 1: sealed temporary runs

Baseline: 4f9e906ef6d8f086aadda63edc44be86a84d6b56, initially clean main.
Local branch: refactor/phase1-temp-runs. No push or merge.

## Files

Modified:
- README.md
- docs/aggregation.md, architecture.md, execution.md, external_sort.md, io_cost_models.md, joins.md
- include/valencedb/execution/{aggregate,external_sort,grace_hash_join,temp_relation,temp_scan}.hpp
- src/execution/{aggregate,external_sort,grace_hash_join,temp_relation,temp_scan}.cpp
- tests/{aggregation,execution,grace_hash_join,integration,nested_loop_join,optimizer}_test.cpp

Added: this report. No files deleted. Generated local results and logs are under
benchmark_results/ (already ignored by the repository), including the baseline
and Phase 1 JSONL files. Preserve that directory as the reference for later phases.

## Representation and ownership

Before: TempRelation combined unfinished page-chain metadata, schema/store
references, sealing/release/writer-started flags, and per-relation reader/writer
counts. Readers borrowed its stable address; sort and join held individually
allocated descriptors through unique_ptr.

After: TempWriter owns unfinished metadata and publishes a sealed TempRun only
after Finish successfully publishes the final page. Any append/publication error
closes the writer and prevents later completion. TempRun has only store/schema
pointers, endpoints, and row/page/modeled-byte counts. It has no lifecycle state
or public unfinished constructor. Readers and TempScan copy it. Sort and join
use vector<TempRun>; aggregation stores an optional<TempRun> after its final
partial flush. Concurrent partition writers retain ordinary unique_ptr ownership.

TempStore still owns the entire query's append-only disk extent. Handle
registration, RAII, short pins, dirty pages, and cleanup ordering are preserved:
close handles, discard temporary frames, reset scratch. Failed allocations and
abandoned prefixes remain charged until checked cleanup. Each reader/writer
retains exactly one staging page. TempRun copies neither own pages nor keep a
query alive. Rvalue schemas are rejected at compile time; named immutable schema
and store lifetimes remain explicit caller obligations.

## Algorithms and tests

The existing external-sort tests still assert the exactly-full private-run path,
pending boundary row, finite descriptor capacity, odd-run carry, priority-queue
merge, four materialized merge levels, and streamed final output. Grace tests
retain duplicate multiplicity, both build sides, skew/oversize BNLJ fallback,
runtime build-cap abandonment, and logical left/right output order. Aggregation
tests retain repeated spills, the triggering row, final flush, partial sorting
and combination, COUNT/SUM/MIN/MAX/AVG, overflow, and empty input.

Temporary-storage tests additionally exercise copies/moves/assignments and
reallocation/reordering/destruction of the source vector while readers and a scan
exist; rewind and repeated EOF; stale runs after cleanup and reuse of page IDs in
a new query; rvalue-schema rejection; abandoned/failed writers; repeated Finish;
page-format, link and row-count corruption; read/write/publication failures;
physical allocation before failed frame acquisition; cumulative caps; cancellation
and checked cleanup. Queries still require eight configured frames. The interleaved
fixture pins seven database frames to verify writing and reading with one available
frame, then checks that all pins and handles are released.

## Memory model

On the tested macOS arm64 / AppleClang 21 build:

| Object | Before bytes | After bytes |
| --- | ---: | ---: |
| Descriptor | 64 | 48 |
| Writer including staging page | 4104 | 4144 |
| Reader including staging page | 4136 | 4184 |

The old run vector additionally stored an 8-byte unique_ptr and allocated each
descriptor separately. Value vectors reserve larger contiguous slots, so removing
heap allocations does not imply every partially filled vector uses less memory.
Both sort arrays remain charged at 64 bytes per reserved slot. Its P+128 merge
allowance still covers the larger reader, pointer, allocation allowance, and heap
entry; tuple-owned memory uses the existing schema-based RowMemoryCharge.
Compile-time size assertions protect these bounds. Fixed scratch covers transient
writer/descriptor metadata; aggregation's existing combine scratch also covers
its descriptor copies and reader metadata.

Grace now uses p=min(64,floor((M-S)/(P+320))) and Dghj=192*p:
two 64-byte descriptor allowances plus 64 bytes of pair counters coexist with
P+128 per active writer. Planner and executor share this function. For two INT64
columns on each side at M=20000, p=2 and hash capacity=36 remain unchanged;
Dghj rises 256→384 and fallback memory falls 19744→19616. The minimum two-partition
allowance rises 18456→18840. Fixtures targeting twelve partitions and mixed
three-partition paths use adjusted budgets to preserve those independent behaviors.

No metric fields were removed or renamed. The historical temp_relations_created
field now counts writer constructions, including empty/abandoned runs; descriptor
copies do not affect it.

## Commands and evidence

Run from the repository root, with stdout/stderr saved in
benchmark_results/phase1_logs/. The complete baseline finished before source edits.
Baseline: 13/13 CTest suites passed, 0 failed; 63/63 smoke runs succeeded, 0 failed.
Raw benchmark records retain reference validation, algorithm counters, physical
page I/O, pins, and scratch cleanup results.

Baseline commands:

~~~sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 --output benchmark_results/baseline_before_refactor.jsonl
~~~

Final verification commands:

~~~sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j2
ctest --test-dir build-sanitized --output-on-failure
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 1 --output benchmark_results/phase1_smoke.jsonl
git diff --check
~~~

Final results: Debug 13/13 passed, 0 failed; ASan/UBSan 13/13 passed,
0 failed, with no reported sanitizer diagnostics; Release smoke 63/63 succeeded,
0 failed. Build logs contain no compiler warnings/errors. git diff --check passed.

All 63 baseline/Phase 1 result digests and row counts match, and every smoke case
passed its reference validation. Physical I/O, BufferPool counters, and scratch
metrics are identical in all 63 cases. Active pins, temporary cached pages,
temporary pins, and scratch extent are zero after cleanup. The algorithm objects
match exactly in 58 cases; the five Grace cases differ only in the recalculated
budget/capacity diagnostics (including the fallback block capacity). Partition
counts, hash/fallback paths, output counts, and completed scan counts are unchanged.

Representative physical counters below are identical before and after.
Temporary writes exclude allocation initialization, which remains separately
reported in the raw results.

| Smoke case | Database reads | Temporary reads | Temporary writes | Temporary allocations | Algorithm evidence |
| --- | ---: | ---: | ---: | ---: | --- |
| sort_m_24576 | 19 | 96 | 96 | 97 | 21 initial runs, 4 intermediate passes, streamed final fan-in 2 |
| sort_m_32768 | 20 | 59 | 59 | 65 | 13 initial runs, 2 intermediate passes, streamed final fan-in 2 |
| join_skew_ghj_auto | 4 | 0 | 0 | 12 | 4 hash pairs and 1 BNLJ fallback |
| aggregate_spill_hash_seq | 11 | 33 | 50 | 64 | 5 overflow flushes, 6 batches, 649 partial records; 4 sort passes |

In the smoke Grace cases at M=32768, Dghj changes 640→960, build capacities
58→57, and fallback block capacities 79→77, with no extra scans or I/O in these
fixtures. Smaller capacities may change paths for other inputs near a boundary;
that is the intentional effect of conservative accounting.

The original single-repeat timing comparison was broadly slower (median ratio
1.883), including untouched buffer/B-tree cases. For example, sort_m_24576 measured
652.250→1228.396 ms and aggregate_spill_hash_seq 377.113→741.878 ms.
To investigate, the unchanged reviewed commit was exported with git archive into
a separate /private/tmp directory and rebuilt in Release. Five representative
cases were then run against both binaries in alternating order, three times each
(30 successful reference-validated runs). All paired result digests and physical
I/O matched. The contemporaneous medians were:

| Case | Rebuilt baseline ms | Phase 1 ms | Phase 1 / baseline |
| --- | ---: | ---: | ---: |
| clock_off | 0.822 | 0.822 | 0.9995 |
| btree_insert | 7.890 | 7.886 | 0.9995 |
| sort_m_24576 | 1222.657 | 1222.824 | 1.0001 |
| join_balanced_ghj_auto | 78.620 | 78.805 | 1.0024 |
| aggregate_spill_hash_seq | 728.733 | 731.054 | 1.0032 |

The original slowdown is reproduced by the baseline binary in the current
environment. The interleaved samples show no meaningful refactoring regression
in these paths; they are not a statistically powered performance claim.

Comparison evidence:
- benchmark_results/baseline_before_refactor.jsonl
- benchmark_results/phase1_smoke.jsonl
- benchmark_results/phase1_comparison.json
- benchmark_results/phase1_timing_recheck.jsonl
- benchmark_results/phase1_timing_summary.json
- benchmark_results/phase1_logs/: baseline/final build and test logs, comparison
  output, size probes, timing scripts, and the temporary baseline checkout path.

Additional comparison commands:

~~~sh
python3 benchmark_results/phase1_logs/compare.py
python3 benchmark_results/phase1_logs/timing_recheck.py
~~~

The timing script records each exact executable/argument list in its JSONL
output. The original baseline JSONL was preserved without replacement.


## Limits

Schemas and stores remain non-owning references; destroying or mutating a named
schema prematurely is a caller error, not something reference counting now tracks.
Memory remains a conservative per-operator model, not whole-process RSS.
Temporary pages are reclaimed only at query cleanup. Single smoke repeats provide
noisy timing samples, not exact performance thresholds. Phase 2 state-machine and
algorithm refactoring is intentionally outside this change.
