# Reproducible ValenceDB algorithm experiments

The offline harness exercises the existing ValenceDB library, persistent tables/indexes, query contexts, optimizer forcing controls, and executors. Its purpose is to connect algorithmic work to cache activity, file operations, and measured elapsed time. It compares ValenceDB's own algorithms; it does not compare production DBMSs or claim portable rankings. No new engine algorithm or benchmark shortcut was added.

## Build and run

Only the existing C++20 toolchain, CMake, and Python standard library are needed. There are no downloads.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --output benchmark_results/smoke.jsonl
```

Smoke has 63 configurations covering all five families, with exact reference validation. One repetition is the fast default. To obtain medians and ranges:

```sh
python3 tools/run_benchmarks.py --suite smoke --build-dir build-release --repeats 3 --output benchmark_results/smoke.jsonl
```

The opt-in full suite increases input sizes and uses three repetitions by default. It keeps finite descriptor and scratch limits in mind, caps the all-equal output, and never enters normal CTest/CI:

```sh
python3 tools/run_benchmarks.py --suite full --build-dir build-release --output benchmark_results/full.jsonl
python3 tools/run_benchmarks.py --suite full --build-dir build-release --family sort --output benchmark_results/full_sort.jsonl
python3 tools/run_benchmarks.py --suite full --list-only
```

`--family` can be repeated to select families. `--seed` defaults to 130013; `--repeats` overrides repetitions; `--timeout` defaults to 300 seconds per child process, including setup/reference work. Every repetition uses the same seed and physical insertion order, and a new process/database/cache. Timed intervals exclude setup, so end-to-end suite runtime is longer than the sum of query times.

The runner writes raw JSONL, a `.summary.json` containing successful-run medians/min/max, and a `.stderr.log` containing process exit statuses/stderr. Redirect its stdout/stderr to a `.log` to preserve the terminal summary. Results and Release builds are ignored by Git. Every child record includes a replayable `command` argument array, including seed, trial and all case-specific options. A unique disposable directory is supplied separately; the executable removes only a directory it successfully created, and rejects an existing `--work-dir`. Python also owns a temporary-directory scope. Output files are overwritten on rerun: choose a different output name to retain earlier measurements.

## Executable CLI and deterministic data

`build-release/valencedb_bench --help` describes actual syntax. It emits one JSON object on stdout, even for ordinary configuration/execution errors, and exits 1 on failure. It accepts:

| Options | Meaning / defaults |
|---|---|
| `--family` | `clock`, `btree_point`, `btree_range`, `btree_insert`, `sort` (default), `join`, `aggregate` |
| `--experiment`, `--seed`, `--trial` | Stable label (`manual`), generator seed (130013), repetition label (1) |
| `--rows`, `--right-rows` | Left/single input (240), right join input (120) |
| `--domain`, `--groups` | Join/read key domain (60), aggregate group domain (60) |
| `--distribution` | `unique`, `sequential`, `uniform` (default), `duplicate`, `all_equal`, `skewed`, `uneven` |
| `--measures` | `mixed` (default), `positive`, `constant` |
| `--payload` | Fixed actual VARCHAR length and declared width, 0–256 bytes (64); zero omits VARCHAR |
| `--frames`, `--memory`, `--max-temp-pages` | F (16), default per-operator M (32768 bytes), append-only scratch page cap (16384) |
| `--sort-memory`, `--join-memory`, `--aggregate-memory` | Positive optional local overrides; omitted/recorded zero means use default M |
| `--scan`, `--right-scan` | `auto`, `seq` (default), `index`; right applies to joins |
| `--join`, `--outer` | `auto`, `nlj`, `bnlj` (default), `ghj`; `auto` (default), `left`, `right`; GHJ rejects forced outer |
| `--aggregate` | `auto`, `hash` (default), `sort` |
| `--ring` | `auto`, `on`, `off` (default), through existing PlanOptions for SeqScan |
| `--range`, `--lookups` | `short` (default), `medium`, `long`; point requests per batch (8) |
| `--hot-pages`, `--scan-batch`, `--control` | CLOCK hot pages (4), scan pages between hot rounds (64), hot-only control |
| `--count-star` | Omit the measure column entirely and request only grouped COUNT(*) |
| `--reference` | Extra untimed exact-reference execution; limited to one million reference output rows |
| `--work-dir` | New disposable directory; default is a unique directory beneath the platform temporary directory |

Physical join/aggregate choices apply to the corresponding family. M overrides are passed through existing PlanOptions: final sort M is separate from aggregate-owned internal sort M. `sort` requires an actual ExternalSort; a forced ordered access that eliminates it is rejected as a mislabeled sort experiment. Read/aggregate families create the same index even for forced SeqScan. No Analyze is run after reopening, so EXPLAIN's estimated distributions use documented optimizer heuristics, not measured cardinalities.

Each ordinary row has INT64 key, INT64 measure, and optional VARCHAR. COUNT(*) rows have only key and optional VARCHAR. Payload bytes use `(insertion_position*17 + byte_position*31 + seed) mod 256`, including embedded NULs. Worst-case schema width equals the selected payload width. Measures are integers in [-100,100], [1,100], or the constant 7. No overflow/cancellation data is used.

The generator uses the specified `mt19937_64` sequence, explicit modulo reduction and Fisher–Yates swaps. It avoids implementation-dependent `std::shuffle`/distribution mappings and never uses `random_device`. Modulo reduction has negligible bias for these small domains; this is a deterministic workload, not a statistical sampling claim. Right-input seed is left seed XOR `0x9e3779b97f4a7c15`.

- `sequential`: keys 0 through n-1 in insertion order.
- `unique`: those keys shuffled; heap placement is independent of index key order.
- `uniform`: each key is PRNG output modulo domain.
- `duplicate`: keys `i mod domain`, then shuffled, giving controlled multiplicities.
- `all_equal`: every key is zero.
- `skewed`: on an 80%-probability PRNG branch use zero, otherwise a uniform-domain key (which may also be zero). This is a simple skew construction, **not Zipfian**.
- `uneven`: two key-zero rows with measures 10 and 20; a cycle of other groups; eight final key-zero rows of measure 100. Smoke uses 359 rows/350 groups. The zero group's expected sum/count is 830/10, AVG=83. Its early two and late eight contributions cross spill batches at the chosen capacity.

## Timing, cache state, and lifecycle

Dataset generation/loading, index creation, explicit persistent Flush/Close, reopening, metadata collection, and cache preparation are outside query timing. All clean metadata pages loaded by reopen are invalidated before the measured interval. Thus query experiments start with a **cold BufferPool**. Repetition reconstructs the identical database in a separate process. This does **not** imply cold OS cache or cold storage device; no OS cache dropping occurs.

Each query interval uses `std::chrono::steady_clock` and includes QueryContext construction, physical planning/executor construction, all blocking Init work, every Next through EOF, bounded diagnostic hashing/order checks, and successful FinishQuery. EXPLAIN formatting, JSON emission, metric validation, reference execution and final read-only database Close are outside that timer. Including planning consistently also captures optimizer/executor initialization costs; these are not isolated operator CPU timings. No query flush is needed after clean read-only setup. Normal temporary writes/dirty evictions remain inside the interval; FinishQuery discards remaining dirty scratch as the real lifecycle specifies.

Point lookup records sum intervals for eight deterministic queries: alternating existing and negative missing keys. The initial pool is cold; the **same pool is deliberately preserved within that batch**, so later lookups may reuse pages. Each lookup has a fresh QueryContext and successful FinishQuery. EXPLAIN captures all eight plans. Range records contain one query. No independent alternative inherits that batch's pool.

CLOCK is an explicitly lower-level BufferPool experiment over real heap pages. A reopened/emptied pool receives five excluded normal accesses per hot page, establishing the same hot cache and usage counts for all variants. Each measured round fetches the hot set once, then the next scan batch; a final hot round follows. Scan pages exclude the hot set and exceed eight times F. On/Off use exactly the same PageId schedule. The control omits only scan accesses. Every fetched page's stored header ID is checked against its scheduled ID. The real default ring size is `min(8,max(1,F/8))`. CLOCK is neither an Executor query nor a disk-cold workload; `finish_query_succeeded` is null.

Insertion is also separate from read-query timing. An empty heap and empty index are built/flushed/closed first, then reopened/emptied. The timed interval includes Database::Insert, automatic index maintenance, the digest, and an explicit persistent Flush of the inserted data. It includes heap/catalog work and allocation zeroing, **not isolated B+ tree insertion CPU**. Subsequent complete IndexScan and reference validation are untimed and require FinishQuery. There is no available split counter, so splits are not invented. `finish_query_succeeded` is null for insertion itself; its index-check query has a separate success field.

## Structured schema and metrics

Schema version 3 uses nested JSON objects; 64-bit hash accumulators are hex strings to avoid downstream floating-point loss. All integers represent counts/bytes unless named otherwise. Every successful record has:

| Field | Contents |
|---|---|
| `config` | Complete parsed configuration, defaults included, seed and trial |
| `build_type`, `compiler`, `platform` | CMake build/compiler/OS/architecture; runner also records observed platform strings |
| `dataset` | Actual rows, heap pages, schema columns, maximum encoded bytes, modeled T, index state/height, total database file pages |
| `physical_plans`, `explain` | Actual operator tree, ordering/ring/outer, estimates, resolved budgets, aggregate internal-sort property; EXPLAIN derives capacities and forcing markers |
| `algorithm` | Concrete executor's runtime counters and local memory derivations; GHJ includes summary pair/orientation/fallback counters; aggregates include internal sort counters/capacities |
| `elapsed_ms`, `timing_boundary`, `buffer_state` | Measured interval and exact state/boundary description |
| `result_rows`, `diagnostic_digest`, `ordered_digest` | Consumed row count and explicit stable diagnostic hashes |
| `reference_validation`, `order_verified`, `finish_query_succeeded` | Applicable validation/lifecycle evidence; page/insert experiments use the documented exceptions |
| `buffer` | Fetches/hits/misses, evictions/dirty evictions, write causes, CLOCK visits/decrements, ring reuses/fallbacks, pins |
| `file_io` | Database/temp page reads/writes/allocations/failures, and writes+allocations including initialization |
| `scratch` | Cumulative allocations, pre-reset extent sum, peak single-query extent, post-reset extent/pins/cache mappings |
| `success`, `failure_stage`, `failure_reason` | Boolean outcome; failed prefixes are invalid, and missing timings/counters remain unmeasured |
| Runner fields | Suite/case/equivalence-group identifiers, exact executable argument array, observed platform |

`order_verified` is present on query records and means advertised ordering was checked, not that unordered operators promise order. Some family-specific keys are intentionally absent when inapplicable. Failures always retain schema/configuration/outcome/stage/reason; subprocess/parse errors are marked `runner_validation` and may lack executable metadata. The runner returns nonzero for any unexpected failure or suite-equivalence error and excludes failures from successful medians. It reparses every saved line, checks required fields/digest format/counters/cleanup, verifies family-specific boundary fixtures, compares equivalence groups, and checks ordered digests where order is fully defined. The selected case matrix determines family coverage and exact expected record count.

BufferPool and DiskManager expose cumulative counters. The harness **copies** both immediately before measured work and subtracts cumulative counters afterward, excluding setup, reopen, cache preparation and prior runs. No peak is subtracted: `peak_pins` explicitly covers the reopened pool lifetime, including metadata preload, and `peak_pins_at_start` is supplied. There is no resettable interval pin-peak API. Algorithm metrics reset at Init and are read after FinishQuery/Close; aggregate Close captures final internal-sort counters. TempMetrics is copied **before** FinishQuery resets current extent. A successful query checks zero physical/current scratch extent, zero pins, and no Temporary cache mappings afterward.

Keep these layers distinct:

1. **Logical algorithm work:** scans, comparisons, runs/passes, partition writes and completed hash/fallback reads, partial spill records.
2. **BufferPool:** fetch requests and cache outcomes. Tuple-at-a-time heap scans can fetch a page repeatedly; misses are not logical relation scans.
3. **File operations:** successful 4096-byte Read/Write calls and separate allocations. Allocation writes a zero page and flushes, counted as an allocation **instead of** an ordinary write. Total initialized-page write operations are writes + allocations. These are not fsync or device counters.
4. **Scratch extent:** append-only allocated pages, including released relations, until whole-query reset. Release drops handles, performs no reclamation writes and does not shrink extent.

The sort/aggregate APIs do not expose an exact all-phase logical temp-page transfer counter. Their run/spill counters and actual file/buffer traffic are recorded without inventing a perfect split. GHJ does expose partition-write/completed-read logical page counters. Peak retained rows/groups and modeled bytes are reported where the existing operator supports them. Input generation retains input vectors outside timing; the benchmark process is not bounded by operator M. No RSS is measured; **M is neither RSS nor a whole-query memory limit**. Each heavyweight operator and internal sorter has its own actual local allowance; allowances are not summed into a query cap. See [io_cost_models.md](io_cost_models.md) for underlying definitions.

## Result validation

Timed queries retain one tuple plus a few digest/order counters and consume EOF. No complete result vector grows with join output. Tuple serialization for hashing includes column count, fixed type tags, NULL markers, signed INT64 bits, IEEE DOUBLE bits, string lengths and every byte, all with explicit little-endian 64-bit lengths/numbers. Each tuple gets FNV-1a-64; the unordered diagnostic state combines count, XOR, modular sum and a modular sum of a second explicit integer mixer. A rolling order-sensitive mixer is also recorded. This is a **diagnostic result digest, not cryptographic proof**; collisions are possible. Sorting ties are not promised stable, so ordered comparisons are limited to unique-key sort cases and ascending distinct aggregate groups.

Smoke separately constructs independent canonical reference bags: explicit predicate filtering, nested loops for joins, and integer count/sum/min/max maps with final `double(sum)/double(count)` for AVG. It does not use engine AggregateState, engine joins, or engine predicate evaluation as the oracle. A second untimed real query checks every canonical tuple and multiplicity exactly against that bag; its digest must match the timed execution. This keeps timing consumption bounded while checking more than two engine digests. The timing/reference link remains diagnostic, as honestly labeled. COUNT(*) has no measure column and no invented value. Both BNLJ orientations and every forced alternative must match the same group digest. Full runs omit the potentially large canonical bag but retain full consumption, ordering/cleanup checks and cross-alternative diagnostic equivalence. Sort and B+ tree outputs also always match an independently filtered generated-input digest, including at full scale, without retaining results.

All-equal cardinality is checked before execution with checked multiplication and capped by the harness at ten million emitted rows; the shipped smoke/full cases are much smaller. No early output truncation is used. Successful query status requires EOF and successful FinishQuery; cleanup errors fail the run.

## What the experiments demonstrate

**CLOCK / scan pollution.** The small hot-only control shows whether the hot set fits. Sequential scans can age/recycle pages needed by point activity. The bounded scan ring preferentially reuses a small eligible subset of frames; it is PostgreSQL-inspired, not PostgreSQL-identical. Compare hot-set misses, total misses, CLOCK work, ring activity and elapsed time separately. Fewer hot misses need not imply a measurable time improvement.

**B+ tree.** Unique shuffled keys and duplicate keys exercise known indexes through point lookups and inclusive ranges. Short/medium/long upper bounds are respectively `floor(max_key/100)`, `floor(max_key/4)`, and max_key, with lower bound zero. Point batches contain hits and misses. Tree height and linked leaf traversal help locate RIDs, but the index maps key to RID: heap tuples remain independently stored. The index can be efficient at locating matching RIDs while unclustered heap fetching dominates broad ranges. Measured read-query totals include tree/leaf activity **and heap fetch effects**, without claiming a separately instrumented tree/heap I/O split. Reopened cache state and range selectivity matter as well as height.

**ExternalSort.** M controls private run size and merge fan-in; F controls the shared page cache. The M sweep fixes F/data/schema. The F sweep fixes M/data/schema and checks identical M-derived capacities. Using actual T/S/D/R/C_sort/k, let `r=ceil(rows/C_sort)` and repeatedly reduce `r=ceil(r/k)` while r>k to count materialized intermediate levels L. The final merge is streamed. Under packed-page assumptions temp data transfers are approximately `(2L+2)N` plus original child input once; a materialized-final convention would add a final output write (and potentially a parent reread). Real page tail waste, singleton carries, allocation initialization and caching make file counters differ from this model. The suite checks multi-pass work and streamed-final flags, not monotonic elapsed-time claims.

**Joins.** NLJ replays its physical inner per outer row. For physical outer R and inner S_input its logical model is `pages(R) + rows(R)*pages(S_input)`. BNLJ retains decoded rows in capacity `C_block=floor((M-Sreserve)/Touter)` and uses `pages(R) + ceil(rows(R)/C_block)*pages(S_input)`. It performs the same candidate comparisons with fewer inner rescans. The textbook B-2 formula combines a page-buffer budget; ValenceDB separates private decoded-row M from shared-cache F. Both orientations matter on asymmetric inputs. Cache residency can make file-read counts identical despite very different scan counts.

GHJ consumes both children once, writes partitions, then build/probes bounded pairs. Ideal normal-case packed traffic is `3*(N_R+N_S)`, or more precisely original child input + partition writes + completed partition reads. Fragmentation and empty pairs change this. Oversized skew pairs deliberately use bounded BNLJ over the materialized partitions; substitute that pair's BNLJ rescan model for its hash reads. There is no recursive repartitioning. Full duplicate multiplicity and potentially quadratic output are preserved; fallback is an intended algorithm path, not a defect.

**Aggregation.** Fitting HashAggregate makes one raw input pass plus expected hash CPU. Spilling adds writes of partial-state records, ExternalSort over those partials, and combination; it does not replay raw input. Spill batches include the final nonempty map flush, so `spill_batches=overflow_flushes+1` after spilling. SortAggregate uses small adjacent-group state once input is ordered, reuses genuine IndexScan ordering, and otherwise incurs ExternalSort over raw input. The fixtures vary group cardinality, rows, widths, measures and M, including repeated groups across batches, no-spill, COUNT(*) and unequal AVG batch sizes. Checked arithmetic order-dependence on canceling-overflow inputs is outside these successful comparisons.

## Add or repeat an experiment

For a standalone forced right-outer BNLJ with a local allowance:

```sh
build-release/valencedb_bench --family join --experiment manual_ratio --seed 130013 --trial 1 --rows 32 --right-rows 480 --domain 40 --distribution uniform --payload 32 --frames 16 --memory 65536 --join-memory 32768 --max-temp-pages 16384 --scan seq --right-scan seq --join bnlj --outer right --ring off --reference > benchmark_results/manual_ratio.jsonl
```

Copy a successful runner record's `command` array to reproduce it, or add a straightforward case in `cases()` in `tools/run_benchmarks.py`. Use one equivalence group only for identical logical data/predicates/aggregate outputs. Change the output path, retain raw records, inspect EXPLAIN/actual executor checks, and first validate new configurations at reference scale. Resource failures are diagnostics, never successful performance samples. There is no experiment DSL, operator factory or third-party benchmark framework.

## Limitations

Single-threaded, single-process, one active query; no SQL frontend, transactions, WAL/crash recovery, concurrency or MVCC. Clean flush/close/reopen is supported, not crash durability. One INT64 key/index per table, simple approximate optimizer statistics, finite sort descriptors and cumulative scratch limits remain. Schema charges and M are modeled memory rather than allocator/RSS measurements. Allocation flushes, filesystem caching, scheduling and bounded sink work affect elapsed time. Run order is deterministic rather than randomized; the three-repetition ranges are observations, not confidence intervals. No hardware/CPU/storage model is inferred from OS/architecture strings. No cold device-cache or cross-DBMS claims are made.

## Phase 2 diagnostic schema

Phase 2 and Phase 3 benchmark records use schema_version=2. Grace Join removes `pairs`,
`partition_write_pages`, `completed_hash_read_pages`, and
`completed_fallback_read_pages`. These copied descriptor data or derived logical
transfers, not physical file I/O. New summary fields `left_fallback_pairs`,
`right_fallback_pairs`, and `completed_fallback_inner_scans` report chosen fallback
orientations and actual BNLJ inner scans from completed pairs. Existing path,
output, peak-memory, BufferPool, DiskManager, and temporary-extent fields retain
their meanings. Partial fallback scans are excluded from the new completed count.
Compare older records by common counters and physical I/O; the old per-pair
records can supply orientation and completed scan totals for successful runs.

Removing the per-pair diagnostics changes the shared Grace memory formula to
`p=min(64,(M-S)/(P+256))`, `Dghj=128*p`. This may change capacities or paths near
boundaries; it does not change the per-operator budget or row charges.

## Phase 4 schema version 3

New records use `schema_version=3`; the runner requires version 3 and validates
plan budgets and aggregate structure recursively. Existing version 2 evidence is
preserved for comparison.

- SortAggregate `children` now contains the actual input, removing the synthetic
  ExternalSort. Its `estimated_local_pages` includes internal sorting; its subtree
  estimate and algorithm remain equivalent. `internal_sort` explicitly records
  whether input sorting is needed. A genuine final ExternalSort remains a node.
- Each heavyweight plan reports its resolved `memory_bytes`. New cost fields are
  `estimated_local_cpu`, `estimated_subtree_cpu`, and `estimated_score` (subtree
  pages + 0.0001 × subtree CPU).
- The human-readable `explain` string is generated from selected facts. It no
  longer includes cumulative estimated scratch extent, cached narrative, or a
  synthetic internal-sort hierarchy. Statistics source appears on scans, forcing
  appears once at the top, and internal sorting is described on the aggregate.
- Result digests, reference checks, physical I/O, buffer/scratch measurements,
  algorithm metrics, and their meanings are unchanged from version 2. No runtime
  counter was removed.

To compare version 2 and 3 plan records, fold a SortAggregate's old synthetic
sort into its local cost and replace that child with the sort's input. Compare
subtree costs, actual algorithms, and execution counters independently; do not
interpret the removed display node as removal of the sorting work.


## Standalone V1 B+ tree probe

`bench/bplus_tree_probe.cpp` measures insertion, exact lookup, and a full inclusive
range with duplicate logical keys, then independently validates raw-page structure
outside each timed operation. V1 removes the former erase phase. Old Phase 3 erase
measurements remain historical and are not a current supported workload.

```sh
c++ -std=c++20 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -Iinclude -Itests \
  bench/bplus_tree_probe.cpp build-release/libvalencedb.a -o build-release/valencedb_bplus_probe
build-release/valencedb_bplus_probe 1 8000 254
build-release/valencedb_bplus_probe 16 8000 254
build-release/valencedb_bplus_probe 1 350 4
```

Arguments are buffer frames, rows, and leaf capacity; internal capacity is 204 for
leaf capacity 254 and otherwise matches the supplied leaf capacity. The probe
prints elapsed time, fetches, reads, writes, allocations, evictions, and peak pins.
The query benchmark harness continues to exercise insertion, IndexScan point/range
access, and cross-component output equivalence without deletion.
