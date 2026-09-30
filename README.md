# ValenceDB

ValenceDB is a compact C++20 disk-oriented relational database engine built around explicit page formats, bounded-memory execution algorithms, and a small cost-based physical planner. The project is intentionally narrow in scope so the storage, indexing, buffer-management, query-execution, and optimization paths remain inspectable end to end.

## Features

- **Page-oriented storage:** explicit serialization, 4096-byte pages, slotted heap pages, stable RIDs, a persistent catalog, and clean close/reopen persistence.
- **Buffer management:** fixed-size BufferPool, PostgreSQL-inspired CLOCK sweep with bounded usage counts, scan-ring support, and RAII page guards.
- **B+ tree index:** page-backed INT64 index with duplicate logical keys, split/merge/redistribution, root contraction, streaming cursors, and automatic table-index maintenance.
- **Basic execution:** owned-tuple SeqScan, IndexScan, Filter, Projection, and append-only temporary relations.
- **External sorting:** bounded run generation, multi-pass merge, finite descriptor accounting, and streamed final merge.
- **Join algorithms:** tuple-at-a-time Nested Loop Join, Block Nested Loop Join, and one-level Grace Hash Join with bounded BNLJ fallback for oversized/skewed partitions.
- **Aggregation:** bounded HashAggregate with partial-state spilling and external recombination, plus SortAggregate with existing-order reuse.
- **Physical planning:** sampled statistics, deterministic candidate selection, forced physical alternatives, and textual EXPLAIN output.
- **Benchmark harness:** reproducible workloads for buffer behavior, B+ tree access, sorting, joins, and aggregation.

## Build and test

Requirements:

- CMake 3.20+
- A C++20 compiler
- Python 3 only for the benchmark runner

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

The CTest suite covers storage primitives, buffer management, heap/catalog behavior, B+ tree insertion and deletion, execution, external sorting, nested-loop joins, Grace Hash Join, aggregation, optimizer behavior, and cross-component integration.

### Sanitizer build

```sh
cmake -S . -B build-sanitized \
  -DCMAKE_BUILD_TYPE=Debug \
  -DVALENCEDB_ENABLE_SANITIZERS=ON
cmake --build build-sanitized -j2
ctest --test-dir build-sanitized --output-on-failure
```

## Benchmarks

Build the Release benchmark executable and run the smoke suite:

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j2
python3 tools/run_benchmarks.py \
  --suite smoke \
  --build-dir build-release \
  --repeats 3 \
  --output benchmark_results/smoke.jsonl
```

The benchmark runner validates output equivalence and writes structured JSONL plus summary data. `benchmark_results/` is intentionally ignored by Git because the files are generated and environment-specific. See [docs/benchmarks.md](docs/benchmarks.md) for workload definitions, metrics, and interpretation.

## Minimal programmatic example

The public API is programmatic rather than SQL-based:

```cpp
#include "valencedb/optimizer/optimizer.hpp"
#include <iostream>

int main() {
    using namespace valencedb;

    Database db("example.vdb", OpenMode::CreateNew, "example.tmp", 8);
    const Schema schema(SchemaKind::Base,
        {{"id", TypeId::Int64}, {"name", TypeId::Varchar, 32}});

    db.CreateTable("people", schema);
    db.Insert("people", Tuple({Value(int64_t{2}), Value(std::string("Ada"))}));
    db.Insert("people", Tuple({Value(int64_t{1}), Value(std::string("Lin"))}));
    db.CreateIndex("people", 0);
    db.Analyze("people");

    {
        QueryContext context(db, QueryResources{32768, 4096});
        QuerySpec query;
        query.left.table = "people";
        query.left.predicates = {{0, CompareOp::GreaterEqual, 1}};
        query.order_column = 0;

        auto chosen = ChoosePlan(context, query);
        std::cout << Explain(chosen);

        chosen.root->Init();
        Tuple row({});
        while (chosen.root->Next(row)) {
            std::cout << row.value(0).as_int64() << ' '
                      << row.value(1).as_string() << '\n';
        }
        context.FinishQuery(*chosen.root);
    }

    db.Close();
}
```

## Architecture

At a high level:

```text
                    QuerySpec + PlanOptions
                              |
                   ChoosePlan / Explain
                              |
                    owning Executor tree
                    /                 \
          Database / B+ tree       TempRelations
                    \                 /
                     shared BufferPool
                              |
                         DiskManager
               persistent file + scratch file
```

`DiskManager` transfers physical pages. `BufferPool` caches them and mediates pin lifetimes through move-only guards. Heap, catalog, and B+ tree code interpret explicit on-disk formats. Executors exchange owned tuples, while external algorithms use query-scoped temporary relations through the same buffer pool. The planner estimates a small set of legal physical alternatives and constructs the selected executor tree without executing it.

## Documentation

| Topic | Document |
|---|---|
| System organization and ownership | [Architecture](docs/architecture.md) |
| Page, tuple, heap, and catalog formats | [Storage](docs/storage.md) |
| CLOCK sweep, scan ring, and guard rules | [Buffer manager](docs/buffer_manager.md) |
| B+ tree search, split, delete, and repair | [B+ tree](docs/bplus_tree.md) |
| Executor lifecycle and temporary storage | [Execution](docs/execution.md) |
| Bounded external merge sort | [External sort](docs/external_sort.md) |
| NLJ, BNLJ, and Grace Hash Join | [Joins](docs/joins.md) |
| Hash and sort aggregation | [Aggregation](docs/aggregation.md) |
| Statistics, costing, and plan selection | [Optimizer](docs/optimizer.md) |
| Measurement layers and page-cost accounting | [I/O cost models](docs/io_cost_models.md) |
| Benchmark methodology and workloads | [Benchmarks](docs/benchmarks.md) |

## Scope and non-goals

ValenceDB is single-process, single-threaded, and supports one active query. Base-table types are NONNULL `INT64` and `VARCHAR`; intermediate/results additionally support `DOUBLE` and typed NULLs. Each table has at most one INT64 index. Automatic plans contain at most one binary join. Join, grouped aggregation, and ordering each use one NONNULL INT64 key; ordering is ascending. Global aggregation is supported.

The project does **not** implement a SQL parser/frontend, transaction manager, MVCC, WAL/ARIES recovery, locking/deadlock detection, concurrency, parallel execution, networking, distribution, or replication. Persistent page allocation and query scratch allocation are append-only within their respective lifetimes. Statistics are sampled and approximate, and the planner is deliberately small rather than a Cascades-style optimizer.
