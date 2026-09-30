#pragma once

#include "valencedb/execution/aggregate.hpp"
#include "valencedb/execution/grace_hash_join.hpp"

namespace valencedb {

struct BaseAccess {
    std::string table;
    std::vector<Int64Predicate> predicates; // AND, in base-table column positions.
    std::optional<ProjectionPlan> projection;
};
struct EquiJoinSpec { size_t left_key, right_key; }; // Access-output positions.
struct QuerySpec {
    BaseAccess left;
    std::optional<BaseAccess> right;
    std::optional<EquiJoinSpec> join;
    std::optional<AggregateSpec> aggregate; // Logical Left+Right positions after join.
    std::optional<size_t> order_column; // Before final projection; ascending NONNULL INT64.
    std::optional<ProjectionPlan> projection;
};
enum class ScanChoice { Auto, SeqScan, IndexScan };
enum class JoinChoice { Auto, NLJ, BNLJ, GHJ };
enum class AggregateChoice { Auto, HashAggregate, SortAggregate };
enum class ScanRingChoice { Auto, On, Off };
struct PlanOptions {
    ScanChoice left_scan = ScanChoice::Auto, right_scan = ScanChoice::Auto;
    JoinChoice join = JoinChoice::Auto;
    std::optional<PhysicalOuter> outer;
    AggregateChoice aggregate = AggregateChoice::Auto;
    ScanRingChoice scan_ring = ScanRingChoice::Auto;
    std::optional<size_t> sort_memory, join_memory, aggregate_memory;
};
struct ModeledCost {
    long double pages = 0, cpu = 0;
    long double Score() const { return pages + 0.0001L * cpu; }
};
enum class PlanKind { SeqScan, IndexScan, Filter, Projection, NLJ, BNLJ, GHJ,
                      GlobalAggregate, HashAggregate, SortAggregate, ExternalSort };
// Small value descriptors only. Candidate comparison never initializes executors.
struct PhysicalPlan {
    PlanKind kind;
    Schema schema;
    Ordering ordering;
    bool exact_rows = false;
    long double rows = 0, row_upper_bound = 0, pages = 0, input_rows = 0;
    ModeledCost local_cost, subtree_cost;
    long double temp_pages = 0, inner_executions = 0;
    std::vector<long double> ndv;
    std::string confidence, details;
    std::vector<PhysicalPlan> children;
    SeqScanPlan seq;
    IndexScanPlan index{"", 0};
    std::optional<Int64Predicate> predicate;
    ProjectionPlan projection;
    JoinPlan join{0, 0};
    AggregateSpec aggregate;
    size_t column = 0;
    std::optional<BlockNestedLoopJoinMemory> block_memory;
    std::optional<GraceHashJoinMemory> grace_memory;
    std::optional<ExternalSortMemory> sort_memory;
    std::optional<AggregateMemory> aggregate_memory;
    PhysicalPlan(PlanKind kind, Schema schema) : kind(kind), schema(std::move(schema)) {}
};
struct OptimizedPlan {
    PhysicalPlan plan;
    std::unique_ptr<Executor> root; // Context must outlive this owning tree.
    QueryResources resources;
    size_t buffer_frames;
    std::vector<std::string> rejected;
};
OptimizedPlan ChoosePlan(QueryContext&, const QuerySpec&, const PlanOptions& = {});
std::string Explain(const OptimizedPlan&);

}  // namespace valencedb
