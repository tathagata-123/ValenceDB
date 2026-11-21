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
enum class StatisticsSource { Heuristic, Sampled, Stale };
// Small value descriptors only. Candidate comparison never initializes executors.
struct PhysicalPlan {
    PlanKind kind;
    Schema schema;
    Ordering ordering;
    bool exact_rows = false;
    long double rows = 0, row_upper_bound = 0;
    ModeledCost local_cost, subtree_cost;
    std::vector<long double> ndv;
    std::vector<PhysicalPlan> children;
    std::optional<size_t> memory_bytes; // Resolved allowance, also passed to the executor.
    StatisticsSource statistics = StatisticsSource::Heuristic; // Snapshot on scan nodes only.
    SeqScanPlan seq;
    IndexScanPlan index{"", 0};
    std::optional<Int64Predicate> predicate;
    ProjectionPlan projection;
    JoinPlan join{0, 0};
    AggregateSpec aggregate;
    size_t column = 0;
    PhysicalPlan(PlanKind kind, Schema schema) : kind(kind), schema(std::move(schema)) {}
    bool NeedsInternalSort() const {
        return kind == PlanKind::SortAggregate && aggregate.group_column &&
            children[0].ordering != Ordering{true, *aggregate.group_column};
    }
};
struct OptimizedPlan {
    PhysicalPlan plan;
    std::unique_ptr<Executor> root; // Context must outlive this owning tree.
    QueryResources resources;
    size_t buffer_frames;
    std::vector<std::string> rejected;
    PlanOptions options; // Explain forcing once, rather than storing prose on every candidate.
    std::optional<size_t> elided_order_column; // ORDER BY position before final projection.
};
OptimizedPlan ChoosePlan(QueryContext&, const QuerySpec&, const PlanOptions& = {});
std::string Explain(const OptimizedPlan&);

}  // namespace valencedb
