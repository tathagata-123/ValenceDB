#include "test_harness.h"
#include "valencedb/common/error.hpp"
#include "valencedb/optimizer/optimizer.hpp"
#include "valencedb/execution/basic_executors.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>

using namespace valencedb;
namespace {
struct Files {
    std::filesystem::path dir, db, temp;
    Files() {
        static size_t serial = 0;
        dir = std::filesystem::temp_directory_path() / ("valencedb_optimizer_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "_" + std::to_string(++serial));
        std::filesystem::create_directory(dir);
        db = dir / "pages.db";
        temp = dir / "scratch.tmp";
    }
    ~Files() { std::filesystem::remove_all(dir); }
};
void Throws(const std::function<void()>& f, const std::string& message) {
    bool caught = false;
    try { f(); } catch (const DbError& e) {
        caught = true;
        VDB_CHECK(std::string(e.what()).find(message) != std::string::npos);
    }
    VDB_CHECK(caught);
}
void Near(long double actual, long double expected) {
    if (std::abs(actual - expected) > 1e-8L * std::max(1.0L, std::abs(expected)))
        throw std::runtime_error("cost mismatch: " + std::to_string(actual) + " != " + std::to_string(expected));
}
Schema Narrow() { return Schema(SchemaKind::Base, {{"key", TypeId::Int64}}); }
Schema Pair() { return Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"value", TypeId::Int64}}); }
Schema Wide() {
    return Schema(SchemaKind::Base, {{"key", TypeId::Int64}, {"a", TypeId::Varchar, 256},
        {"b", TypeId::Varchar, 256}, {"c", TypeId::Varchar, 256}});
}
void Fill(Database& db, const std::string& name, size_t count, size_t distinct = 0, bool wide = false) {
    db.CreateTable(name, wide ? Wide() : Narrow());
    for (size_t i = 0; i < count; ++i) {
        std::vector<Value> values{Value(static_cast<int64_t>(distinct ? i % distinct : i))};
        if (wide) for (size_t c = 0; c < 3; ++c) values.emplace_back(std::string(256, 'x'));
        db.Insert(name, Tuple(std::move(values)));
    }
    db.Analyze(name);
}
QuerySpec Scan(const std::string& name) {
    QuerySpec s;
    s.left.table = name;
    return s;
}
QuerySpec Join(const std::string& left, const std::string& right) {
    auto s = Scan(left);
    s.right = BaseAccess{right, {}, std::nullopt};
    s.join = EquiJoinSpec{0, 0};
    return s;
}
const PhysicalPlan* Find(const PhysicalPlan& p, PlanKind k) {
    if (p.kind == k) return &p;
    for (const auto& c : p.children) if (auto found = Find(c, k)) return found;
    return nullptr;
}
using Bag = std::vector<std::vector<std::byte>>;
Bag Run(QueryContext& q, OptimizedPlan& plan) {
    Bag result;
    Tuple row({});
    plan.root->Init();
    while (plan.root->Next(row)) {
        VDB_CHECK(q.Db().Metrics().active_pins == 0);
        result.push_back(EncodeTuple(plan.root->OutputSchema(), row));
    }
    q.FinishQuery(*plan.root);
    VDB_CHECK(q.Succeeded());
    VDB_CHECK(q.Db().Disk().PageCount(FileKind::Temporary) == 0);
    std::sort(result.begin(), result.end());
    return result;
}
Bag Execute(Database& db, const QuerySpec& spec, const PlanOptions& options = {}, QueryResources resources = {}) {
    QueryContext q(db, resources);
    auto p = ChoosePlan(q, spec, options);
    return Run(q, p);
}
void Statistics() {
    Files f;
    {
        Database db(f.db, OpenMode::CreateNew, f.temp, 8);
        db.CreateTable("t", Pair());
        VDB_CHECK(!db.Statistics("t"));
        db.Analyze("t");
        auto empty = *db.Statistics("t");
        VDB_CHECK(empty.analyzed_rows == 0 && !empty.stale);
        for (const auto& c : empty.columns) {
            VDB_CHECK(!c.minimum && !c.maximum && c.sample.empty());
            Near(c.approximate_ndv, 0);
        }
        for (int64_t i = 0; i < 4097; ++i) db.Insert("t", Tuple({Value(i - 2048), Value(i % 3)}));
        auto min_rid = db.Insert("t", Tuple({Value(INT64_MIN), Value(int64_t{0})}));
        db.Insert("t", Tuple({Value(INT64_MAX), Value(int64_t{2})}));
        VDB_CHECK(db.Statistics("t")->stale);
        db.Analyze("t");
        const auto first = *db.Statistics("t");
        VDB_CHECK(first.analyzed_rows == 4099 && !first.stale);
        VDB_CHECK(first.columns[0].minimum == INT64_MIN && first.columns[0].maximum == INT64_MAX);
        for (const auto& c : first.columns) {
            VDB_CHECK(c.sample.size() == 1024);
            VDB_CHECK(c.approximate_ndv >= 1 && c.approximate_ndv <= first.analyzed_rows);
        }
        Near(first.columns[1].approximate_ndv, 3);
        db.Analyze("t");
        VDB_CHECK(first.columns[0].sample == db.Statistics("t")->columns[0].sample);
        db.Delete("t", min_rid);
        VDB_CHECK(db.Statistics("t")->stale);
        db.Analyze("t");
        VDB_CHECK(db.Statistics("t")->columns[0].minimum == -2048);
        db.CreateIndex("t", 0);
        VDB_CHECK(db.Statistics("t")->stale);
        db.Analyze("t");
        db.Close();
    }
    Database db(f.db, OpenMode::OpenExisting, f.temp, 8);
    VDB_CHECK(!db.Statistics("t"));
    VDB_CHECK(db.Table("t").live_rows == 4098 && db.Index("t").Height() >= 2);
    QueryContext q(db);
    auto p = ChoosePlan(q, Scan("t"));
    VDB_CHECK(Explain(p).find("heuristic") != std::string::npos);
    Throws([&] { db.Analyze("t"); }, "active query");
    q.Close();
    std::cout << "sample=1024, NDV(repeated)=3, signed extrema exact, stale/reanalyze/reopen verified\n";
}
void Selectivity() {
    ColumnStatistics s{0, 99, 100, {}};
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::Equal, 42}), .01L);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::Equal, -1}), 0);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::Equal, 100}), 0);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::Less, 50}), .5L);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::LessEqual, 50}), .51L);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::Greater, 50}), .49L);
    Near(EstimateSelectivity(&s, 100, {0, CompareOp::GreaterEqual, 50}), .5L);
    Near(EstimateSelectivity(nullptr, 100, {0, CompareOp::Equal, 2}), .1L);
    Near(EstimateSelectivity(nullptr, 100, {0, CompareOp::Less, 2}), 1.0L / 3);
    Near(EstimateSelectivity(nullptr, 0, {0, CompareOp::Equal, 2}), 0);
    for (int64_t x : {INT64_MIN, int64_t{-4}, INT64_MAX}) {
        s = {x, x, 1, {}};
        Near(EstimateSelectivity(&s, 10, {0, CompareOp::Equal, x}), 1);
        Near(EstimateSelectivity(&s, 10, {0, CompareOp::Less, x}), 0);
        Near(EstimateSelectivity(&s, 10, {0, CompareOp::LessEqual, x}), 1);
        Near(EstimateSelectivity(&s, 10, {0, CompareOp::Greater, x}), 0);
        Near(EstimateSelectivity(&s, 10, {0, CompareOp::GreaterEqual, x}), 1);
    }
    s = {INT64_MAX - 1, INT64_MAX, 2, {}};
    Near(EstimateSelectivity(&s, 2, {0, CompareOp::Equal, INT64_MAX - 2}), 0);
    Near(EstimateSelectivity(&s, 2, {0, CompareOp::Less, INT64_MAX}), .5L);
    s = {INT64_MIN, INT64_MAX, 2, {}};
    Near(EstimateSelectivity(&s, 2, {0, CompareOp::Less, 0}), .5L);
    Near(EstimateNDV(&s, 1), 1);
}
void ScanChoices() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 16);
    Fill(db, "t", 2000);
    db.CreateIndex("t", 0);
    db.Analyze("t");
    auto spec = Scan("t");
    spec.left.predicates = {{0, CompareOp::Equal, 42}};
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, spec);
        const auto* scan = Find(p.plan, PlanKind::IndexScan);
        VDB_CHECK(scan && !Find(p.plan, PlanKind::SeqScan));
        Near(scan->rows, 1);
        Near(scan->local_cost.pages, db.Index("t").Height() - 1 + 1 + 1);
        Near(p.plan.rows, 1);
        VDB_CHECK(Explain(p).find("heap_fetches=1") != std::string::npos);
        std::cout << "selective scan: rows=" << scan->rows << " index pages=" << scan->local_cost.pages
                  << " versus SeqScan=" << db.Table("t").heap_pages << '\n';
        VDB_CHECK(Run(q, p).size() == 1);
    }
    spec.left.predicates = {{0, CompareOp::GreaterEqual, 0}, {0, CompareOp::LessEqual, 1999}};
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, spec);
        const auto* scan = Find(p.plan, PlanKind::SeqScan);
        VDB_CHECK(scan && scan->seq.use_scan_ring);
        Near(scan->local_cost.pages, db.Table("t").heap_pages);
        std::cout << "wide range: SeqScan pages=" << scan->local_cost.pages << '\n';
        q.Close();
    }
    for (auto ring : {ScanRingChoice::Auto, ScanRingChoice::On, ScanRingChoice::Off}) {
        QueryContext q(db);
        PlanOptions o;
        o.left_scan = ScanChoice::SeqScan;
        o.scan_ring = ring;
        auto p = ChoosePlan(q, spec, o);
        const auto* scan = Find(p.plan, PlanKind::SeqScan);
        VDB_CHECK(scan && scan->seq.use_scan_ring == (ring != ScanRingChoice::Off));
        VDB_CHECK(Explain(p).find("forced scan") != std::string::npos);
        q.Close();
    }
    PlanOptions index, seq;
    index.left_scan = ScanChoice::IndexScan;
    seq.left_scan = ScanChoice::SeqScan;
    VDB_CHECK(Execute(db, spec, index) == Execute(db, spec, seq));
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, Scan("t"), index);
        Near(p.plan.local_cost.pages, db.Index("t").Height() - 1 + std::ceil(2000.0L / 254) + 2000);
        VDB_CHECK(p.plan.local_cost.pages > db.Table("t").heap_pages);
        q.Close();
    }
    db.Insert("t", Tuple({Value(int64_t{900000})}));
    spec.left.predicates = {{0, CompareOp::Equal, 900000}};
    Bag stale;
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, spec);
        Near(p.plan.rows, 0); // Advisory zero must not remove the real filter/access.
        VDB_CHECK(Explain(p).find("stale-sampled") != std::string::npos);
        stale = Run(q, p);
        VDB_CHECK(stale.size() == 1);
    }
    db.Analyze("t");
    VDB_CHECK(stale == Execute(db, spec));
    VDB_CHECK(stale == Execute(db, spec, seq));
    std::cout << "stale out-of-range estimate=0 still returned the inserted row\n";
}
void OrderAndProjection() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "wide", 150, 0, true);
    db.CreateIndex("wide", 0);
    db.Analyze("wide");
    auto spec = Scan("wide");
    spec.order_column = 0;
    PlanOptions seq;
    seq.left_scan = ScanChoice::SeqScan;
    {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec);
        auto alternative = ChoosePlan(q, spec, seq);
        VDB_CHECK(p.plan.kind == PlanKind::IndexScan);
        VDB_CHECK(alternative.plan.kind == PlanKind::ExternalSort);
        VDB_CHECK(p.plan.subtree_cost.Score() < alternative.plan.subtree_cost.Score());
        VDB_CHECK(Explain(p).find("Sort eliminated") != std::string::npos);
        std::cout << "order benefit: full IndexScan=" << p.plan.subtree_cost.Score()
                  << " SeqScan+sort=" << alternative.plan.subtree_cost.Score() << '\n';
        VDB_CHECK(Run(q, p).size() == 150);
    }
    db.CreateTable("cols", Schema(SchemaKind::Base, {{"a", TypeId::Int64}, {"b", TypeId::Int64},
        {"c", TypeId::Int64}, {"key", TypeId::Int64}}));
    for (int64_t i = 0; i < 20; ++i) db.Insert("cols", Tuple({Value(20-i), Value(i), Value(i), Value(i)}));
    db.CreateIndex("cols", 3);
    db.Analyze("cols");
    spec = Scan("cols");
    spec.left.predicates = {{0, CompareOp::Greater, 5}};
    spec.left.projection = ProjectionPlan{{0, 3}};
    spec.order_column = 1;
    spec.projection = ProjectionPlan{{1, 0}};
    PlanOptions index;
    index.left_scan = ScanChoice::IndexScan;
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, spec, index);
        VDB_CHECK(!Find(p.plan, PlanKind::ExternalSort));
        VDB_CHECK((p.plan.ordering == Ordering{true, 0}));
        VDB_CHECK((p.plan.children[0].ordering == Ordering{true, 1}));
        VDB_CHECK((Find(p.plan, PlanKind::Filter)->ordering == Ordering{true, 3}));
        VDB_CHECK(p.root->OutputOrdering() == p.plan.ordering);
        VDB_CHECK(Run(q, p).size() == 15);
    }
    spec.left.projection = ProjectionPlan{{0}};
    spec.order_column = 0;
    spec.projection.reset();
    {
        QueryContext q(db);
        auto p = ChoosePlan(q, spec, index);
        VDB_CHECK(p.plan.kind == PlanKind::ExternalSort);
        VDB_CHECK(!p.plan.children[0].ordering.ordered);
        VDB_CHECK(Run(q, p).size() == 15);
    }
}
void JoinChoices() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "one", 1);
    Fill(db, "twenty", 20);
    Fill(db, "thirty", 30);
    Fill(db, "large", 1000);
    const std::vector<std::tuple<std::string, std::string, PlanKind>> cases{
        {"one", "thirty", PlanKind::NLJ}, {"twenty", "thirty", PlanKind::BNLJ},
        {"large", "large", PlanKind::GHJ}};
    for (const auto& [left, right, expected] : cases) {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, Join(left, right));
        VDB_CHECK(p.plan.kind == expected);
        std::cout << "join " << left << '+' << right << " score=" << p.plan.subtree_cost.Score()
                  << " " << p.plan.details << '\n';
        if (p.plan.block_memory) std::cout << "C_block=" << p.plan.block_memory->block_capacity << '\n';
        if (p.plan.grace_memory) {
            const auto m = *p.plan.grace_memory;
            std::cout << "p=" << m.partition_count << " Dghj=" << m.descriptor_bytes
                      << " build=" << m.left_build_capacity << ',' << m.right_build_capacity << '\n';
            const auto* actual = dynamic_cast<GraceHashJoin*>(p.root.get());
            VDB_CHECK(actual && actual->Memory().partition_count == m.partition_count);
            VDB_CHECK(actual->Memory().left_build_capacity == m.left_build_capacity);
            VDB_CHECK(actual->Memory().fallback_memory == m.fallback_memory);
            const auto& l = p.plan.children[0];
            const auto& r = p.plan.children[1];
            Near(p.plan.local_cost.pages, 2 * (l.pages + r.pages));
            Near(p.plan.subtree_cost.pages, l.subtree_cost.pages + r.subtree_cost.pages + p.plan.local_cost.pages);
        }
        VDB_CHECK(Run(q, p).size() == db.Table(left).live_rows);
    }
    Fill(db, "narrow", 40);
    Fill(db, "wide", 5, 0, true);
    for (bool mirrored : {false, true}) {
        QueryContext q(db, {18200, 4096});
        PlanOptions o;
        o.join = JoinChoice::BNLJ;
        auto p = ChoosePlan(q, mirrored ? Join("wide", "narrow") : Join("narrow", "wide"), o);
        VDB_CHECK(p.plan.join.outer == (mirrored ? PhysicalOuter::Right : PhysicalOuter::Left));
        VDB_CHECK(p.plan.block_memory->block_capacity == 29);
        Near(p.plan.inner_executions, 2);
        auto* actual = dynamic_cast<BlockNestedLoopJoin*>(p.root.get());
        VDB_CHECK(actual && actual->Memory().block_capacity == 29);
        std::cout << "wide-row BNLJ " << p.plan.details << " C=29 versus wide C=2\n";
        VDB_CHECK(Run(q, p).size() == 5);
    }
    // NLJ chooses the one-row side as outer even when it is logical Right.
    {
        QueryContext q(db);
        PlanOptions o;
        o.join = JoinChoice::NLJ;
        auto p = ChoosePlan(q, Join("thirty", "one"), o);
        VDB_CHECK(p.plan.join.outer == PhysicalOuter::Right);
        VDB_CHECK(Run(q, p).size() == 1);
    }
}
void ReplayCosts() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "outer", 40);
    Fill(db, "inner", 1000);
    db.CreateIndex("inner", 0);
    db.Analyze("inner");
    auto spec = Join("outer", "inner");
    spec.right->predicates = {{0, CompareOp::Equal, 7}};
    for (auto algorithm : {JoinChoice::NLJ, JoinChoice::BNLJ}) {
        QueryContext q(db, {10500, 4096});
        PlanOptions o;
        o.join = algorithm;
        o.outer = PhysicalOuter::Left;
        auto p = ChoosePlan(q, spec, o);
        const auto& inner = p.plan.children[1];
        VDB_CHECK(Find(inner, PlanKind::IndexScan));
        const long double replays = algorithm == JoinChoice::NLJ ? 40 : 7;
        Near(p.plan.inner_executions, replays);
        Near(inner.subtree_cost.pages, 3);
        Near(p.plan.subtree_cost.pages, 1 + replays * 3);
        Near(p.plan.local_cost.pages, 0);
        Near(p.plan.local_cost.cpu, 40);
        Near(p.plan.subtree_cost.cpu, replays * inner.subtree_cost.cpu + p.plan.local_cost.cpu);
        if (algorithm == JoinChoice::BNLJ) VDB_CHECK(p.plan.block_memory->block_capacity == 6);
        std::cout << "selected index inner replay=" << replays << "*3, subtree pages=" << p.plan.subtree_cost.pages << '\n';
        VDB_CHECK(Run(q, p).size() == 1);
    }
}
void ForcedJoins() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    db.CreateTable("left", Pair());
    db.CreateTable("right", Pair());
    for (int64_t i = 0; i < 15; ++i) {
        db.Insert("left", Tuple({Value(i % 4), Value(i + 100)}));
        db.Insert("right", Tuple({Value(i % 4), Value(i + 900)}));
    }
    db.Analyze("left");
    db.Analyze("right");
    auto spec = Join("left", "right");
    auto expected = Execute(db, spec, {}, {32768, 4096});
    for (auto algorithm : {JoinChoice::NLJ, JoinChoice::BNLJ, JoinChoice::GHJ}) {
        for (auto side : {PhysicalOuter::Left, PhysicalOuter::Right}) {
            if (algorithm == JoinChoice::GHJ && side == PhysicalOuter::Right) continue;
            QueryContext q(db, {32768, 4096});
            PlanOptions o;
            o.join = algorithm;
            if (algorithm != JoinChoice::GHJ) o.outer = side;
            auto p = ChoosePlan(q, spec, o);
            const auto expected_kind = algorithm == JoinChoice::NLJ ? PlanKind::NLJ :
                algorithm == JoinChoice::BNLJ ? PlanKind::BNLJ : PlanKind::GHJ;
            VDB_CHECK(p.plan.kind == expected_kind);
            VDB_CHECK(Explain(p).find("forced join") != std::string::npos);
            if (algorithm != JoinChoice::GHJ) {
                VDB_CHECK(p.plan.join.outer == side);
                VDB_CHECK(Explain(p).find("forced outer") != std::string::npos);
            }
            VDB_CHECK(p.root->OutputSchema().column(1).name == "value");
            VDB_CHECK(Run(q, p) == expected);
        }
    }
    // Asymmetric projected keys ensure swaps do not use original source positions.
    spec.left.projection = ProjectionPlan{{1, 0}};
    spec.join = EquiJoinSpec{1, 0};
    PlanOptions l, r;
    l.join = r.join = JoinChoice::BNLJ;
    l.outer = PhysicalOuter::Left;
    r.outer = PhysicalOuter::Right;
    VDB_CHECK(Execute(db, spec, l) == Execute(db, spec, r));
    {
        QueryContext q(db, {1, 4096});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::NLJ);
        VDB_CHECK(Explain(p).find("BNLJ rejected") != std::string::npos);
        VDB_CHECK(Explain(p).find("GHJ rejected") != std::string::npos);
        l.join = JoinChoice::BNLJ;
        Throws([&] { (void)ChoosePlan(q, spec, l); }, "block join memory");
        PlanOptions g;
        g.join = JoinChoice::GHJ;
        Throws([&] { (void)ChoosePlan(q, spec, g); }, "Grace join memory");
        q.Close();
    }
    {
        QueryContext q(db, {18000, 4096}); // Covers scratch, but p<2.
        PlanOptions g;
        g.join = JoinChoice::GHJ;
        Throws([&] { (void)ChoosePlan(q, spec, g); }, "at least two partitions");
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind != PlanKind::GHJ);
        q.Close();
    }
}
void GraceFallback() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "skew", 100, 1);
    QueryContext q(db, {20000, 4096});
    PlanOptions o;
    o.join = JoinChoice::GHJ;
    auto p = ChoosePlan(q, Join("skew", "skew"), o);
    auto* actual = dynamic_cast<GraceHashJoin*>(p.root.get());
    VDB_CHECK(actual && p.plan.kind == PlanKind::GHJ);
    VDB_CHECK(Run(q, p).size() == 10000);
    VDB_CHECK(actual->Metrics().fallback_pairs == 1);
    VDB_CHECK(Explain(p).find("runtime BNLJ overflow fallback") != std::string::npos);
}
void AggregateChoices() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "few", 500, 3);
    Fill(db, "many", 1000);
    auto spec = Scan("few");
    spec.aggregate = AggregateSpec{0, std::nullopt, true};
    {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::HashAggregate);
        Near(p.plan.rows, 3);
        Near(p.plan.local_cost.pages, 0);
        Near(p.plan.subtree_cost.pages, p.plan.children[0].subtree_cost.pages);
        VDB_CHECK(p.plan.aggregate_memory->hash_group_capacity == 117);
        auto* actual = dynamic_cast<HashAggregate*>(p.root.get());
        VDB_CHECK(actual && actual->Memory().hash_group_capacity == 117);
        VDB_CHECK(Explain(p).find("predicted spill=no") != std::string::npos);
        std::cout << "HashAggregate groups=3 capacity=117 score=" << p.plan.subtree_cost.Score() << '\n';
        VDB_CHECK(Run(q, p).size() == 3);
    }
    spec.left.table = "many";
    {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::SortAggregate);
        VDB_CHECK(p.plan.children[0].kind == PlanKind::ExternalSort);
        VDB_CHECK(p.plan.children[0].sort_memory->max_bytes == p.plan.aggregate_memory->sort_memory_bytes);
        auto* actual = dynamic_cast<SortAggregate*>(p.root.get());
        VDB_CHECK(actual && actual->Memory().sort_memory_bytes == p.plan.aggregate_memory->sort_memory_bytes);
        PlanOptions h;
        h.aggregate = AggregateChoice::HashAggregate;
        auto hash = ChoosePlan(q, spec, h);
        VDB_CHECK(hash.plan.kind == PlanKind::HashAggregate);
        VDB_CHECK(Explain(hash).find("predicted spill=yes") != std::string::npos);
        const long double partial_pages = std::ceil(1000.0L / 72);
        Near(hash.plan.local_cost.pages, 12 * partial_pages); // write/read partial + 10 sort temp passes.
        Near(hash.plan.subtree_cost.pages, hash.plan.children[0].subtree_cost.pages + hash.plan.local_cost.pages);
        std::cout << "high NDV: SortAggregate=" << p.plan.subtree_cost.Score()
                  << " HashAggregate spill=" << hash.plan.subtree_cost.Score() << '\n';
        VDB_CHECK(Run(q, p).size() == 1000);
    }
    // A narrow indexed filter makes index access attractive; streaming aggregation then wins.
    db.CreateIndex("many", 0);
    db.Analyze("many");
    spec.left.predicates = {{0, CompareOp::Equal, 3}};
    spec.order_column = 0;
    {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::SortAggregate);
        VDB_CHECK(Find(p.plan, PlanKind::IndexScan));
        VDB_CHECK(!Find(p.plan, PlanKind::ExternalSort));
        VDB_CHECK(Explain(p).find("sort elided=yes") != std::string::npos);
        VDB_CHECK((p.plan.ordering == Ordering{true, 0}));
        std::cout << "ordered SortAggregate score=" << p.plan.subtree_cost.Score() << " sort elided\n";
        VDB_CHECK(Run(q, p).size() == 1);
    }
    spec.left.table = "few";
    spec.left.predicates.clear();
    spec.order_column.reset();
    auto expected = Execute(db, spec, {}, {32768, 4096});
    for (auto algorithm : {AggregateChoice::HashAggregate, AggregateChoice::SortAggregate}) {
        PlanOptions o;
        o.aggregate = algorithm;
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec, o);
        VDB_CHECK(p.plan.kind == (algorithm == AggregateChoice::HashAggregate ? PlanKind::HashAggregate : PlanKind::SortAggregate));
        VDB_CHECK(Explain(p).find("forced aggregate") != std::string::npos);
        VDB_CHECK(Run(q, p) == expected);
    }
    // Run the actual high-NDV forced hash spill and compare bags with raw SortAggregate.
    spec.left.table = "many";
    PlanOptions h, s;
    h.aggregate = AggregateChoice::HashAggregate;
    s.aggregate = AggregateChoice::SortAggregate;
    VDB_CHECK(Execute(db, spec, h, {65536, 4096}) == Execute(db, spec, s, {65536, 4096}));
}
void GlobalAndValidation() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "empty", 0);
    Fill(db, "t", 3);
    auto spec = Scan("empty");
    spec.aggregate = AggregateSpec{std::nullopt, std::nullopt, true};
    {
        QueryContext q(db, {1500, 7});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::GlobalAggregate);
        VDB_CHECK(!Find(p.plan, PlanKind::ExternalSort));
        VDB_CHECK(p.plan.aggregate_memory->hash_group_capacity == 0);
        p.root->Init();
        Tuple row({});
        VDB_CHECK(p.root->Next(row) && row.value(0).as_int64() == 0);
        VDB_CHECK(!p.root->Next(row));
        q.FinishQuery(*p.root);
    }
    QueryContext q(db, {32768, 99});
    auto invalid = spec;
    invalid.aggregate = AggregateSpec{std::nullopt, 0, false, true};
    invalid.order_column = 0;
    Throws([&] { (void)ChoosePlan(q, invalid); }, "NONNULL INT64");
    invalid = Scan("t");
    invalid.right = BaseAccess{"empty", {}, std::nullopt};
    Throws([&] { (void)ChoosePlan(q, invalid); }, "appear together");
    invalid = Scan("t");
    invalid.left.predicates = {{0, static_cast<CompareOp>(99), 0}};
    Throws([&] { (void)ChoosePlan(q, invalid); }, "unsupported predicate");
    invalid = Scan("t");
    invalid.left.projection = ProjectionPlan{{0}};
    invalid.order_column = 1;
    Throws([&] { (void)ChoosePlan(q, invalid); }, "projected key");
    invalid = Scan("t");
    invalid.aggregate = AggregateSpec{0, std::nullopt, false, true};
    Throws([&] { (void)ChoosePlan(q, invalid); }, "measure column");
    invalid.aggregate = AggregateSpec{};
    Throws([&] { (void)ChoosePlan(q, invalid); }, "at least one output");
    PlanOptions o;
    o.left_scan = ScanChoice::IndexScan;
    Throws([&] { (void)ChoosePlan(q, Scan("t"), o); }, "no matching index");
    o = {};
    o.outer = PhysicalOuter::Right;
    Throws([&] { (void)ChoosePlan(q, Scan("t"), o); }, "require a join");
    o.join = JoinChoice::GHJ;
    Throws([&] { (void)ChoosePlan(q, Join("t", "t"), o); }, "no physical outer");
    o = {};
    o.aggregate = AggregateChoice::SortAggregate;
    Throws([&] { (void)ChoosePlan(q, spec, o); }, "requires grouping");
    o = {};
    o.left_scan = static_cast<ScanChoice>(100);
    Throws([&] { (void)ChoosePlan(q, Scan("t"), o); }, "unsupported plan option");
    o = {};
    o.sort_memory = 0;
    Throws([&] { (void)ChoosePlan(q, Scan("t"), o); }, "must be positive");
    q.Close();
}
void MemoryAndCost() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    Fill(db, "one", 1);
    Fill(db, "three", 3, 1);
    Fill(db, "wide", 150, 0, true);
    {
        QueryContext q(db, {11200, 4096});
        auto spec = Scan("one");
        spec.order_column = 0;
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.sort_memory->run_capacity == 2 && p.plan.sort_memory->merge_fan_in == 0);
        VDB_CHECK(Run(q, p).size() == 1);
    }
    {
        QueryContext q(db, {11200, 4096});
        auto spec = Scan("three");
        spec.order_column = 0;
        spec.left.predicates = {{0, CompareOp::Equal, 99}}; // Sample estimate zero cannot prove no sort needed.
        Throws([&] { (void)ChoosePlan(q, spec); }, "k >= 2");
        q.Close();
    }
    {
        QueryContext q(db, {10488, 4096});
        auto spec = Scan("one");
        spec.aggregate = AggregateSpec{0, std::nullopt, true};
        PlanOptions o;
        o.aggregate = AggregateChoice::HashAggregate;
        auto p = ChoosePlan(q, spec, o);
        VDB_CHECK(p.plan.aggregate_memory->hash_group_capacity == 1);
        VDB_CHECK(Run(q, p).size() == 1); // Exact bound proves spill cannot be needed.
    }
    for (size_t memory : {size_t{1}, size_t{10392}, size_t{10488}}) {
        QueryContext q(db, {memory, 4096});
        auto spec = Scan("three");
        spec.aggregate = AggregateSpec{0, std::nullopt, true};
        PlanOptions o;
        o.aggregate = AggregateChoice::HashAggregate;
        Throws([&] { (void)ChoosePlan(q, spec, o); }, "no feasible");
        o.aggregate = AggregateChoice::SortAggregate;
        Throws([&] { (void)ChoosePlan(q, spec, o); }, "no feasible");
        q.Close();
    }
    {
        QueryContext q(db, {32768, 4096});
        auto spec = Scan("wide");
        spec.order_column = 0;
        auto p = ChoosePlan(q, spec);
        const auto& m = *p.plan.sort_memory;
        VDB_CHECK(m.run_capacity == 8 && m.merge_fan_in == 2 && m.descriptor_capacity == 32);
        Near(p.plan.local_cost.pages, 300);
        Near(p.plan.subtree_cost.pages, 330);
        Near(p.plan.temp_pages, 150); // Initial + 4 intermediate levels, append-only.
        auto* actual = dynamic_cast<ExternalSort*>(p.root.get());
        VDB_CHECK(actual && actual->Memory().run_capacity == m.run_capacity);
        VDB_CHECK(actual->Memory().merge_fan_in == m.merge_fan_in);
        VDB_CHECK(Run(q, p).size() == 150);
    }
    Fill(db, "too_many", 257, 0, true);
    {
        QueryContext q(db, {32768, 4096});
        auto spec = Scan("too_many");
        spec.order_column = 0;
        Throws([&] { (void)ChoosePlan(q, spec); }, "exact input exceeds initial run descriptor cap");
        // Once filtered, row count is uncertain: the sorter enforces its own descriptor cap.
        spec.left.predicates = {{0, CompareOp::Equal, 1}};
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(Run(q, p).size() == 1);
    }
    // max_temp_pages is runtime authoritative; it is carried unchanged and not an estimate rejection.
    {
        QueryContext q(db, {32768, 1});
        PlanOptions o;
        o.join = JoinChoice::GHJ;
        auto p = ChoosePlan(q, Join("three", "three"), o);
        VDB_CHECK(p.resources.max_temp_pages == 1);
        VDB_CHECK(Explain(p).find("max_temp_pages=1") != std::string::npos);
        Throws([&] { p.root->Init(); }, "temporary");
        p.root->Close();
        q.Close();
        VDB_CHECK(!q.Succeeded());
    }
    // Helper calculations are shared with unchanged execution algorithms; check independent formulas.
    for (const auto& schema : {Narrow(), Pair(), Wide()}) {
        for (size_t memory : {size_t{32768}, size_t{65536}, size_t{1048576}}) {
            QueryResources resources{memory, 4096};
            const auto t = RowMemoryCharge(schema), scratch = 8192 + 3 * t + 1024;
            const auto sort = ComputeSortMemory(resources, schema);
            VDB_CHECK(sort.scratch_bytes == scratch);
            VDB_CHECK(sort.descriptor_capacity == std::min(size_t{4096}, memory / 1024));
            VDB_CHECK(sort.descriptor_bytes == 128 * sort.descriptor_capacity);
            VDB_CHECK(sort.run_capacity == (memory - scratch - sort.descriptor_bytes) / t);
            VDB_CHECK(sort.merge_fan_in == std::min(size_t{64}, (memory - scratch - sort.descriptor_bytes) / (4096 + t + 128)));
            auto block = ComputeBlockJoinMemory(resources, schema, Narrow(), schema);
            VDB_CHECK(block.block_capacity == (memory - scratch) / t);
            auto grace = ComputeGraceJoinMemory(resources, schema, Narrow(), schema);
            VDB_CHECK(grace.partition_count == std::min(size_t{64}, (memory - scratch) / 4224));
            VDB_CHECK(grace.descriptor_bytes == 128 * grace.partition_count);
            VDB_CHECK(grace.left_build_capacity == (memory - scratch - grace.descriptor_bytes) / (t + 96));
            VDB_CHECK(grace.fallback_memory == memory - grace.descriptor_bytes);
            AggregateSpec spec{0, std::nullopt, true};
            const auto output = AggregateOutputSchema(schema, spec);
            auto hash = ComputeHashAggregateMemory(resources, schema, output, true);
            VDB_CHECK(hash.scratch_bytes == 8192 + 3 * std::max(t, size_t{360}) + 1024);
            VDB_CHECK(hash.hash_group_capacity == (memory - hash.scratch_bytes) / 192);
            VDB_CHECK(hash.sort_memory_bytes == memory - hash.combine_bytes - 4096);
            auto stream = ComputeSortAggregateMemory(resources, schema, output, true);
            VDB_CHECK(stream.sort_memory_bytes == memory - stream.combine_bytes);
        }
    }
    ModeledCost huge{1e30L, 1e38L};
    VDB_CHECK(std::isfinite(huge.Score()) && huge.Score() > 1e30L);
}
void PipelineAndExplain() {
    Files f;
    Database db(f.db, OpenMode::CreateNew, f.temp, 8);
    db.CreateTable("employees", Pair());
    db.CreateTable("departments", Pair());
    for (int64_t i = 0; i < 100; ++i) db.Insert("employees", Tuple({Value(i % 5), Value(i + 1)}));
    for (int64_t i = 0; i < 5; ++i) db.Insert("departments", Tuple({Value(i), Value(i + 100)}));
    db.Analyze("employees");
    db.Analyze("departments");
    auto spec = Join("employees", "departments");
    spec.left.predicates = {{1, CompareOp::Greater, 20}};
    spec.right->predicates = {{0, CompareOp::Less, 4}};
    spec.aggregate = AggregateSpec{0, 1, true, true};
    spec.order_column = 0;
    spec.projection = ProjectionPlan{{2, 0, 1}};
    Bag reference;
    const auto schema = IntermediateSchema({{"sum", TypeId::Int64}, {"group", TypeId::Int64}, {"count", TypeId::Int64}});
    for (int64_t key = 0; key < 4; ++key) {
        int64_t sum = 0, count = 0;
        for (int64_t i = 20; i < 100; ++i) if (i % 5 == key) { sum += i + 1; ++count; }
        reference.push_back(EncodeTuple(schema, Tuple({Value(sum), Value(key), Value(count)})));
    }
    std::sort(reference.begin(), reference.end());
    {
        QueryContext q(db, {32768, 4096});
        auto p = ChoosePlan(q, spec);
        VDB_CHECK(p.plan.kind == PlanKind::Projection);
        VDB_CHECK(Find(p.plan, PlanKind::Filter));
        VDB_CHECK((p.root->OutputOrdering() == Ordering{true, 1}));
        VDB_CHECK(Run(q, p) == reference);
    }
    PlanOptions o;
    o.join = JoinChoice::GHJ;
    o.aggregate = AggregateChoice::HashAggregate;
    o.join_memory = 65536;
    o.aggregate_memory = 32768;
    o.sort_memory = 24576;
    std::string example;
    {
        QueryContext q(db, {1048576, 4096});
        auto p = ChoosePlan(q, spec, o);
        const auto* join = Find(p.plan, PlanKind::GHJ);
        const auto* aggregate = Find(p.plan, PlanKind::HashAggregate);
        const auto* sort = Find(p.plan, PlanKind::ExternalSort);
        VDB_CHECK(join && aggregate && sort);
        VDB_CHECK(join->grace_memory->max_bytes == 65536);
        VDB_CHECK(aggregate->aggregate_memory->max_bytes == 32768);
        VDB_CHECK(sort->sort_memory->max_bytes == 24576);
        VDB_CHECK(!join->ordering.ordered && !aggregate->ordering.ordered);
        VDB_CHECK(Find(join->children[0], PlanKind::Filter) && Find(join->children[1], PlanKind::Filter));
        example = Explain(p);
        for (const auto& text : {"Projection", "ExternalSort", "HashAggregate", "GHJ", "SeqScan", "employees", "departments",
                "estimated_rows=", "input_rows=", "schema=[", "local_cost=", "subtree_cost=", "order=", "M=65536", "M=32768",
                "M=24576", "C_sort=", "k=", "p=", "Dghj=", "build_capacities=", "group_capacity=", "F=8", "max_temp_pages=4096",
                "sampled", "uncertain", "runtime BNLJ", "per heavyweight operator", "not runtime measurements"})
            VDB_CHECK(example.find(text) != std::string::npos);
        VDB_CHECK(Run(q, p) == reference);
    }
    db.CreateIndex("employees", 0);
    db.Analyze("employees");
    VDB_CHECK(Execute(db, spec) == reference);
    o.left_scan = ScanChoice::IndexScan;
    VDB_CHECK(Execute(db, spec, o) == reference);
    std::cout << "EXPLAIN worked plan:\n" << example;
    // Deterministic ties: the one-row join ties NLJ/BNLJ; identical left/right paths prefer Left.
    Fill(db, "tie", 1);
    QueryContext q(db, {32768, 4096});
    auto first = ChoosePlan(q, Join("tie", "tie"));
    VDB_CHECK(first.plan.kind == PlanKind::NLJ && first.plan.join.outer == PhysicalOuter::Left);
    for (size_t i = 0; i < 10; ++i) {
        auto next = ChoosePlan(q, Join("tie", "tie"));
        VDB_CHECK(Explain(first) == Explain(next));
    }
    q.Close();
}
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::pair<std::string, std::function<void()>>> cases{
        {"statistics", Statistics}, {"selectivity", Selectivity}, {"scans", ScanChoices},
        {"ordering", OrderAndProjection}, {"joins", JoinChoices}, {"replay_costs", ReplayCosts},
        {"forced_joins", ForcedJoins}, {"grace_fallback", GraceFallback}, {"aggregates", AggregateChoices}, {"validation", GlobalAndValidation},
        {"memory_costs", MemoryAndCost}, {"pipeline_explain", PipelineAndExplain}};
    size_t passed = 0;
    try {
        for (const auto& [name, run] : cases) {
            if (argc > 1 && name.find(argv[1]) == std::string::npos) continue;
            std::cout << "RUN " << name << std::endl;
            run();
            ++passed;
            std::cout << "PASS " << name << std::endl;
        }
        VDB_CHECK(passed > 0);
        std::cout << "optimizer groups passed=" << passed << '\n';
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "optimizer failure: " << e.what() << '\n';
        return 1;
    }
}
