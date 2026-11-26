#include "valencedb/optimizer/statistics.hpp"
#include "valencedb/storage/database.hpp"
#include <algorithm>
#include <cmath>

namespace valencedb {
namespace {
uint64_t SampleHash(uint64_t value) {
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}
}

void Database::Analyze(const std::string& name) {
    CheckMutable();
    const auto table = Table(name);
    TableStatistics stats;
    stats.columns.resize(table.schema.size());
    auto scan = Scan(name);
    while (auto row = scan.Next()) {
        ++stats.analyzed_rows;
        for (size_t c = 0; c < table.schema.size(); ++c) {
            if (table.schema.column(c).type != TypeId::Int64) continue;
            auto& s = stats.columns[c];
            const auto value = row->second.value(c).as_int64();
            s.minimum = s.minimum ? std::min(*s.minimum, value) : value;
            s.maximum = s.maximum ? std::max(*s.maximum, value) : value;
            if (s.sample.size() < kStatisticsSampleLimit) s.sample.push_back(value);
            else {
                const auto slot = SampleHash(stats.analyzed_rows + c * UINT64_C(0x9e3779b97f4a7c15))
                                  % stats.analyzed_rows;
                if (slot < kStatisticsSampleLimit) s.sample[slot] = value;
            }
        }
    }
    for (auto& s : stats.columns) {
        if (s.sample.empty()) continue;
        auto sorted = s.sample; // Bounded scratch; no table-wide distinct collection.
        std::sort(sorted.begin(), sorted.end());
        size_t distinct = 0, singletons = 0;
        for (size_t i = 0; i < sorted.size();) {
            size_t end = i + 1;
            while (end < sorted.size() && sorted[end] == sorted[i]) ++end;
            ++distinct;
            if (end == i + 1) ++singletons;
            i = end;
        }
        const long double unseen = static_cast<long double>(stats.analyzed_rows - sorted.size());
        s.approximate_ndv = std::clamp(distinct + singletons * unseen / sorted.size(),
                                      1.0L, static_cast<long double>(stats.analyzed_rows));
    }
    statistics_.insert_or_assign(name, std::move(stats));
}
const TableStatistics* Database::Statistics(const std::string& name) const {
    (void)Table(name);
    const auto found = statistics_.find(name);
    return found == statistics_.end() ? nullptr : &found->second;
}
void Database::StaleStatistics(const std::string& name) {
    const auto found = statistics_.find(name);
    if (found != statistics_.end()) found->second.stale = true;
}
long double EstimateNDV(const ColumnStatistics* stats, long double rows) {
    if (rows <= 0) return 0;
    if (rows < 1) return rows;
    return std::clamp(stats && stats->approximate_ndv > 0 ? stats->approximate_ndv : std::sqrt(rows),
                      1.0L, rows);
}
long double EstimateSelectivity(const ColumnStatistics* s, long double rows, const Int64Predicate& p) {
    if (rows <= 0) return 0;
    if (!s || !s->minimum || !s->maximum)
        return p.op == CompareOp::Equal ? 0.1L : 1.0L / 3;
    const int64_t low = *s->minimum, high = *s->maximum, value = p.constant;
    if (p.op == CompareOp::Equal)
        return value < low || value > high ? 0 : 1 / std::max(1.0L, EstimateNDV(s, rows));
    // Compare endpoints in INT64 first. Unsigned differences avoid signed overflow
    // and cancellation near INT64_MIN/MAX, including on double-width long double.
    const auto distance = [](int64_t a, int64_t b) -> long double {
        return static_cast<long double>(static_cast<uint64_t>(a) - static_cast<uint64_t>(b));
    };
    const long double domain = distance(high, low) + 1;
    switch (p.op) {
        case CompareOp::Less:
            if (value <= low) return 0;
            if (value > high) return 1;
            return std::clamp(distance(value, low) / domain, 0.0L, 1.0L);
        case CompareOp::LessEqual:
            if (value < low) return 0;
            if (value >= high) return 1;
            return std::clamp((distance(value, low) + 1) / domain, 0.0L, 1.0L);
        case CompareOp::Greater:
            if (value < low) return 1;
            if (value >= high) return 0;
            return std::clamp(distance(high, value) / domain, 0.0L, 1.0L);
        case CompareOp::GreaterEqual:
            if (value <= low) return 1;
            if (value > high) return 0;
            return std::clamp((distance(high, value) + 1) / domain, 0.0L, 1.0L);
        case CompareOp::Equal: break;
    }
    return 0;
}

}  // namespace valencedb
