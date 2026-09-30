#pragma once

#include "valencedb/types/tuple.hpp"
#include <algorithm>
#include <random>
#include <stdexcept>

namespace valencedb::bench {

struct DatasetConfig {
    size_t rows = 240, domain = 60, payload = 64;
    uint64_t seed = 130013;
    std::string distribution = "uniform", measures = "mixed";
};

inline Schema DatasetSchema(size_t payload, bool count_star = false) {
    std::vector<Column> columns{{"key", TypeId::Int64}};
    if (!count_star) columns.push_back({"measure", TypeId::Int64});
    if (payload) columns.push_back({"payload", TypeId::Varchar, static_cast<uint16_t>(payload)});
    return Schema(SchemaKind::Base, std::move(columns));
}

// mt19937_64 has a specified sequence. Modulo and explicit Fisher-Yates avoid
// implementation-dependent uniform_int_distribution / shuffle mappings.
inline std::vector<Tuple> Dataset(const DatasetConfig& c, bool count_star = false) {
    if (!c.domain || c.payload > 256) throw std::runtime_error("invalid dataset width/domain");
    std::mt19937_64 random(c.seed);
    std::vector<int64_t> keys(c.rows);
    for (size_t i = 0; i < c.rows; ++i) {
        if (c.distribution == "unique" || c.distribution == "sequential") keys[i] = i;
        else if (c.distribution == "uniform") keys[i] = random() % c.domain;
        else if (c.distribution == "duplicate") keys[i] = i % c.domain;
        else if (c.distribution == "all_equal") keys[i] = 0;
        else if (c.distribution == "skewed") keys[i] = random() % 10 < 8 ? 0 : random() % c.domain;
        else if (c.distribution == "uneven") {
            // Two early key-0 rows, a full cycle of other groups, eight late
            // key-0 rows. At smoke M these cross unequal partial batches.
            if (c.rows < c.domain + 9) throw std::runtime_error("uneven needs domain+9 rows");
            keys[i] = i < 2 || i >= c.rows - 8 ? 0 : 1 + (i - 2) % (c.domain - 1);
        } else throw std::runtime_error("unknown distribution: " + c.distribution);
    }
    if (c.distribution == "unique" || c.distribution == "duplicate")
        for (size_t i = keys.size(); i > 1; --i) std::swap(keys[i - 1], keys[random() % i]);
    std::vector<Tuple> rows;
    rows.reserve(c.rows);
    for (size_t i = 0; i < c.rows; ++i) {
        std::vector<Value> values{Value(keys[i])};
        if (!count_star) {
            int64_t measure = 0;
            if (c.measures == "mixed") measure = static_cast<int64_t>(random() % 201) - 100;
            else if (c.measures == "positive") measure = 1 + random() % 100;
            else if (c.measures == "constant") measure = 7;
            else throw std::runtime_error("unknown measure distribution");
            if (c.distribution == "uneven" && keys[i] == 0)
                measure = i == 0 ? 10 : i == 1 ? 20 : 100;
            values.emplace_back(measure);
        }
        if (c.payload) {
            std::string payload(c.payload, '\0');
            for (size_t j = 0; j < payload.size(); ++j)
                payload[j] = static_cast<char>((i * 17 + j * 31 + c.seed) & 255);
            values.emplace_back(std::move(payload));
        }
        rows.emplace_back(std::move(values));
    }
    return rows;
}

} // namespace valencedb::bench
