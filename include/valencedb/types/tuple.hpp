#pragma once

#include "valencedb/types/schema.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace valencedb {

class Value {
public:
    Value(int64_t value);
    Value(double value);
    Value(std::string value);
    static Value Null(TypeId type);

    TypeId type() const noexcept { return type_; }
    bool is_null() const noexcept { return std::holds_alternative<std::monostate>(data_); }
    int64_t as_int64() const;
    double as_double() const;
    const std::string& as_string() const;

private:
    Value(TypeId type);
    TypeId type_;
    std::variant<std::monostate, int64_t, double, std::string> data_;
};

class Tuple {
public:
    explicit Tuple(std::vector<Value> values) : values_(std::move(values)) {}
    size_t size() const noexcept { return values_.size(); }
    const Value& value(size_t index) const;

private:
    std::vector<Value> values_;
};

std::vector<std::byte> EncodeTuple(const Schema& schema, const Tuple& tuple);
Tuple DecodeTuple(const Schema& schema, std::span<const std::byte> bytes);

}  // namespace valencedb
