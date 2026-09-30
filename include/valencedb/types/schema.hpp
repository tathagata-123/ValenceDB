#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace valencedb {

enum class TypeId { Int64, Varchar, Double };
enum class SchemaKind { Base, Intermediate };

struct Column {
    std::string name;
    TypeId type;
    uint16_t varchar_limit = 0;
    bool nullable = false;
};

class Schema {
public:
    Schema(SchemaKind kind, std::vector<Column> columns);
    SchemaKind kind() const noexcept { return kind_; }
    size_t size() const noexcept { return columns_.size(); }
    const Column& column(size_t index) const;
    size_t max_encoded_bytes() const noexcept { return max_encoded_bytes_; }
    size_t encoded_limit() const noexcept;

private:
    SchemaKind kind_;
    std::vector<Column> columns_;
    size_t max_encoded_bytes_ = 4;
};

}  // namespace valencedb
