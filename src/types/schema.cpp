#include "valencedb/types/schema.hpp"
#include "valencedb/common/error.hpp"

#include <utility>

namespace valencedb {

Schema::Schema(SchemaKind kind, std::vector<Column> columns)
    : kind_(kind), columns_(std::move(columns)) {
    const size_t max_columns = kind == SchemaKind::Base ? 8 : 16;
    if (columns_.empty() || columns_.size() > max_columns)
        throw DbError("schema column count is outside its allowed range");

    for (const Column& column : columns_) {
        if (column.name.empty()) throw DbError("schema column name is empty");
        if (kind == SchemaKind::Base && (column.type == TypeId::Double || column.nullable))
            throw DbError("base schema cannot contain DOUBLE or nullable columns");
        switch (column.type) {
            case TypeId::Int64:
            case TypeId::Double:
                if (column.varchar_limit != 0)
                    throw DbError("non-VARCHAR column has a VARCHAR limit");
                max_encoded_bytes_ += 8;
                break;
            case TypeId::Varchar:
                if (column.varchar_limit == 0 || column.varchar_limit > 256)
                    throw DbError("VARCHAR limit must be between 1 and 256 bytes");
                max_encoded_bytes_ += 2 + column.varchar_limit;
                break;
            default:
                throw DbError("unknown schema column type");
        }
    }
    if (max_encoded_bytes_ > encoded_limit())
        throw DbError("schema maximum encoded tuple exceeds its size limit");
}

const Column& Schema::column(size_t index) const {
    if (index >= columns_.size()) throw DbError("schema column index out of range");
    return columns_[index];
}

size_t Schema::encoded_limit() const noexcept {
    return kind_ == SchemaKind::Base ? 1024 : 2048;
}

}  // namespace valencedb
