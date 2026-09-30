#include "valencedb/types/tuple.hpp"
#include "valencedb/common/error.hpp"
#include "valencedb/storage/page_codec.hpp"

#include <cstring>
#include <utility>

namespace valencedb {

Value::Value(TypeId type) : type_(type), data_(std::monostate{}) {}
Value::Value(int64_t value) : type_(TypeId::Int64), data_(value) {}
Value::Value(double value) : type_(TypeId::Double), data_(value) {}
Value::Value(std::string value) : type_(TypeId::Varchar), data_(std::move(value)) {}
Value Value::Null(TypeId type) { return Value(type); }

int64_t Value::as_int64() const {
    if (type_ != TypeId::Int64 || is_null()) throw DbError("value is not an INT64");
    return std::get<int64_t>(data_);
}

double Value::as_double() const {
    if (type_ != TypeId::Double || is_null()) throw DbError("value is not a DOUBLE");
    return std::get<double>(data_);
}

const std::string& Value::as_string() const {
    if (type_ != TypeId::Varchar || is_null()) throw DbError("value is not a VARCHAR");
    return std::get<std::string>(data_);
}

const Value& Tuple::value(size_t index) const {
    if (index >= values_.size()) throw DbError("tuple value index out of range");
    return values_[index];
}

std::vector<std::byte> EncodeTuple(const Schema& schema, const Tuple& tuple) {
    if (tuple.size() != schema.size()) throw DbError("tuple column count differs from schema");
    size_t size = 4;
    uint16_t nullmask = 0;
    for (size_t i = 0; i < schema.size(); ++i) {
        const Column& column = schema.column(i);
        const Value& value = tuple.value(i);
        if (value.type() != column.type) throw DbError("tuple value type differs from schema");
        if (value.is_null()) {
            if (!column.nullable || schema.kind() == SchemaKind::Base)
                throw DbError("NULL is not allowed in this column");
            nullmask |= uint16_t(1u << i);
            continue;
        }
        size_t field_size = 8;
        if (column.type == TypeId::Varchar) {
            const size_t length = value.as_string().size();
            if (length > column.varchar_limit) throw DbError("VARCHAR exceeds column limit");
            field_size = 2 + length;
        }
        if (field_size > schema.encoded_limit() - size)
            throw DbError("encoded tuple exceeds schema size limit");
        size += field_size;
    }
    std::vector<std::byte> bytes(size);
    WriteU16(bytes, 0, static_cast<uint16_t>(schema.size()));
    WriteU16(bytes, 2, nullmask);
    size_t offset = 4;
    for (size_t i = 0; i < schema.size(); ++i) {
        const Value& value = tuple.value(i);
        if (value.is_null()) continue;
        switch (value.type()) {
            case TypeId::Int64:
                WriteI64(bytes, offset, value.as_int64());
                offset += 8;
                break;
            case TypeId::Double:
                WriteDouble(bytes, offset, value.as_double());
                offset += 8;
                break;
            case TypeId::Varchar: {
                const std::string& string = value.as_string();
                WriteU16(bytes, offset, static_cast<uint16_t>(string.size()));
                offset += 2;
                if (!string.empty()) std::memcpy(bytes.data() + offset, string.data(), string.size());
                offset += string.size();
                break;
            }
        }
    }
    return bytes;
}

Tuple DecodeTuple(const Schema& schema, std::span<const std::byte> bytes) {
    if (bytes.size() > schema.encoded_limit()) throw DbError("encoded tuple exceeds size limit");
    if (ReadU16(bytes, 0) != schema.size()) throw DbError("encoded column count mismatch");
    const uint16_t nullmask = ReadU16(bytes, 2);
    if ((uint32_t(nullmask) >> schema.size()) != 0)
        throw DbError("tuple null mask has out-of-range bits");
    std::vector<Value> values;
    values.reserve(schema.size());
    size_t offset = 4;
    for (size_t i = 0; i < schema.size(); ++i) {
        const Column& column = schema.column(i);
        if ((nullmask & (1u << i)) != 0) {
            if (!column.nullable || schema.kind() == SchemaKind::Base)
                throw DbError("encoded NULL is not allowed in this column");
            values.push_back(Value::Null(column.type));
            continue;
        }
        switch (column.type) {
            case TypeId::Int64:
                values.emplace_back(ReadI64(bytes, offset));
                offset += 8;
                break;
            case TypeId::Double:
                values.emplace_back(ReadDouble(bytes, offset));
                offset += 8;
                break;
            case TypeId::Varchar: {
                const size_t length = ReadU16(bytes, offset);
                offset += 2;
                if (length > column.varchar_limit || offset > bytes.size() ||
                    length > bytes.size() - offset)
                    throw DbError("encoded VARCHAR length is invalid");
                std::string string(length, '\0');
                if (length != 0) std::memcpy(string.data(), bytes.data() + offset, length);
                values.emplace_back(std::move(string));
                offset += length;
                break;
            }
        }
    }
    if (offset != bytes.size()) throw DbError("encoded tuple has trailing bytes");
    return Tuple(std::move(values));
}

}  // namespace valencedb
