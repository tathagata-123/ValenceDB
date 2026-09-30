#pragma once

#include <stdexcept>
#include <string>

namespace valencedb::test {

class CheckFailure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

inline void Check(bool condition, const char* expression, const char* file, int line) {
    if (!condition) {
        throw CheckFailure(std::string(file) + ":" + std::to_string(line) +
                           ": check failed: " + expression);
    }
}

}  // namespace valencedb::test

#define VDB_CHECK(expression) \
    ::valencedb::test::Check(static_cast<bool>(expression), #expression, __FILE__, __LINE__)
