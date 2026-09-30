#pragma once

#include <stdexcept>

namespace valencedb {

class DbError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

}  // namespace valencedb
