#pragma once

#include <string_view>

namespace valencedb {

// Stable project identifier exposed by the core library.
std::string_view ProjectName() noexcept;

}  // namespace valencedb
