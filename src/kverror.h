#pragma once
#include <stdexcept>

namespace nanokv {
using namespace std::string_literals;
struct TypeError : std::runtime_error {
  using std::runtime_error::runtime_error;
};

}  // namespace nanokv

#define _CHECK_FIELD(base, field, type) \
  if (!base->field()) throw ::nanokv::TypeError(#field " is not found in " #type)
