#pragma once
#include <flatbuffers/verifier.h>

#include <string_view>

namespace nanokv {
template <typename T>
inline T const *parseBuffer(std::string_view buffer) {
  if (flatbuffers::Verifier((uint8_t const *)buffer.data(), buffer.size()).VerifyBuffer<T>()) {
    return flatbuffers::GetRoot<T>(buffer.data());
  } else {
    return nullptr;
  }
}
}  // namespace nanokv