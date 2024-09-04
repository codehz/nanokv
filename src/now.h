#pragma once

#include <chrono>
#include <cstdint>

inline static uint64_t now() {
  auto value =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  return value < 0 ? 0 : (uint64_t)value;
}