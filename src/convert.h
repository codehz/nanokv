#pragma once
#include <flatbuffers/flatbuffers.h>
#include <leveldb/db.h>

#include <bit>
#include <concepts>
#include <string_view>

#include "ulid/ulid.h"

namespace nanokv {

template <typename T, typename R>
concept SliceLike = requires(T slice) {
  { *slice.data() } -> std::convertible_to<R>;
  sizeof(*slice.data()) == sizeof(R);
  { slice.size() } -> std::convertible_to<size_t>;
};

inline static leveldb::Slice ToSlice(SliceLike<char> auto const &slice) {
  return {(char const *)slice.data(), slice.size()};
}
inline static std::string ToString(SliceLike<char> auto const &slice) {
  return {(char const *)slice.data(), slice.size()};
}
inline static std::string_view ToStringView(SliceLike<char> auto const &slice) {
  return {(char const *)slice.data(), slice.size()};
}

inline static flatbuffers::Offset<flatbuffers::Vector<uint8_t>> CloneVector(flatbuffers::FlatBufferBuilder &builder,
                                                                            SliceLike<uint8_t> auto const  &slice) {
  return builder.CreateVector((uint8_t const *)slice.data(), slice.size());
}

inline static constexpr auto big_endian(std::integral auto value) {
  if constexpr (std::endian::native == std::endian::little)
    return std::byteswap(value);
  else
    return value;
}

template <typename T>
concept is_enum = std::is_enum_v<T>;

class OwnedSlice {
  uint8_t *ptr;
  size_t   len;

 public:
  struct ByteProxy {
    uint8_t *ptr;
    size_t   pos;

    template <typename T>
    inline constexpr size_t operator=(T const &value) {
      if constexpr (std::integral<T>) {
        auto converted = big_endian(value);
        std::memcpy(ptr + pos, &converted, sizeof converted);
        return pos + sizeof converted;
      } else if constexpr (is_enum<T>) {
        auto underlying = static_cast<std::underlying_type_t<T>>(value);
        return this->operator=(underlying);
      } else if constexpr (std::same_as<T, ulid::ULID>) {
        ulid::MarshalBinaryTo(value, ptr + pos);
        return pos + 16;
      } else if constexpr (SliceLike<T, uint8_t>) {
        std::memcpy(ptr + pos, value.data(), value.size());
        return pos + value.size();
      } else {
        std::memcpy(ptr + pos, &value, sizeof value);
        return pos + sizeof value;
      }
    }
    inline constexpr ByteProxy operator<<(auto const &value) { return {ptr, this->operator=(value)}; }
  };
  struct ArrayProxy {
    uint8_t *ptr;
    size_t   len;

    inline constexpr ByteProxy operator[](size_t pos) { return {ptr, pos}; }
    inline constexpr uint8_t  *operator+(ptrdiff_t off) { return ptr + off; }
    inline constexpr           operator uint8_t *() { return ptr; }
  };
  inline constexpr OwnedSlice(size_t len, std::invocable<ArrayProxy> auto fn) : ptr(new uint8_t[len]), len(len) {
    std::invoke(fn, ArrayProxy{ptr, len});
  }
  inline constexpr OwnedSlice(OwnedSlice &&other) : ptr(other.ptr), len(other.len) { other.ptr = nullptr; }
  OwnedSlice(OwnedSlice const &) = delete;
  inline constexpr OwnedSlice &operator=(OwnedSlice &&other) {
    delete[] ptr;
    ptr       = other.ptr;
    len       = other.len;
    other.ptr = nullptr;
    return *this;
  }
  inline constexpr ~OwnedSlice() { delete[] ptr; }

  operator leveldb::Slice() const { return {(char *)ptr, len}; }
  operator std::string_view() const { return {(char *)ptr, len}; }
};

struct alloc_slice {
  size_t len;
  alloc_slice(size_t len) : len(len) {}
  auto operator->*(std::invocable<OwnedSlice::ArrayProxy> auto fn) { return OwnedSlice(len, fn); }
};

}  // namespace nanokv
