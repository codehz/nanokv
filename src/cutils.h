#pragma once

#include <concepts>
#include <cstring>
#include <type_traits>

template <typename T, void (*deleter)(T *loop)>
struct craii {
  T *ptr;

  craii(T *ptr) : ptr{ptr} {}
  ~craii() { deleter(ptr); }

  operator T *() { return ptr; }
};

struct cext {
  void *target;
  cext(void *target) : target(target) {}
  template <typename T>
  void operator=(T &&value) {
    std::memcpy(target, &value, sizeof(T));
  }
  template <typename T>
  operator T() {
    T value;
    std::memcpy(&value, target, sizeof(T));
    return value;
  }
};

namespace cdelegate_details {

template <typename Fn, typename T>
concept converter = requires(Fn fn, T value) {
  { fn(value) } -> std::same_as<void *>;
};

template <typename Sig>
struct converter_input;

template <typename T>
struct converter_input<void *(*)(T)> {
  using type = T;
};

template <typename Sig>
using converter_input_t = typename converter_input<Sig>::type;

template <typename Context, typename Input, converter<Input *> auto converter>
class cdelegate_impl {
  template <typename... Args>
  struct extract_context;

  template <typename First, typename... Rest>
  struct extract_context<First, Rest...> {
    inline static Context &extract(First first, Rest... args) {
      if constexpr (std::same_as<First, Input *>) {
        return *(Context *)converter(first);
      } else {
        return extract_context<Rest...>::extract(args...);
      }
    }
  };

 public:
  template <auto fn>
  static auto func(auto... args) {
    auto ctx = extract_context<decltype(args)...>::extract(args...);
    return fn(ctx, std::move(args)...);
  }

  template <auto fn>
  static auto method(auto... args) {
    auto ctx = extract_context<decltype(args)...>::extract(args...);
    if constexpr (std::is_pointer_v<decltype(ctx)>) {
      return (ctx->*fn)(std::move(args)...);
    } else {
      return (ctx.*fn)(std::move(args)...);
    }
  }
};

template <typename Context, auto converter>
using cdelegate = cdelegate_impl<Context, std::remove_pointer_t<converter_input_t<decltype(converter)>>, converter>;

}  // namespace cdelegate_details

using cdelegate_details::cdelegate;
