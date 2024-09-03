#pragma once
#include <iterator>

namespace nanokv {

template <typename Fn>
struct FnIter {
  using iterator_category = std::output_iterator_tag;
  using value_type        = void;
  using difference_type   = std::ptrdiff_t;
  using pointer           = void;
  using reference         = void;

  inline FnIter(Fn const &fn) : fn{fn} {}
  inline FnIter(FnIter &&iter) : fn{std::move(iter.fn)} {}
  inline FnIter(FnIter const &iter) : fn{iter.fn} {}
  inline FnIter &operator=(FnIter &&iter) {
    fn = std::move(iter.fn);
    return *this;
  }
  inline FnIter &operator=(FnIter const &iter) {
    fn = iter.fn;
    return *this;
  }
  struct proxy {
    Fn fn;

    inline proxy const &operator=(auto const &v) const & {
      fn(v);
      return *this;
    }
  };
  inline proxy   operator*() { return {fn}; }
  inline FnIter &operator++() { return *this; }
  inline FnIter &operator++(int) { return *this; }

  inline difference_type operator-(FnIter const &) { return 0; }

 private:
  Fn fn;
};

template <typename Fn>
FnIter(Fn const &iter) -> FnIter<Fn>;

}  // namespace nanokv
