#pragma once

#include <atomic>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>

#include "shared_generated.h"

namespace nanokv {

class Server;

struct ValuePack {
  std::string           value;
  shared::ValueEncoding encoding;
  uint64_t              versionstamp;
};

struct EnqueueValuePack {
  std::string           value;
  shared::ValueEncoding encoding;
  uint64_t              schedule;
  uint64_t              sequence;
};

struct QueueSnapshot {
  uint64_t schedule;
  uint64_t sequence;

  void update(QueueSnapshot const &rhs) {
    if (rhs > *this) {
      *this = rhs;
    }
  }

  auto operator<=>(QueueSnapshot const &) const = default;
};

inline void operator<<=(std::atomic<QueueSnapshot> &lhs, QueueSnapshot const &rhs) {
  QueueSnapshot old;
  do {
    old = lhs.load();
    if (old > rhs) break;
  } while (!lhs.compare_exchange_weak(old, rhs));
}

using UpdateMap = std::map<std::string, std::optional<ValuePack>>;

using EnqueueMap = std::map<std::string, EnqueueValuePack>;

class QueueListener {
  class LazyMap {
    using map_t = std::map<std::string, std::atomic<QueueSnapshot>, std::less<>>;
    union {
      map_t map;
    };
    bool   inited = false;
    map_t &get() {
      if (!inited) {
        new (&map) map_t();
        inited = true;
      }
      return map;
    }

   public:
    inline LazyMap() {}
    inline LazyMap(LazyMap const &rhs) {}
    inline LazyMap(LazyMap &&rhs) {}
    inline auto &&operator[](auto index) { return get()[index]; }
    inline auto   begin() { return get().begin(); }
    inline auto   end() { return get().end(); }
    inline map_t &operator*() { return get(); }
    inline map_t *operator->() { return &get(); }
    inline ~LazyMap() {
      if (inited) {
        map.~map();
      }
    }
  };

 public:
  LazyMap tracking;
  inline virtual ~QueueListener()               = default;
  virtual Server *getServer()                   = 0;
  virtual void    notify(std::string_view data) = 0;
};

struct string_hash {
  using is_transparent        = void;
  using transparent_key_equal = std::equal_to<>;

  inline std::size_t operator()(std::convertible_to<std::string_view> auto const &s) const {
    return std::hash<std::string_view>{}(s);
  }
};

using QueueListenerMap = std::unordered_multimap<std::string, QueueListener *, string_hash, std::equal_to<>>;

}  // namespace nanokv
