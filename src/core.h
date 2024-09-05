#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

#include "cutils.h"
#include "libusockets.h"
#include "server.h"
#include "storage.h"

namespace nanokv {

struct CoreOptions final {
  std::string                db_path;
  std::optional<uint32_t>    port;
  std::optional<uint32_t>    threads;
  std::optional<std::string> cert;
  std::optional<std::string> key;
  std::optional<std::string> passphrase;
  std::optional<std::string> ssl_ciphers;

  ClusterOptions cluster;

  ClusterOptions const &init_cluster() &;
};

class Core;

class CoreTimer {
  us_timer_t           *timer;
  std::mutex            mutex;
  std::atomic<uint64_t> next = ~0ull;
  void (*cb)(us_timer_t *t);

 public:
  CoreTimer(Core *, void (*cb)(us_timer_t *t));

  bool schedule(uint64_t target);
  void invoke();
  bool need_schedule(uint64_t target);
  void close();
};

class Core {
  craii<us_loop_t, us_loop_free>             loop;
  nanokv::Storage                            storage;
  nanokv::ServerCluster                      cluster;
  CoreTimer                                  key_expires_timer;
  CoreTimer                                  queue_timer;
  std::mutex                                 mutex;
  std::vector<uWS::MoveOnlyFunction<void()>> defers;

  void wakeup_callback(us_loop_t *);
  void cleanup_expired_keys(us_timer_t *);
  void check_queues(us_timer_t *);

 public:
  Core(CoreOptions &opts);

  void run();
  void stop();
  void reset_key_expires_timer(uint64_t next);
  void reset_queue_timer(uint64_t next);
  void defer(uWS::MoveOnlyFunction<void()> &&cb);

  friend class CoreTimer;
};

}  // namespace nanokv
