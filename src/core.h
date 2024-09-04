#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cutils.h"
#include "libusockets.h"
#include "server.h"
#include "storage.h"

namespace nanokv {

class Core;

class CoreTimer {
  us_timer_t           *timer;
  std::mutex            mutex;
  std::atomic<uint64_t> next = ~0ull;
  void (*cb)(us_timer_t *t);

 public:
  CoreTimer(Core *, void (*cb)(us_timer_t *t));

  void schedule(uint64_t target);
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
  Core();

  void run();
  void stop();
  void reset_key_expires_timer(uint64_t next);
  void reset_queue_timer(uint64_t next);
  void defer(uWS::MoveOnlyFunction<void()> &&cb);

  friend class CoreTimer;
};

}  // namespace nanokv
