#include "core.h"

#include <spdlog/spdlog.h>

#include <thread>

#include "now.h"

namespace nanokv {

ClusterOptions const &CoreOptions::init_cluster() & {
  cluster.server.port = port.value_or(2256);
  if (cert && key) {
    cluster.server.ssl = nanokv::ServerOptions::SSLOptions{
      .key        = key->c_str(),
      .cert       = cert->c_str(),
      .passphrase = passphrase ? passphrase->c_str() : nullptr,
      .ciphers    = ssl_ciphers ? ssl_ciphers->c_str() : nullptr,
    };
  }
  cluster.num_servers = std::max(1u, threads.value_or(std::thread::hardware_concurrency()) - 1);
  return cluster;
}

using CoreDelegate  = cdelegate<Core *, us_loop_ext>;
using TimerDelegate = cdelegate<Core *, us_timer_ext>;

inline static constexpr uint64_t max_sleep = 1000 * 30ull;

CoreTimer::CoreTimer(Core *core, void (*cb)(us_timer_t *t))
    : timer(us_create_timer(core->loop, 0, sizeof(Core *))), cb(cb) {
  cext(us_timer_ext(timer)) = core;
}

void CoreTimer::schedule(uint64_t target) {
  auto timestamp = now();
  if (target >= next && next > timestamp) return;
  if (target < timestamp) {
    us_timer_set(timer, cb, 0, max_sleep);
    next = ~0ull;
  }
  auto wait = std::min(max_sleep, target - timestamp);
  if (timestamp + wait == next) return;
  us_timer_set(timer, cb, wait, max_sleep);
  next = timestamp + wait;
}

bool CoreTimer::need_schedule(uint64_t target) {
  auto snapshot = next.load();
  return snapshot > target;
}

void CoreTimer::close() { us_timer_close(timer); }

Core::Core(CoreOptions &opts)
    : loop(us_create_loop(
          nullptr, CoreDelegate::method<&Core::wakeup_callback>, [](auto) {}, [](auto) {}, sizeof(Core *))),
      storage(this, opts.db_path.c_str()),
      cluster(opts.init_cluster()),
      key_expires_timer(this, TimerDelegate::method<&Core::cleanup_expired_keys>),
      queue_timer(this, TimerDelegate::method<&Core::check_queues>) {
  cext(us_loop_ext(loop)) = this;
}

void Core::defer(uWS::MoveOnlyFunction<void()> &&cb) {
  {
    std::lock_guard lock{mutex};
    defers.push_back(std::move(cb));
  }
  us_wakeup_loop(loop);
}

void Core::wakeup_callback(us_loop_t *loop) {
  std::vector<uWS::MoveOnlyFunction<void()>> temp;
  {
    std::lock_guard lock{mutex};
    std::swap(temp, defers);
  }

  for (auto &cb : temp) {
    cb();
  }
}

void Core::cleanup_expired_keys(us_timer_t *timer) {
  auto     map  = std::make_shared<nanokv::UpdateMap>();
  uint64_t next = ~0ull;
  storage.cleanup_expired(*map, next);
  key_expires_timer.schedule(next);
  if (!map->empty()) {
    spdlog::info("Drop {} expired entries", map->size());
    cluster.dispatch_updates(std::move(map));
  }
}

void Core::check_queues(us_timer_t *timer) {
  uint64_t next = ~0ull;
  storage.schedule_queues(cluster.active_queues, next);
  queue_timer.schedule(next);
}

void Core::run() {
  key_expires_timer.schedule(0);
  queue_timer.schedule(0);
  us_loop_run(loop);
  cluster.join();
}

void Core::stop() {
  cluster.close();
  key_expires_timer.close();
  queue_timer.close();
}

void Core::reset_key_expires_timer(uint64_t next) {
  if (!key_expires_timer.need_schedule(next)) return;
  defer([=, this] { key_expires_timer.schedule(next); });
}

void Core::reset_queue_timer(uint64_t next) {
  if (!queue_timer.need_schedule(next)) return;
  defer([=, this] { queue_timer.schedule(next); });
}

}  // namespace nanokv
