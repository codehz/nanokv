#include "server.h"

#include <flatbuffers/verifier.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/spdlog.h>
#include <uwebsockets/App.h>

#include <algorithm>
#include <iterator>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>

#include "convert.h"
#include "fbextra.h"
#include "iter.h"
#include "kverror.h"
#include "schema_generated.h"
#include "storage.h"

namespace nanokv {

class WatchState {
  std::set<std::string, std::less<>> keys;

 public:
  inline void subscribe(std::string_view key) { keys.insert(std::string{key}); }
  inline void unsubscribe(std::string_view key) { keys.erase(keys.find(key)); }
  inline bool has(std::string_view key) { return keys.find(key) != keys.end(); }
  inline void intersection(UpdateMap const &rhs, std::output_iterator<UpdateMap::value_type> auto &&output) {
    constexpr auto take_str = [](auto &&v) {
      if constexpr (requires { v.first; })
        return v.first;
      else
        return v;
    };
    std::ranges::set_intersection(rhs, keys, output, std::less<>{}, take_str, take_str);
  }
};

template <bool SSL>
struct TemplatedPolyApp : PolyApp, uWS::TemplatedApp<SSL, TemplatedPolyApp<SSL>> {
  std::set<uWS::WebSocket<SSL, true, WatchState> *> watches;

  struct QueueState : QueueListener {
    uWS::WebSocket<SSL, true, QueueState> *ws;
    Server                                *server;

    inline void init(Server *server, uWS::WebSocket<SSL, true, QueueState> *ws) {
      this->server = server;
      this->ws     = ws;
    }

    inline bool subscribe(std::string_view key) {
      auto [it, ok] = tracking->emplace(std::piecewise_construct, std::forward_as_tuple(std::string(key)),
                                        std::forward_as_tuple(QueueSnapshot{0, 0}));
      return ok;
    }
    inline bool unsubscribe(std::string_view key) {
      auto it = tracking->find(key);
      if (it != tracking.end()) {
        tracking->erase(it);
        return true;
      } else {
        return false;
      }
    }
    inline bool    has(std::string_view key) { return tracking->find(key) != tracking.end(); }
    inline auto    begin() { return tracking.begin(); }
    inline auto    end() { return tracking.end(); }
    inline Server *getServer() { return server; }
    inline void    notify(std::string_view data) { ws->send(data); }
  };

  inline void dispatch_updates(std::shared_ptr<UpdateMap> const &updates) {
    using namespace flatbuffers;

    FlatBufferBuilder builder;
    for (auto &watch : this->watches) {
      std::vector<Offset<packet::KvEntry>> entries;
      watch->getUserData()->intersection(*updates, FnIter([&](UpdateMap::value_type const &pair) {
        if (pair.second) {
          entries.push_back(packet::CreateKvEntry(builder, CloneVector(builder, pair.first),
                                                  CloneVector(builder, pair.second->value), pair.second->encoding,
                                                  pair.second->version));
        } else {
          entries.push_back(packet::CreateKvEntry(builder, CloneVector(builder, pair.first)));
        }
      }));
      auto dbg = updates->begin()->first;
      if (!entries.empty()) {
        builder.Finish(packet::CreateWatchOutput(builder, 0, builder.CreateVector(entries)));
        watch->send(ToStringView(builder.Release()));
      }
    }
  }
  inline void shutdown() { this->close(); }
};

using TCPApp = TemplatedPolyApp<false>;
using SSLApp = TemplatedPolyApp<true>;

Server::Server(ServerCluster *cluster)
    : cluster{cluster}, thread{[this]() {
        loop = uWS::Loop::get();
        TCPApp app;
        initApp(this, app);
        this->app = &app;
        app.run();
        this->app = nullptr;
      }} {}

void Server::join() { thread.join(); }

void Server::defer(uWS::MoveOnlyFunction<void()> &&cb) { loop->defer(std::move(cb)); }

template <typename T>
inline void initApp(Server *server, T &app) {
  using QueueState = typename T::QueueState;
  app.post("/snapshot_read",
           [server](auto res, auto req) {
             res->onAborted([]() { spdlog::info("aborted"); })
                 ->onData([=, buffer = std::string{}](std::string_view data, bool isLast) mutable {
                   buffer.append(data);
                   if (isLast) {
                     auto packet = parseBuffer<packet::SnapshotRead>(buffer);
                     if (!packet) {
                       res->writeStatus("400 Bad Request")->end("Bad Request", true);
                     } else {
                       flatbuffers::FlatBufferBuilder builder;
                       try {
                         auto output = Storage::instance->snapshot_read(builder, packet);
                         builder.Finish(output);
                         auto response = builder.GetBufferSpan();
                         res->end(ToStringView(response));
                       } catch (std::runtime_error const &e) {
                         res->writeStatus("500 Internal Server Error")->end(e.what(), true);
                       }
                     }
                   }
                 });
           })
      .post("/atomic_write",
            [server](auto res, auto req) {
              res->onAborted([]() { spdlog::info("aborted"); })
                  ->onData([=, buffer = std::string{}](std::string_view data, bool isLast) mutable {
                    buffer.append(data);
                    if (isLast) {
                      auto packet = parseBuffer<packet::AtomicWrite>(buffer);
                      if (!packet) {
                        res->writeStatus("400 Bad Request")->end("Bad Request", true);
                      } else {
                        flatbuffers::FlatBufferBuilder builder;
                        auto                           updates  = std::make_shared<UpdateMap>();
                        auto                           enqueues = std::make_shared<EnqueueMap>();
                        bool                           ok       = false;
                        try {
                          auto output = Storage::instance->atomic_write(builder, packet, *updates, *enqueues);
                          builder.Finish(output);
                          auto response = builder.GetBufferSpan();
                          res->end(ToStringView(response));
                          ok = true;
                        } catch (std::runtime_error const &e) {
                          res->writeStatus("500 Internal Server Error")->end(e.what(), true);
                        }
                        if (ok) {
                          try {
                            if (updates->size() > 0) server->cluster->dispatch_updates(std::move(updates));
                            if (enqueues->size() > 0) server->cluster->dispatch_queues(std::move(enqueues));
                          } catch (std::runtime_error const &e) {
                            spdlog::error(e.what());
                          }
                        }
                      }
                    }
                  });
            })
      .template ws<QueueState>("/listen", {.maxPayloadLength         = 1024 * 1024,
                                           .idleTimeout              = 10,
                                           .maxBackpressure          = 1024 * 1024,
                                           .closeOnBackpressureLimit = true,
                                           .open = [=](auto *ws) { ws->getUserData()->init(server, ws); },
                                           .message =
                                               [=](auto *ws, std::string_view message, uWS::OpCode opCode) {
                                                 if (opCode != uWS::BINARY) {
                                                   ws->end(1007, "Invalid message type");
                                                   return;
                                                 }
                                                 auto request = parseBuffer<packet::Listen>(message);
                                                 if (!request) {
                                                   ws->end(1007, "Invalid message type");
                                                   return;
                                                 }
                                                 try {
                                                   std::lock_guard lock{server->cluster->queues_mutex};
                                                   auto           &queue = server->cluster->active_queues;
                                                   auto           &state = *ws->getUserData();
                                                   if (request->added() && request->added()->size() > 0) {
                                                     for (auto key : *request->added()) {
                                                       _CHECK_FIELD(key, key, ListenKey);
                                                       if (state.subscribe(ToStringView(*key->key()))) {
                                                         queue.emplace(ToStringView(*key->key()), &state);
                                                       }
                                                     }
                                                   }
                                                   if (request->removed() && request->removed()->size() > 0) {
                                                     for (auto key : *request->removed()) {
                                                       _CHECK_FIELD(key, key, ListenKey);
                                                       if (state.unsubscribe(ToStringView(*key->key()))) {
                                                         auto [it, end] = queue.equal_range(ToStringView(*key->key()));
                                                         for (; it != end; it++) {
                                                           if (it->second == &state) {
                                                             queue.erase(it);
                                                             break;
                                                           }
                                                         }
                                                       }
                                                     }
                                                   }
                                                 } catch (std::runtime_error const &e) {
                                                   ws->end(1007, e.what());
                                                   return;
                                                 }
                                                 if (request->added() && request->added()->size() > 0) {
                                                   try {
                                                     flatbuffers::FlatBufferBuilder builder;
                                                     auto offset = Storage::instance->listen_fetch(builder, request);
                                                     if (!offset.IsNull()) {
                                                       builder.Finish(offset);
                                                       auto response = builder.GetBufferSpan();
                                                       ws->send(ToStringView(response));
                                                     }
                                                   } catch (std::runtime_error const &e) {
                                                     ws->end(1007, e.what());
                                                     return;
                                                   }
                                                 }
                                               },
                                           .close =
                                               [=](auto *ws, int code, std::string_view message) {
                                                 std::lock_guard lock{server->cluster->queues_mutex};
                                                 auto           &queue = server->cluster->active_queues;
                                                 auto           &state = *ws->getUserData();
                                                 for (auto &key : state) {
                                                   auto [it, end] = queue.equal_range(key.first);
                                                   for (; it != end; it++) {
                                                     if (it->second == &state) {
                                                       queue.erase(it);
                                                       break;
                                                     }
                                                   }
                                                 }
                                               }})
      .template ws<WatchState>(
          "/watch", {.maxPayloadLength         = 1024 * 1024,
                     .idleTimeout              = 10,
                     .maxBackpressure          = 1024 * 1024,
                     .closeOnBackpressureLimit = true,
                     .open                     = [&app](auto *ws) { app.watches.insert(ws); },
                     .message =
                         [=](auto *ws, std::string_view message, uWS::OpCode opCode) {
                           if (opCode != uWS::BINARY) {
                             ws->end(1007, "Invalid message type");
                             return;
                           }
                           auto request = parseBuffer<packet::Watch>(message);
                           if (!request) {
                             ws->end(1007, "Invalid message type");
                             return;
                           }
                           try {
                             auto id = request->id();
                             _CHECK_FIELD(request, keys, Watch);
                             if (id) {
                               for (auto key : *request->keys()) {
                                 _CHECK_FIELD(key, key, WatchKey);
                                 ws->getUserData()->subscribe(ToStringView(*key->key()));
                               }
                               flatbuffers::FlatBufferBuilder builder;
                               builder.Finish(Storage::instance->watch_fetch(builder, request));
                               auto response = builder.GetBufferSpan();
                               ws->send(ToStringView(response));
                             } else {
                               for (auto key : *request->keys()) {
                                 _CHECK_FIELD(key, key, WatchKey);
                                 ws->getUserData()->unsubscribe(ToStringView(*key->key()));
                               }
                             }
                           } catch (std::runtime_error const &e) {
                             ws->end(1007, e.what());
                             return;
                           }
                         },
                     .close = [&app](auto *ws, int code, std::string_view message) { app.watches.erase(ws); }})
      .listen(2256, [](auto *listen_socket) {
        if (listen_socket) {
          spdlog::info("Listening on port {}", 2256);
        } else {
          spdlog::info("Failed to listen on port {}", 2256);
        }
      });
}

void Server::close() {
  loop->defer([this] {
    if (app) app->shutdown();
  });
}

ServerCluster::ServerCluster(size_t num_servers) {
  servers.reserve(num_servers);
  for (int i = 0; i < num_servers; i++) {
    servers.emplace_back(this);
  }
}
void ServerCluster::dispatch_updates(std::shared_ptr<UpdateMap> &&updates) {
  if (closed) return;
  for (auto &server : servers) {
    server.loop->defer([=, this, &server] {
      if (closed || !server.app) return;
      server.app->dispatch_updates(updates);
    });
  }
}

void ServerCluster::dispatch_queues(std::shared_ptr<EnqueueMap> &&enqueues) {
  if (closed) return;
  using EnqueueKV = std::pair<std::string_view, EnqueueValuePack *>;

  std::lock_guard<std::mutex>                                    guard(queues_mutex);
  std::multimap<Server *, std::pair<QueueListener *, EnqueueKV>> map;

  for (auto it = enqueues->begin(); it != enqueues->end(); ++it) {
    auto queue = active_queues.find(it->first);
    if (queue != active_queues.end()) {
      auto server = queue->second->getServer();
      map.emplace(server, std::make_pair(queue->second, std::make_pair(std::string_view(it->first), &it->second)));
    }
  }

  std::multimap<QueueListener *, EnqueueKV> packed;
  for (auto it = map.begin(); it != map.end();) {
    auto [start, end] = map.equal_range(it->first);
    std::transform(start, end, std::inserter(packed, packed.end()), [](auto it) { return it.second; });

    it->first->loop->defer([=, this, packed = std::move(packed)]() {
      using namespace flatbuffers;

      (void)enqueues;
      if (closed) return;
      FlatBufferBuilder                       builder;
      std::vector<Offset<packet::QueueEntry>> entries;

      for (auto it = packed.begin(); it != packed.end();) {
        auto [start, end] = packed.equal_range(it->first);
        auto listener     = it->first;
        std::transform(start, end, std::back_inserter(entries), [&](std::pair<QueueListener *, EnqueueKV> const &item) {
          auto &[key, pack] = item.second;
          listener->tracking->find(key)->second <<= {pack->schedule, pack->sequence};
          return packet::CreateQueueEntry(builder, CloneVector(builder, key), CloneVector(builder, pack->value),
                                          pack->encoding, pack->schedule, pack->sequence);
        });
        builder.Finish(packet::CreateListenOutput(builder, builder.CreateVector(entries)));
        listener->notify(ToStringView(builder.Release()));
        entries.clear();
        it = end;
      }
    });
    packed.clear();
    it = end;
  }
}

void ServerCluster::join() {
  for (auto &server : servers) {
    server.join();
  }
}

void ServerCluster::close() {
  closed = true;
  for (auto &server : servers) {
    server.close();
  }
}

}  // namespace nanokv