#pragma once
#include <flatbuffers/detached_buffer.h>
#include <leveldb/db.h>
#include <uwebsockets/App.h>

#include <atomic>
#include <string>
#include <vector>

#include "shared.h"

namespace nanokv {

struct ServerOptions final {
  struct SSLOptions {
    char const *key;
    char const *cert;
    char const *passphrase;
    char const *ciphers;
  };
  uint16_t                  port;
  std::optional<SSLOptions> ssl;
};

struct ClusterOptions final {
  ServerOptions server;
  size_t        num_servers;
};

class ServerCluster;
class Server;

struct PolyApp {
  inline virtual ~PolyApp()                                                = default;
  virtual void dispatch_updates(std::shared_ptr<UpdateMap> const &updates) = 0;
  virtual void shutdown()                                                  = 0;
};

template <typename T>
inline void initApp(Server *server, T &app, uint16_t port);

class Server {
 protected:
  ServerCluster *cluster;
  PolyApp       *app  = nullptr;
  uWS::Loop     *loop = nullptr;
  std::thread    thread;

 public:
  Server(ServerCluster *cluster, ServerOptions const &opts);
  void close();
  void join();
  void defer(uWS::MoveOnlyFunction<void()> &&cb);

  template <typename T>
  friend void initApp(Server *server, T &app, uint16_t port);
  friend class ServerCluster;
};

class ServerCluster {
  std::atomic<bool>   closed = false;
  std::vector<Server> servers;

 public:
  std::mutex       queues_mutex;
  QueueListenerMap active_queues;

  ServerCluster(ClusterOptions const &);
  void dispatch_updates(std::shared_ptr<UpdateMap> &&updates);
  void dispatch_queues(std::shared_ptr<EnqueueMap> &&enqueues);
  void join();
  void close();

  std::vector<std::string> get_active_queue_keys() const;
};

}  // namespace nanokv