#pragma once
#include <leveldb/db.h>

#include <map>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "schema_generated.h"
#include "shared.h"

namespace nanokv {

class Core;

class Storage {
  std::mutex                   mutex;
  std::unique_ptr<leveldb::DB> db;
  uint64_t                     current_versionstamp(leveldb::ReadOptions &options);
  uint64_t                     increment_versionstamp(leveldb::ReadOptions &options, leveldb::WriteBatch &batch);
  Core                        *app;

 public:
  Storage(Core *app, char const *path);
  inline static Storage                          *instance = nullptr;
  flatbuffers::Offset<packet::WatchOutput>        watch_fetch(flatbuffers::FlatBufferBuilder &builder,
                                                              packet::Watch const            *watch);
  flatbuffers::Offset<packet::SnapshotReadOutput> snapshot_read(flatbuffers::FlatBufferBuilder &builder,
                                                                packet::SnapshotRead const     *read);
  flatbuffers::Offset<packet::AtomicWriteOutput>  atomic_write(flatbuffers::FlatBufferBuilder &builder,
                                                               packet::AtomicWrite const *write, UpdateMap &updates,
                                                               EnqueueMap &enqueues);
  flatbuffers::Offset<packet::ListenOutput>       listen_fetch(flatbuffers::FlatBufferBuilder &builder,
                                                               packet::Listen const           *listen);
  void                                            cleanup_expired(UpdateMap &map, uint64_t &next);
  void                                            schedule_queues(QueueListenerMap &listeners, uint64_t &next);
  // flatbu
};

}  // namespace nanokv