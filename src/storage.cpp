#include "storage.h"

#include <leveldb/comparator.h>
#include <leveldb/write_batch.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/spdlog.h>

#include <bit>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "convert.h"
#include "core.h"
#include "fbextra.h"
#include "internal_generated.h"
#include "kverror.h"
#include "now.h"

namespace nanokv {
using flatbuffers::FlatBufferBuilder;
using flatbuffers::Offset;
using flatbuffers::Vector;

enum struct SpecialKey : uint8_t {
  EXPIRES = 1,
  QUEUE   = 2,
};

inline static leveldb::Slice EXPIRES_BASE_KEY = {"\0\1", 2};
inline static leveldb::Slice QUEUE_BASE_KEY   = {"\0\2", 2};

inline static constexpr OwnedSlice createExpiresKey(uint64_t timestamp, SliceLike<char> auto const &slice) {
  return alloc_slice(slice.size() + 10)->*[&](auto data) noexcept {
    data[0] = 0;
    data[1] = SpecialKey::EXPIRES;
    data[2] << timestamp << slice;
  };
}

inline static constexpr uint64_t readExpiresKey(SliceLike<uint8_t> auto slice) noexcept {
  uint64_t timestamp;
  std::memcpy(&timestamp, slice.data() + 2, sizeof timestamp);
  return big_endian(timestamp);
}

inline static constexpr OwnedSlice createQueueKey(uint64_t schedule, uint64_t sequence,
                                                  SliceLike<char> auto const &slice) {
  return alloc_slice(20 + slice.size())->*[&](auto data) noexcept {
    data[0] = 0;
    data[1] = SpecialKey::QUEUE;
    data[2] = (uint16_t)slice.size();
    data[4] << slice << schedule << sequence;
  };
}

inline static constexpr std::tuple<std::string_view, uint64_t, uint64_t> readQueueKey(
    SliceLike<uint8_t> auto slice) noexcept {
  uint64_t schedule, sequence;
  std::memcpy(&schedule, slice.data() + slice.size() - 16, sizeof schedule);
  std::memcpy(&sequence, slice.data() + slice.size() - 8, sizeof sequence);
  return {{(char const *)slice.data() + 4, slice.size() - 20}, big_endian(schedule), big_endian(sequence)};
}

inline static constexpr OwnedSlice createQueueSequenceKey(SliceLike<char> auto const &slice) {
  return alloc_slice(4 + slice.size())->*[&](auto data) noexcept {
    data[0] = 0;
    data[1] = SpecialKey::QUEUE;
    data[2] = (uint16_t)slice.size();
    data[4] << slice;
  };
}

inline static constexpr uint64_t readQueueSequence(std::string_view buffer) {
  if (buffer.size() != 8) {
    return 0ull;
  }
  uint64_t sequence_id;
  std::memcpy(&sequence_id, buffer.data(), sizeof sequence_id);
  return big_endian(sequence_id);
}

inline static void encodeQueueSequence(uint64_t sequence_id, char *buffer) {
  uint64_t sequence_id_big_endian = big_endian(sequence_id);
  std::memcpy(buffer, &sequence_id_big_endian, sizeof sequence_id_big_endian);
}

inline static constexpr OwnedSlice createQueueScanKey(uint64_t timestamp, SliceLike<char> auto const &slice) {
  return alloc_slice(12 + slice.size())->*[&](auto data) noexcept {
    data[0] = 0;
    data[1] = SpecialKey::QUEUE;
    data[2] = (uint16_t)slice.size();
    data[4] << slice << timestamp;
  };
}

static leveldb::DB *open(char const *path) {
  if (Storage::instance) {
    throw std::runtime_error("Database already initialized");
  }
  leveldb::Options options;
  options.create_if_missing = true;
  options.reuse_logs        = true;
  leveldb::DB    *temp;
  leveldb::Status status = leveldb::DB::Open(options, path, &temp);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  return temp;
}

Storage::Storage(Core *app, char const *path) : db{open(path)}, app(app) { Storage::instance = this; }

struct SnapshotAutofree {
  leveldb::DB             *db;
  leveldb::Snapshot const *value;
  leveldb::ReadOptions     options;

  inline SnapshotAutofree(leveldb::DB *db) : db{db}, value{db->GetSnapshot()} { options.snapshot = value; }

  inline ~SnapshotAutofree() { db->ReleaseSnapshot(value); }
};

inline static leveldb::Slice KEY_VERSIONSTAMP;

inline static Offset<packet::KvEntry> CreateKvEntryFromValueHolder(FlatBufferBuilder           &builder,
                                                                   Offset<Vector<uint8_t>>      key,
                                                                   internal::ValueHolder const &holder) {
  std::optional<Offset<Vector<uint8_t>>> value_offset;

  auto data = holder.value();
  if (data && data->size() > 0) {
    value_offset = CloneVector(builder, *data);
  }
  packet::KvEntryBuilder entry{builder};
  entry.add_key(key);
  if (value_offset) {
    entry.add_value(*value_offset);
  }
  entry.add_encoding(holder.encoding());
  entry.add_version(holder.version());
  return entry.Finish();
}
inline static uint64_t       castVersionstamp(std::string_view str) { return *std::bit_cast<uint64_t *>(str.data()); }
inline static leveldb::Slice encodeVersionstamp(uint64_t &version) { return {(char const *)&version, 8}; }

uint64_t Storage::current_versionstamp(leveldb::ReadOptions &options) {
  std::string versionstamp(8, (char)0);
  auto        status = db->Get(options, KEY_VERSIONSTAMP, &versionstamp);
  if (!status.ok() && !status.IsNotFound()) {
    throw std::runtime_error(status.ToString());
  }
  return castVersionstamp(versionstamp);
}
uint64_t Storage::increment_versionstamp(leveldb::ReadOptions &options, leveldb::WriteBatch &batch) {
  static uint64_t temp = 0;
  std::string     versionstamp(8, (char)0);
  auto            status = db->Get(options, KEY_VERSIONSTAMP, &versionstamp);
  if (!status.ok() && !status.IsNotFound()) {
    throw std::runtime_error(status.ToString());
  }
  temp = castVersionstamp(versionstamp) + 1;
  batch.Put(KEY_VERSIONSTAMP, encodeVersionstamp(temp));
  return temp;
}

Offset<packet::WatchOutput> Storage::watch_fetch(FlatBufferBuilder &builder, packet::Watch const *watch) {
  auto                                 timestamp = now();
  leveldb::ReadOptions                 read_options;
  leveldb::Status                      status;
  SnapshotAutofree                     snapshot{db.get()};
  std::vector<Offset<packet::KvEntry>> values;
  std::string                          buffer;

  read_options.snapshot = snapshot.value;
  for (auto key : *watch->keys()) {
    status = db->Get(read_options, ToSlice(*key->key()), &buffer);
    if (status.ok()) {
      auto holder = flatbuffers::GetRoot<internal::ValueHolder>(buffer.data());
      if (holder->expired_at() > 0 && holder->expired_at() < timestamp) goto skip;
      values.push_back(CreateKvEntryFromValueHolder(builder, CloneVector(builder, *key->key()), *holder));
    } else {
    skip:
      values.push_back(packet::CreateKvEntry(builder, CloneVector(builder, *key->key())));
    }
  }
  return packet::CreateWatchOutput(builder, watch->id(), builder.CreateVector(values));
}

Offset<packet::SnapshotReadOutput> Storage::snapshot_read(FlatBufferBuilder          &builder,
                                                          packet::SnapshotRead const *read) {
  auto                                         timestamp = now();
  leveldb::ReadOptions                         read_options;
  leveldb::Status                              status;
  SnapshotAutofree                             snapshot{db.get()};
  std::vector<Offset<packet::ReadRangeOutput>> ranges;
  std::string                                  buffer;
  auto                                         comparator = leveldb::BytewiseComparator();

  read_options.snapshot = snapshot.value;
  _CHECK_FIELD(read, requests, SnapshotRead);
  for (auto request : *read->requests()) {
    if (request->exact()) {
      _CHECK_FIELD(request, start, ReadRange);
      auto key = ToSlice(*request->start());
      if (key[0] == 0) goto skip_exact;
      status = db->Get(read_options, key, &buffer);
      if (status.ok()) {
        auto holder = flatbuffers::GetRoot<internal::ValueHolder>(buffer.data());
        if (holder->expired_at() > 0 && holder->expired_at() < timestamp) goto skip_exact;
        ranges.push_back(packet::CreateReadRangeOutput(
            builder, builder.CreateVector(
                         {CreateKvEntryFromValueHolder(builder, CloneVector(builder, *request->start()), *holder)})));
      } else {
      skip_exact:
        ranges.push_back(packet::CreateReadRangeOutput(builder));
      }
    } else {
      std::unique_ptr<leveldb::Iterator>   iterator{db->NewIterator(read_options)};
      std::vector<Offset<packet::KvEntry>> values;
      _CHECK_FIELD(request, start, ReadRange);
      _CHECK_FIELD(request, end, ReadRange);
      auto start = ToSlice(*request->start());
      auto end   = ToSlice(*request->end());
      if (start[0] == 0) start = {"\1", 1};
      if (comparator->Compare(start, end) > 0) {
        ranges.push_back(packet::CreateReadRangeOutput(builder));
        continue;
      }
      if (request->reverse()) {
        iterator->Seek(end);
        if (!iterator->Valid()) {
          iterator->SeekToLast();
          if (!iterator->Valid()) goto skip_range;
        } else {
          iterator->Prev();
        }
        for (; iterator->Valid() && comparator->Compare(iterator->key(), start) >= 0; iterator->Prev()) {
          auto holder = flatbuffers::GetRoot<internal::ValueHolder>(iterator->value().data());
          if (holder->expired_at() > 0 && holder->expired_at() < timestamp) continue;
          values.push_back(CreateKvEntryFromValueHolder(builder, CloneVector(builder, iterator->key()), *holder));
          if (request->limit() > 0 && values.size() >= request->limit()) {
            break;
          }
        }
      } else {
        for (iterator->Seek(start); iterator->Valid() && comparator->Compare(iterator->key(), end) < 0;
             iterator->Next()) {
          auto holder = flatbuffers::GetRoot<internal::ValueHolder>(iterator->value().data());
          if (holder->expired_at() > 0 && holder->expired_at() < timestamp) continue;
          values.push_back(CreateKvEntryFromValueHolder(builder, CloneVector(builder, iterator->key()), *holder));
          if (request->limit() > 0 && values.size() >= request->limit()) {
            break;
          }
        }
      }
    skip_range:
      ranges.push_back(packet::CreateReadRangeOutput(builder, builder.CreateVector(values)));
    }
  }
  return packet::CreateSnapshotReadOutputDirect(builder, &ranges);
}

inline static flatbuffers::DetachedBuffer createValueHolder(Vector<uint8_t, flatbuffers::uoffset_t> const *input,
                                                            shared::ValueEncoding encoding, uint64_t version,
                                                            uint64_t expired_at) {
  FlatBufferBuilder builder;
  builder.Finish(internal::CreateValueHolder(builder, input && input->size() > 0 ? CloneVector(builder, *input) : 0,
                                             encoding, version, expired_at));
  return builder.Release();
}

inline static flatbuffers::DetachedBuffer createQueueValueHolder(Vector<uint8_t, flatbuffers::uoffset_t> const *input,
                                                                 shared::ValueEncoding encoding) {
  FlatBufferBuilder builder;
  builder.Finish(internal::CreateQueueValueHolder(
      builder, input && input->size() > 0 ? CloneVector(builder, *input) : 0, encoding));
  return builder.Release();
}

Offset<packet::AtomicWriteOutput> Storage::atomic_write(FlatBufferBuilder &builder, packet::AtomicWrite const *write,
                                                        UpdateMap &updates, EnqueueMap &enqueues) {
  auto                        timestamp = now();
  std::lock_guard<std::mutex> gurad(mutex);
  leveldb::Status             status;
  SnapshotAutofree            snapshot{db.get()};
  leveldb::WriteOptions       write_options;
  leveldb::WriteBatch         batch;
  auto                        versionstamp   = increment_versionstamp(snapshot.options, batch);
  uint64_t                    key_expires    = ~0ull;
  uint64_t                    queue_schedule = ~0ull;

  if (write->checks())
    for (auto check : *write->checks()) {
      _CHECK_FIELD(check, key, Check);
      auto key     = ToSlice(*check->key());
      auto version = check->version();
      if (key[0] == 0) goto check_fail;
      std::string buffer;
      status = db->Get(snapshot.options, key, &buffer);
      if (version == 0) {
        if (status.IsNotFound()) continue;
      } else {
        if (status.ok()) {
          auto actual = flatbuffers::GetRoot<internal::ValueHolder>(buffer.data())->version();
          if (actual == version) continue;
        }
        goto check_fail;
      }
    }
  if (write->dequeues())
    for (auto dequeue : *write->dequeues()) {
      _CHECK_FIELD(dequeue, key, Dequeue);
      auto        key = createQueueKey(dequeue->schedule(), dequeue->sequence(), *dequeue->key());
      std::string buffer;
      status = db->Get(snapshot.options, key, &buffer);
      if (status.IsNotFound()) goto check_fail;
      if (!status.ok()) throw std::runtime_error(status.ToString());
      batch.Delete(key);
    }
  if (write->mutations())
    for (auto mutation : *write->mutations()) {
      _CHECK_FIELD(mutation, key, Mutation);
      auto key = ToSlice(*mutation->key());
      if (key[0] == 0) goto check_fail;
      if (mutation->type() == packet::MutationType_DELETE) {
      early_delete:
        std::string buffer;
        status = db->Get(snapshot.options, key, &buffer);
        if (status.IsNotFound()) {
          continue;
        }
        auto holder = flatbuffers::GetRoot<internal::ValueHolder>(buffer.data());
        if (holder->expired_at()) batch.Delete(createExpiresKey(holder->expired_at(), key));
        batch.Delete(key);
        updates[ToString(key)] = std::nullopt;
      } else if (mutation->type() == packet::MutationType_SET) {
        _CHECK_FIELD(mutation, value, Mutation);
        if (mutation->expired_at() > 0) {
          if (timestamp > mutation->expired_at()) goto early_delete;
          batch.Put(createExpiresKey(mutation->expired_at(), key), {});
          key_expires = std::min(key_expires, mutation->expired_at());
        }
        auto buffer = createValueHolder(mutation->value(), mutation->encoding(), versionstamp, mutation->expired_at());
        batch.Put(key, ToSlice(buffer));
        updates[ToString(key)] = {ToString(*mutation->value()), mutation->encoding(), versionstamp};
      }
    }
  if (write->enqueues())
    for (auto enqueue : *write->enqueues()) {
      _CHECK_FIELD(enqueue, key, Enqueue);
      _CHECK_FIELD(enqueue, value, Enqueue);
      auto        key    = ToSlice(*enqueue->key());
      auto        seqkey = createQueueSequenceKey(key);
      std::string seqbuf{};
      status        = db->Get(snapshot.options, seqkey, &seqbuf);
      auto sequence = status.ok() ? readQueueSequence(seqbuf) : 0;
      ++sequence;
      seqbuf.resize(sizeof sequence);
      encodeQueueSequence(sequence, seqbuf.data());
      batch.Put(seqkey, seqbuf);
      uint64_t schedule = enqueue->schedule();
      if (schedule < timestamp) schedule = timestamp;
      auto queuekey = createQueueKey(schedule, sequence, key);
      auto holder   = createQueueValueHolder(enqueue->value(), enqueue->encoding());
      batch.Put(queuekey, ToSlice(holder));
      if (schedule == timestamp)
        enqueues[ToString(key)] = {.value    = enqueue->value() ? ToString(*enqueue->value()) : "",
                                   .encoding = enqueue->encoding(),
                                   .schedule = schedule,
                                   .sequence = sequence};
      else
        queue_schedule = std::min(queue_schedule, schedule);
    }
  status = db->Write(write_options, &batch);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  if (key_expires != ~0ull) {
    app->reset_key_expires_timer(key_expires);
  }
  if (queue_schedule != ~0ull) {
    app->reset_queue_timer(queue_schedule);
  }
  return packet::CreateAtomicWriteOutput(builder, true, versionstamp);
check_fail:
  return packet::CreateAtomicWriteOutput(builder);
}

Offset<packet::ListenOutput> Storage::listen_fetch(FlatBufferBuilder &builder, packet::Listen const *listen) {
  auto                                    timestamp = now();
  leveldb::Status                         status;
  SnapshotAutofree                        snapshot{db.get()};
  std::unique_ptr<leveldb::Iterator>      it{db->NewIterator(snapshot.options)};
  std::vector<Offset<packet::QueueEntry>> values;

  for (auto target : *listen->added()) {
    auto first = createQueueScanKey(0, *target->key());
    auto last  = createQueueKey(timestamp, ~0ull, *target->key());
    for (it->Seek(first); it->Valid() && it->key().compare(last) < 0; it->Next()) {
      auto [key, schedule, sequence] = readQueueKey(it->key());
      auto holder                    = flatbuffers::GetRoot<internal::QueueValueHolder>(it->value().data());
      values.push_back(packet::CreateQueueEntry(builder, CloneVector(builder, key),
                                                CloneVector(builder, *holder->value()), holder->encoding(), schedule,
                                                sequence));
    }
  }

  if (values.empty()) return 0;
  return packet::CreateListenOutput(builder, builder.CreateVector(values));
}

void Storage::cleanup_expired(UpdateMap &updates, uint64_t &next) {
  auto                               timestamp = now();
  std::lock_guard<std::mutex>        gurad(mutex);
  leveldb::Status                    status;
  SnapshotAutofree                   snapshot{db.get()};
  leveldb::WriteOptions              write_options;
  leveldb::WriteBatch                batch;
  std::unique_ptr<leveldb::Iterator> it{db->NewIterator(snapshot.options)};
  int                                count = 0;

  for (it->Seek(EXPIRES_BASE_KEY); it->Valid() && it->key().starts_with(EXPIRES_BASE_KEY); it->Next()) {
    if (it->key().size() <= 10) {
      batch.Delete(it->key());
      count++;
      continue;
    }
    uint64_t expired_at = readExpiresKey(it->key());
    if (expired_at <= timestamp) {
      batch.Delete(it->key());
      leveldb::Slice target{it->key().data() + 10, it->key().size() - 10};
      batch.Delete(target);
      updates[ToString(target)] = std::nullopt;
      count++;
    } else {
      next = std::min(next, expired_at);
      break;
    }
  }
  if (!count) return;
  increment_versionstamp(snapshot.options, batch);
  status = db->Write(write_options, &batch);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  return;
}

void Storage::schedule_queues(QueueListenerMap &listeners, uint64_t &next) {
  auto                               timestamp = now();
  leveldb::Status                    status;
  SnapshotAutofree                   snapshot{db.get()};
  std::unique_ptr<leveldb::Iterator> db_iterator{db->NewIterator(snapshot.options)};

  for (auto listener_iterator = listeners.begin(); listener_iterator != listeners.end();) {
    FlatBufferBuilder builder;
    auto [start, end]     = listeners.equal_range(listener_iterator->first);
    auto &[key, listener] = *listener_iterator;
    auto &ref             = listener->tracking[key];
    auto  snapshot        = ref.load();

    std::vector<Offset<packet::QueueEntry>> entries;

    auto first  = createQueueKey(snapshot.schedule, snapshot.sequence, key);
    auto middle = createQueueKey(timestamp, ~0ull, key);
    auto last   = createQueueKey(~0ull, ~0ull, key);

    db_iterator->Seek(first);
    for (; db_iterator->Valid() && db_iterator->key().compare(last) < 0; db_iterator->Next()) {
      if (db_iterator->key().compare(first) == 0) continue;
      auto [key, schedule, sequence] = readQueueKey(db_iterator->key());
      if (db_iterator->key().compare(middle) > 0) {
        next = std::min(next, schedule);
        break;
      }
      auto holder = flatbuffers::GetRoot<internal::QueueValueHolder>(db_iterator->value().data());

      snapshot.update({.schedule = schedule, .sequence = sequence});
      entries.push_back(packet::CreateQueueEntry(builder, CloneVector(builder, key),
                                                 CloneVector(builder, *holder->value()), holder->encoding(), schedule,
                                                 sequence));
    }

    if (!entries.empty()) {
      builder.Finish(packet::CreateListenOutput(builder, builder.CreateVector(entries)));
      listener->getServer()->defer(
          [listener = listener, buffer = builder.Release()] { listener->notify(ToStringView(buffer)); });
      ref <<= snapshot;
    }
    listener_iterator = end;
  }
}

}  // namespace nanokv