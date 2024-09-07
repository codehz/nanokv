import { randomBytes } from "node:crypto";
import {
  type AtomicOperation,
  AtomicOperationImpl,
  AtomicOperationProxy,
} from "./atomic";
import { packKey, unpackKey } from "./kv_key";
import { MutationType } from "./packet";
import {
  Protocol,
  type ProtocolEncoding,
  type RawCheck,
  type RawDequeue,
  type RawEnqueue,
  type RawKvEntry,
  type RawMutation,
  type RawReadRange,
} from "./protocol";
import { Reactor } from "./reactor";
import type {
  DistributiveProp,
  KvKeyPrefix,
  KvKeyToObject,
  RemoveKvPairPrefix,
  SelectKvPair,
  SelectKvPairByPrefix,
  ValueFotKvPair,
} from "./type_helpers";
import type {
  KvCommitError,
  KvCommitResult,
  KvEntry,
  KvEntryMaybe,
  KvKey,
  KvListOptions,
  KvListSelector,
  KvPair,
  KvQueueEntry,
} from "./types";
import { WebSocketConnection } from "./ws";
export { type ProtocolEncoding } from "./protocol";

export type { AtomicOperation } from "./atomic";
export * from "./types";

const START = new Uint8Array([0x00]);
const END = new Uint8Array([0xff]);

export type KvPairNotFound<X extends KvPair> = {
  key: X["key"];
  value: undefined;
  versionstamp: undefined;
};
export type KvPairMaybe<X extends KvPair> = X | KvPairNotFound<X>;

class WatchState {
  entry: KvEntryMaybe;
  #reactors: Reactor<void>[] = [];
  constructor(key: KvKey, public packed: Uint8Array, reactor: Reactor<void>) {
    this.entry = { key, value: undefined, versionstamp: 0n };
    this.#reactors.push(reactor);
  }
  addReactor(reactor: Reactor<void>) {
    this.#reactors.push(reactor);
  }
  removeReactor(reactor: Reactor<void>) {
    this.#reactors = this.#reactors.filter((r) => r !== reactor);
    return this.#reactors.length === 0;
  }
  notify(value: KvEntryMaybe) {
    if (this.entry.versionstamp === value.versionstamp) return;
    this.entry = value;
    for (const reactor of this.#reactors) {
      reactor.continue();
    }
  }
}

class ListenState {
  localqueue: KvQueueEntry[] = [];
  reactor = new Reactor<void>();

  take() {
    return this.localqueue.splice(0);
  }

  enqueue(entry: KvQueueEntry) {
    this.localqueue.push(entry);
    this.reactor.continue();
  }
}

export interface KvApi<
  E extends KvEntry = KvEntry,
  Q extends KvQueueEntry = KvQueueEntry
> {
  /**
   * Retrieve the value and versionstamp for the given key from the database in the form of a {@link KvEntryMaybe}.
   * If no value exists for the key, the returned entry will have a null value and versionstamp.
   */
  get<K extends E["key"]>(key: K): Promise<KvPairMaybe<SelectKvPair<E, K>>>;
  /**
   * Retrieve multiple values and versionstamp from the database in the form of an array of {@link KvEntryMaybe} objects.
   * The returned array will have the same length as the keys array, and the entries will be in the same order as the keys.
   * If no value exists for a given key, the returned entry will have a null value and versionstamp.
   */
  getMany<Ks extends E["key"][] | []>(
    keys: Ks
  ): Promise<{
    [N in keyof Ks]: KvPairMaybe<SelectKvPair<E, Ks[N]>>;
  }>;
  /**
   * Set the value for the given key in the database. If a value already exists for the key, it will be overwritten.
   *
   * Optionally an expireIn option can be specified to set a time-to-live (TTL) for the key.
   * The TTL is specified in milliseconds, and the key will be deleted from the database at earliest after the specified number of milliseconds have elapsed.
   * Once the specified duration has passed, the key may still be visible for some additional time.
   * If the expireIn option is not specified, the key will not expire.
   *
   * @param key - The key to set.
   * @param value - The value to set.
   * @param options - Options for the set operation.
   * @param options.expireIn - The number of milliseconds after which the key-value entry will expire.
   */
  set<K extends E["key"]>(
    key: K,
    value: ValueFotKvPair<E, K>,
    options?: { expireIn?: number }
  ): Promise<KvCommitResult | KvCommitError>;
  /**
   * Delete the value for the given key from the database.
   * If no value exists for the key, this operation is a no-op.
   *
   * @param key - The key to delete.
   * @return A promise resolving to an object containing a boolean indicating whether the operation was successful, and the versionstamp of the deleted key-value entry.
   */
  delete(key: E["key"]): Promise<KvCommitResult | KvCommitError>;
  /**
   * Retrieve a list of keys in the database.
   * The returned list is a ReadableStream which can be used to iterate over the entries in the database.
   * Each list operation must specify a selector which is used to specify the range of keys to return.
   * The selector can either be a prefix selector, or a range selector:
   * * A prefix selector selects all keys that start with the given prefix of key parts.
   *   For example, the selector ["users"] will select all keys that start with the prefix ["users"], such as ["users", "alice"] and ["users", "bob"].
   *   Note that you can not partially match a key part, so the selector ["users", "a"] will not match the key ["users", "alice"].
   *   A prefix selector may specify a start key that is used to skip over keys that are lexicographically less than the start key.
   * * A range selector selects all keys that are lexicographically between the given start and end keys
   *   (including the start, and excluding the end).
   *   For example, the selector ["users", "a"], ["users", "n"] will select all keys that start with the prefix ["users"] and have a second key part that is lexicographically between a and n,
   *   such as ["users", "alice"], ["users", "bob"], and ["users", "mike"], but not ["users", "noa"] or ["users", "zoe"].
   *
   * @see
   * The options argument can be used to specify additional options for the list operation.
   * See the documentation for {@link KvListOptions} for more information.
   */
  list<K extends E["key"], P extends KvKeyPrefix<K>>(
    selector: KvListSelector<K, P>,
    options?: KvListOptions
  ): ReadableStream<SelectKvPairByPrefix<E, P>>;
  /**
   * Create a new {@link AtomicOperation} object which can be used to perform an atomic transaction on the database.
   * This does not perform any operations on the database -
   * the atomic transaction must be committed explicitly using the {@link AtomicOperation.commit} method once all checks and mutations have been added to the operation.
   */
  atomic(): AtomicOperation<E, Q>;
  /**
   * Watch for changes to the given keys in the database.
   * The returned stream is a ReadableStream that emits a new value whenever any of the watched keys change their versionstamp.
   * The emitted value is an array of KvEntryMaybe objects,
   * with the same length and order as the keys array.
   * If no value exists for a given key, the returned entry will have a null value and versionstamp.
   *
   * The returned stream does not return every single intermediate state of the watched keys,
   * but rather only keeps you up to date with the latest state of the keys.
   * This means that if a key is modified multiple times quickly,
   * you may not receive a notification for every single change,
   * but rather only the latest state of the key.
   *
   * @param {E["key"][]} keys - An array of keys to watch for changes.
   */
  watch<Ks extends E["key"][] | []>(
    keys: Ks
  ): ReadableStream<{
    [N in keyof Ks]: KvPairMaybe<SelectKvPair<E, Ks[N]>>;
  }>;
  /**
   * Listen for queue values to be delivered from the database queue,
   * which were enqueued with {@link AtomicOperation.enqueue}, you can spcify which keys to listen to.
   *
   * You need to use {@link AtomicOperation.dequeue} to dequeue values from the queue after you have received and processed them,
   * or you will get duplicates in next time you call this method.
   *
   * @param {...const Ks extends Q["key"][]} keys - The keys to listen to.
   * @return {ReadableStream<{ [N in keyof Ks]: _KvWithKey<Q, Ks[N]> }[keyof Ks]>} A readable stream of key-value pairs.
   */
  listenQueue<Ks extends Q["key"][] | []>(
    ...keys: Ks
  ): ReadableStream<
    {
      [N in keyof Ks]: SelectKvPair<Q, Ks[N]>;
    }[keyof Ks]
  >;

  /** Get a subspace of the database, operating on all key with the given prefix. */
  subspace<P extends KvKeyPrefix<E["key"]> & KvKeyPrefix<Q["key"]>>(
    prefix: P
  ): KvApi<RemoveKvPairPrefix<E, P>, RemoveKvPairPrefix<Q, P>>;
}

/**
 * @public
 * A key-value database designed for efficient data storage and retrieval.
 *
 * In this database, data is saved and accessed through key-value pairs.
 * The key is a {@link KvKey}, and the value is any JavaScript object that can be structured and serialized.
 * Keys are sorted lexicographically as per the {@link KvKey} documentation, and each key in the database is unique.
 * When a key is read, the system returns the most recent value associated with it.
 * Keys can be removed from the database, and upon deletion, they will no longer appear during read operations.
 *
 * Values can be any JavaScript object that is {@link https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Structured_clone_algorithm structured-serializable}, such as objects, arrays, strings, or numbers.
 *
 * Each key is versioned upon writing by assigning it an ever-increasing "versionstamp".
 * This versionstamp signifies the versionstamp of the key-value pair at a specific moment and facilitates transactional operations on the database without the need for locks.
 * Atomic operations, which can include conditions to check against the expected versionstamp, ensure that the operation only completes if the versionstamp matches the anticipated value.
 *
 * The NanoKV database also provides a lightweight message queue feature.
 * Please note: Although the API names are similar to those of denokv,
 * there are significant differences in their behaviors.
 * For instance, denokv's message delivery is more inclined towards real-time processing,
 * and thus denokv will attempt to redeliver to the consumer within a specific time frame
 * (with settings for the number of retries and frequency).
 * In contrast, NanoKV leans more towards the essence of a "queue",
 * where consumers can retrieve outdated messages from the queue (if not manually {@link AtomicOperation.dequeue dequeued}),
 * as well as receive new messages in real-time.
 *
 * Additionally, NanoKV supports the existence of multiple queues simultaneously.
 * Different parts of an application (such as various microservices) can listen only to the queues they are interested in,
 * whereas denokv can only have a single global queue (for a denokv instance).
 * This is also a significant difference.
 *
 * @template E KvEntry type
 * @template Q KvQueueEntry type
 * @example
 * ```ts
 * type EntryType =
 *   | KvEntry<["user", id: number, "name"], string>
 *   | KvEntry<["user", id: number, "age"], number>;
 * type QueueType =
 *   | KvQueueEntry<["user-joined"], number>
 *   | KvQueueEntry<["user-left"], number>;
 * const kv = new NanoKV<EntryType, QueueType>("http://127.0.0.1:2256");
 * ```
 */
export class NanoKV<
  E extends KvEntry = KvEntry,
  Q extends KvQueueEntry = KvQueueEntry
> implements KvApi<E, Q>
{
  #watch: WebSocketConnection;
  #subscriptions = new Map<string, WatchState>();
  #firstWatch = new Map<number, Reactor<void>>();
  #listen: WebSocketConnection;
  #queues = new Map<string, ListenState>();
  #protocol: Protocol;

  /** Create a new NanoKV connection. */
  constructor(
    /** Current NanoKV endpoint */
    public readonly endpoint: string,
    /** Specify a custom encoding method for values, by default it will use JSON as serialization format */
    options: { encoding?: ProtocolEncoding } = {}
  ) {
    this.#protocol = new Protocol(options.encoding);
    this.#watch = new WebSocketConnection(
      endpoint + "/watch",
      (send) => {
        const keys = [...this.#subscriptions.values()].map(
          ({ packed }) => packed
        );
        send(this.#protocol.encodeWatch({ id: -1, keys }));
      },
      (data) => {
        const { id, values } = this.#protocol.decodeWatchOutputRaw(data);
        for (const { key, value, versionstamp } of values) {
          const stringified = Buffer.from(key).toString("base64");
          const subscription = this.#subscriptions.get(stringified);
          if (subscription) {
            if (versionstamp != null)
              subscription.notify({
                key: unpackKey(key),
                value,
                versionstamp,
              });
            else
              subscription.notify({
                key: unpackKey(key),
                value: null,
                versionstamp,
              });
          }
        }
        if (id == -1) {
          for (const reactor of this.#firstWatch.values()) {
            reactor.continue();
          }
          this.#firstWatch.clear();
        } else if (id) {
          const reactor = this.#firstWatch.get(id);
          if (reactor) {
            reactor.continue();
            this.#firstWatch.delete(id);
          }
        }
      }
    );
    this.#listen = new WebSocketConnection(
      endpoint + "/listen",
      (send, reason) => {
        if (reason) {
          console.error(reason);
        }
        const added = [...this.#queues.keys()].map((key) =>
          Buffer.from(key, "base64")
        );
        send(this.#protocol.encodeListen({ added }));
      },
      (data) => {
        const entries = this.#protocol.decodeListenOutputRaw(data);
        const triggered = new Set<ListenState>();
        for (const { key, schedule, sequence, value } of entries) {
          const stringified = Buffer.from(key).toString("base64");
          const queue = this.#queues.get(stringified);
          if (!queue) continue;
          triggered.add(queue);
          queue.enqueue({
            key: unpackKey(key),
            value,
            schedule,
            sequence,
          });
        }
      }
    );
  }

  async #snapshot_read(ranges: RawReadRange[]): Promise<RawKvEntry[][]> {
    const request = this.#protocol.encodeSnapshotRead(ranges);
    const res = await fetch(`${this.endpoint}/snapshot_read`, {
      method: "POST",
      body: request,
    });
    if (res.status === 200) {
      const response = await res.arrayBuffer();
      return this.#protocol.decodeSnapshotReadOutput(new Uint8Array(response));
    } else {
      throw new Error(await res.text());
    }
  }
  async #atomic_write(param: {
    checks?: RawCheck[];
    mutations?: RawMutation[];
    enqueues?: RawEnqueue[];
    dequeues?: RawDequeue[];
  }): Promise<KvCommitResult | KvCommitError> {
    const request = this.#protocol.encodeAtomicWrite(param);
    const res = await fetch(`${this.endpoint}/atomic_write`, {
      method: "POST",
      body: request,
    });
    if (res.status === 200) {
      const response = await res.arrayBuffer();
      return this.#protocol.decodeAtomicWriteOutput(new Uint8Array(response));
    } else {
      throw new Error(await res.text());
    }
  }

  async get<K extends E["key"]>(
    key: K
  ): Promise<KvPairMaybe<SelectKvPair<E, K>>> {
    const [[entry]] = await this.#snapshot_read([{ start: key, exact: true }]);
    if (entry)
      return {
        key,
        value: entry.value as any,
        versionstamp: entry.versionstamp as bigint,
      } as SelectKvPair<E, K>;
    return {
      key,
      value: undefined,
      versionstamp: undefined,
    } as KvPairNotFound<SelectKvPair<E, K>>;
  }

  async getMany<Ks extends E["key"][] | []>(
    keys: Ks
  ): Promise<{
    [N in keyof Ks]: KvPairMaybe<SelectKvPair<E, Ks[N]>>;
  }> {
    const result = await this.#snapshot_read(
      keys.map((key) => ({ start: key, exact: true } as const))
    );
    return result.map(([entry], i) => {
      if (!entry)
        return {
          key: keys[i],
          value: undefined,
          versionstamp: undefined,
        };
      return {
        key: entry.key,
        value: entry.value,
        versionstamp: entry.versionstamp,
      };
    }) as any;
  }

  async set<K extends E["key"]>(
    key: K,
    value: ValueFotKvPair<E, K>,
    { expireIn }: { expireIn?: number } = {}
  ): Promise<KvCommitResult | KvCommitError> {
    return await this.#atomic_write({
      mutations: [
        {
          key,
          type: MutationType.SET,
          value,
          expired_at: expireIn ? Date.now() + expireIn : undefined,
        },
      ],
    });
  }

  async delete(key: E["key"]): Promise<KvCommitResult | KvCommitError> {
    return await this.#atomic_write({
      mutations: [{ key, type: MutationType.DELETE }],
    });
  }

  list<K extends E["key"], P extends KvKeyPrefix<K>>(
    selector: KvListSelector<K, P>,
    options: KvListOptions = {}
  ): ReadableStream<SelectKvPairByPrefix<E, P>> {
    let { limit = 500, reverse = false, batchSize = 128, cursor } = options;
    const base =
      "prefix" in selector
        ? {
            start: Buffer.concat([packKey(selector.prefix), START]),
            end: Buffer.concat([packKey(selector.prefix), END]),
            ...selector,
          }
        : selector;
    if (batchSize <= 0 || batchSize >= 1024 || !Number.isFinite(batchSize)) {
      batchSize = 1024;
    }
    return new ReadableStream<SelectKvPairByPrefix<E, P>>(
      {
        pull: async (controller) => {
          const [values] = await this.#snapshot_read([
            {
              ...base,
              ...(cursor
                ? reverse
                  ? { end: cursor }
                  : { start: Buffer.concat([packKey(cursor), START]) }
                : undefined),
              reverse,
              limit: Math.min(limit, batchSize),
              exact: false,
            },
          ]);
          for (const item of values) controller.enqueue(item as any);
          if (values.length == batchSize && limit > batchSize) {
            cursor = values[values.length - 1].key;
            limit -= batchSize;
          } else {
            controller.close();
          }
        },
      },
      { highWaterMark: batchSize }
    );
  }

  atomic(): AtomicOperation<E, Q> {
    return new AtomicOperationImpl(this.#atomic_write.bind(this));
  }

  #genWatchId() {
    while (true) {
      const value = randomBytes(4).readInt32BE(0);
      if (value && value != -1 && !this.#firstWatch.has(value)) return value;
    }
  }

  watch<Ks extends E["key"][] | []>(
    keys: Ks
  ): ReadableStream<{
    [N in keyof Ks]: KvPairMaybe<SelectKvPair<E, Ks[N]>>;
  }> {
    const reactor = new Reactor<void>();
    const cached = new Map<string, Uint8Array>();
    const id = this.#genWatchId();
    this.#firstWatch.set(id, reactor);
    return new ReadableStream<{
      [N in keyof Ks]: KvPairMaybe<SelectKvPair<E, Ks[N]>>;
    }>(
      {
        start: () => {
          const packeds: Uint8Array[] = [];
          for (const key of keys) {
            const packed = packKey(key as KvKey);
            packeds.push(packed);
            const stringified = Buffer.from(packed).toString("base64");
            cached.set(stringified, packed);
            const state = this.#subscriptions.get(stringified);
            if (state) {
              state.addReactor(reactor);
            } else {
              this.#subscriptions.set(
                stringified,
                new WatchState(key as KvKey, packed, reactor)
              );
            }
          }
          this.#watch.open();
          this.#watch.trySend(() =>
            this.#protocol.encodeWatch({ id, keys: packeds })
          );
        },
        pull: async (controller) => {
          await reactor;
          const snapshot = [];
          for (const key of cached.keys()) {
            const entry = this.#subscriptions.get(key)!.entry;
            snapshot.push(entry);
          }
          controller.enqueue(snapshot as any);
        },
        cancel: () => {
          this.#firstWatch.delete(id);
          const packeds: Uint8Array[] = [];
          for (const [key, packed] of cached) {
            if (this.#subscriptions.get(key)!.removeReactor(reactor)) {
              this.#subscriptions.delete(key);
              packeds.push(packed);
            }
          }
          if (packeds.length) {
            this.#watch.trySend(() =>
              this.#protocol.encodeWatch({ id: 0, keys: packeds })
            );
          }
          if (this.#subscriptions.size === 0) {
            this.#watch.close();
          }
        },
      },
      { highWaterMark: 0 }
    );
  }

  listenQueue<Ks extends Q["key"][] | []>(
    ...keys: Ks
  ): ReadableStream<
    {
      [N in keyof Ks & number]: SelectKvPair<Q, Ks[N]>;
    }[keyof Ks & number]
  > {
    const state = new ListenState();
    const cached = new Map<string, Uint8Array>();
    const packeds: Uint8Array[] = [];
    return new ReadableStream<
      {
        [N in keyof Ks & number]: SelectKvPair<Q, Ks[N]>;
      }[keyof Ks & number]
    >(
      {
        start: () => {
          for (const key of keys) {
            const packed = packKey(key as KvKey);
            packeds.push(packed);
            const stringified = Buffer.from(packed).toString("base64");
            cached.set(stringified, packed);
            if (this.#queues.has(stringified)) {
              throw new Error("cannot listen to an already listened queue");
            }
            this.#queues.set(stringified, state);
          }
          this.#listen.open();
          this.#listen.trySend(() =>
            this.#protocol.encodeListen({ added: packeds })
          );
        },
        pull: async (controller) => {
          await state.reactor;
          for (const item of state.take()) controller.enqueue(item as any);
        },
        cancel: () => {
          for (const [key] of cached) {
            this.#queues.delete(key);
          }
          if (packeds.length) {
            this.#watch.trySend(() =>
              this.#protocol.encodeListen({ removed: packeds })
            );
          }
          if (this.#queues.size === 0) {
            this.#listen.close();
          }
        },
      },
      { highWaterMark: 0 }
    );
  }
  subspace<P extends KvKeyPrefix<E["key"]> & KvKeyPrefix<Q["key"]>>(
    prefix: P
  ): KvApi<RemoveKvPairPrefix<E, P>, RemoveKvPairPrefix<Q, P>> {
    return new SubspaceProxy(this as KvApi, prefix);
  }
}

class SubspaceProxy<
  E extends KvEntry = KvEntry,
  Q extends KvQueueEntry = KvQueueEntry
> implements KvApi<E, Q>
{
  #parent: KvApi;
  #prefix: KvKey;
  constructor(parent: KvApi, prefix: KvKey) {
    this.#parent = parent;
    this.#prefix = prefix;
  }
  get<K extends E["key"]>(
    key: K
  ): Promise<KvPairMaybe<Extract<E, { key: K }>>> {
    return this.#parent.get([...this.#prefix, ...key]) as any;
  }
  getMany<Ks extends [] | E["key"][]>(
    keys: Ks
  ): Promise<{ [N in keyof Ks]: KvPairMaybe<Extract<E, { key: Ks[N] }>> }> {
    return this.#parent.getMany(
      keys.map((key) => [...this.#prefix, ...key])
    ) as any;
  }
  set<K extends E["key"]>(
    key: K,
    value: DistributiveProp<Extract<E, { key: KvKeyToObject<K> }>, "value">,
    options?: { expireIn?: number }
  ): Promise<KvCommitResult | KvCommitError> {
    return this.#parent.set([...this.#prefix, ...key], value, options);
  }
  delete(key: E["key"]): Promise<KvCommitResult | KvCommitError> {
    return this.#parent.delete([...this.#prefix, ...key]);
  }
  list<K extends E["key"], P extends KvKeyPrefix<K>>(
    selector: KvListSelector<K, P>,
    options?: KvListOptions
  ): ReadableStream<Extract<E, { key: KvKeyToObject<P> }>> {
    return this.#parent.list(
      Object.fromEntries(
        Object.entries(selector).map(([k, v]) => [k, [...this.#prefix, ...v]])
      ) as any,
      options
    ) as any;
  }
  atomic(): AtomicOperation<E, Q> {
    return new AtomicOperationProxy(this.#parent.atomic(), this.#prefix);
  }
  watch<Ks extends [] | E["key"][]>(
    keys: Ks
  ): ReadableStream<{
    [N in keyof Ks]: KvPairMaybe<Extract<E, { key: Ks[N] }>>;
  }> {
    return this.#parent.watch(
      keys.map((key) => [...this.#prefix, ...key])
    ) as any;
  }
  listenQueue<Ks extends [] | Q["key"][]>(
    ...keys: Ks
  ): ReadableStream<{ [N in keyof Ks]: Extract<Q, { key: Ks[N] }> }[keyof Ks]> {
    return this.#parent.listenQueue(
      ...keys.map((key) => [...this.#prefix, ...key])
    ) as any;
  }
  subspace<P extends KvKeyPrefix<E["key"]> & KvKeyPrefix<Q["key"]>>(
    prefix: P
  ): KvApi<RemoveKvPairPrefix<E, P>, RemoveKvPairPrefix<Q, P>> {
    return new SubspaceProxy(this.#parent, [...this.#prefix, ...prefix]);
  }
}
