import { randomBytes } from "node:crypto";
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
  FilterTupleValuePair,
  FilterTupleValuePairByPrefix,
  TuplePrefix,
  ValueForTuple,
} from "./type_helpers";
import type {
  AtomicCheck,
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
 *
 * Each key is versioned upon writing by assigning it an ever-increasing "versionstamp".
 * This versionstamp signifies the versionstamp of the key-value pair at a specific moment and facilitates transactional operations on the database without the need for locks.
 * Atomic operations, which can include conditions to check against the expected versionstamp, ensure that the operation only completes if the versionstamp matches the anticipated value.
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
> {
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

  /**
   * Retrieve the value and versionstamp for the given key from the database in the form of a {@link KvEntryMaybe}.
   * If no value exists for the key, the returned entry will have a null value and versionstamp.
   */
  async get<K extends E["key"]>(
    key: K
  ): Promise<KvPairMaybe<FilterTupleValuePair<E, K>>> {
    const [[entry]] = await this.#snapshot_read([{ start: key, exact: true }]);
    if (entry)
      return {
        key,
        value: entry.value as any,
        versionstamp: entry.versionstamp as bigint,
      } as FilterTupleValuePair<E, K>;
    return {
      key,
      value: undefined,
      versionstamp: undefined,
    } as KvPairNotFound<FilterTupleValuePair<E, K>>;
  }

  /**
   * Retrieve multiple values and versionstamp from the database in the form of an array of {@link KvEntryMaybe} objects.
   * The returned array will have the same length as the keys array, and the entries will be in the same order as the keys.
   * If no value exists for a given key, the returned entry will have a null value and versionstamp.
   */
  async getMany<Ks extends E["key"][] | []>(
    keys: Ks
  ): Promise<{
    [N in keyof Ks]: KvPairMaybe<FilterTupleValuePair<E, Ks[N]>>;
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
  async set<K extends E["key"]>(
    key: K,
    value: ValueForTuple<E, K>,
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

  /**
   * Delete the value for the given key from the database.
   * If no value exists for the key, this operation is a no-op.
   *
   * @param key - The key to delete.
   * @return A promise resolving to an object containing a boolean indicating whether the operation was successful, and the versionstamp of the deleted key-value entry.
   */
  async delete(key: E["key"]): Promise<KvCommitResult | KvCommitError> {
    return await this.#atomic_write({
      mutations: [{ key, type: MutationType.DELETE }],
    });
  }

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
  list<K extends E["key"], P extends TuplePrefix<K>>(
    selector: KvListSelector<K, P>,
    options: KvListOptions = {}
  ): ReadableStream<FilterTupleValuePairByPrefix<E, P>> {
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
    return new ReadableStream<FilterTupleValuePairByPrefix<E, P>>(
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

  /**
   * Create a new {@link AtomicOperation} object which can be used to perform an atomic transaction on the database.
   * This does not perform any operations on the database -
   * the atomic transaction must be committed explicitly using the {@link AtomicOperation.commit} method once all checks and mutations have been added to the operation.
   */
  atomic(): AtomicOperation<E, Q> {
    return new AtomicOperation(this.#atomic_write.bind(this));
  }

  #genWatchId() {
    while (true) {
      const value = randomBytes(4).readInt32BE(0);
      if (value && value != -1 && !this.#firstWatch.has(value)) return value;
    }
  }

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
    [N in keyof Ks & number]: KvPairMaybe<FilterTupleValuePair<E, Ks[N]>>;
  }> {
    const reactor = new Reactor<void>();
    const cached = new Map<string, Uint8Array>();
    const id = this.#genWatchId();
    this.#firstWatch.set(id, reactor);
    return new ReadableStream<{
      [N in keyof Ks & number]: KvPairMaybe<FilterTupleValuePair<E, Ks[N]>>;
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

  /**
   * Listen for queue values to be delivered from the database queue,
   * which were enqueued with {@link AtomicOperation.enqueue}.
   * The provided handler callback is invoked on every dequeued value.
   * A failed callback invocation is automatically retried multiple times until it succeeds or until the maximum number of retries is reached.
   *
   * @param {...const Ks extends Q["key"][]} keys - The keys to listen to.
   * @return {ReadableStream<{ [N in keyof Ks]: _KvWithKey<Q, Ks[N]> }[keyof Ks & number]>} A readable stream of key-value pairs.
   */
  listenQueue<Ks extends Q["key"][] | []>(
    ...keys: Ks
  ): ReadableStream<
    {
      [N in keyof Ks & number]: FilterTupleValuePair<Q, Ks[N]>;
    }[keyof Ks & number]
  > {
    const state = new ListenState();
    const cached = new Map<string, Uint8Array>();
    const packeds: Uint8Array[] = [];
    return new ReadableStream<
      {
        [N in keyof Ks & number]: FilterTupleValuePair<Q, Ks[N]>;
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
}

/**
 * An atomic operation on {@link NanoKV} allows for multiple updates to be conducted in a single, indivisible transaction.
 * These operations require explicit commitment through the invocation of the commit method, as they do not auto-commit by default.
 *
 * Atomic operations facilitate the execution of several mutations on the key-value store in a single transaction, ensuring consistency.
 * They also enable conditional updates by incorporating {@link AtomicCheck}s, which verify that mutations only occur if the key-value pair meets specific versionstamp criteria.
 * Should any check fail, the entire operation is aborted, leaving the store unchanged.
 *
 * The sequence of mutations is preserved as per the order specified in the operation, with checks executed prior to any mutations, although the sequence of checks is not observable.
 *
 * Atomic operations are instrumental in implementing optimistic locking,
 * a strategy where a mutation is executed only if the key-value pair has remained unaltered since the last read operation.
 * This is achieved by including a check to verify that the versionstamp of the key-value pair has not changed since it was last accessed.
 * If the check fails, the mutation is aborted, and the operation is considered unsuccessful.
 * The read-modify-write cycle can be repeated in a loop until it completes successfully.
 *
 * The commit method of an atomic operation yields a result indicating the success of checks and the execution of mutations.
 * A {@link KvCommitError} with an `ok: false` property signifies failure due to a check.
 * Other failures, such as storage errors or invalid values, trigger exceptions.
 * Successful operations return a {@link KvCommitResult} object with an ok: true property, along with the versionstamp of the committed value.
 * @hideconstructor
 */
export class AtomicOperation<E extends KvEntry, Q extends KvQueueEntry> {
  #checks: RawCheck[] = [];
  #mutations: RawMutation[] = [];
  #enqueues: RawEnqueue[] = [];
  #dequeues: RawDequeue[] = [];

  /** @hidden */
  #committer: (param: {
    checks: RawCheck[];
    mutations: RawMutation[];
    enqueues: RawEnqueue[];
    dequeues: RawDequeue[];
  }) => Promise<KvCommitResult | KvCommitError>;

  constructor(
    committer: (param: {
      checks?: RawCheck[];
      mutations?: RawMutation[];
      enqueues?: RawEnqueue[];
      dequeues?: RawDequeue[];
    }) => Promise<KvCommitResult | KvCommitError>
  ) {
    this.#committer = committer;
  }

  /**
   * Add to the operation a check that ensures that the versionstamp of the key-value pair in the KV store matches the given versionstamp.
   * If the check fails, the entire operation will fail and no mutations will be performed during the commit.
   */
  check(...entries: AtomicCheck<E["key"]>[]): this {
    this.#checks.push(...entries);
    return this;
  }

  /**
   * Add to the operation a mutation that sets the value of the specified key to the specified value if all checks pass during the commit.
   *
   * Optionally an expireIn option can be specified to set a time-to-live (TTL) for the key.
   * The TTL is specified in milliseconds, and the key will be deleted from the database at earliest after the specified number of milliseconds have elapsed.
   * Once the specified duration has passed, the key may still be visible for some additional time.
   * If the expireIn option is not specified, the key will not expire.
   */
  set<K extends E["key"]>(
    key: K,
    value: ValueForTuple<E, K>,
    { expireIn }: { expireIn?: number } = {}
  ): this {
    this.#mutations.push({
      key,
      type: MutationType.SET,
      value,
      expired_at: expireIn ? Date.now() + expireIn : undefined,
    });
    return this;
  }

  /** Add to the operation a mutation that deletes the specified key if all checks pass during the commit. */
  delete<K extends E["key"]>(key: K): this {
    this.#mutations.push({ key, type: MutationType.DELETE });
    return this;
  }

  /** Add to the operation a mutation that enqueues a value into the queue if all checks pass during the commit. */
  enqueue<K extends Q["key"]>(
    key: K,
    value: ValueForTuple<Q, K>,
    {
      delay = 0,
      schedule = delay === 0 ? undefined : Date.now() + delay,
    }: { delay?: number; schedule?: number } = {}
  ): this {
    this.#enqueues.push({
      key,
      value,
      schedule,
    });
    return this;
  }

  /** Add to the operation a mutation that dequeues a value from the queue if all checks pass during the commit. */
  dequeue(...keys: { key: Q["key"]; schedule: number; sequence: bigint }[]) {
    this.#dequeues.push(...keys);
    return this;
  }

  /**
   * Commit the operation to the KV store.
   * Returns a value indicating whether checks passed and mutations were performed.
   * If the operation failed because of a failed check, the return value will be a {@link KvCommitError} with an ok: false property.
   * If the operation failed for any other reason (storage error, invalid value, etc.), an exception will be thrown.
   * If the operation succeeded, the return value will be a {@link KvCommitResult} object with a ok: true property and the versionstamp of the value committed to KV.
   *
   * If the commit returns ok: false, one may create a new atomic operation with updated checks and mutations and attempt to commit it again.
   * See the note on optimistic locking in the documentation for {@link AtomicOperation}.
   */
  commit(): Promise<KvCommitResult | KvCommitError> {
    return this.#committer({
      checks: this.#checks,
      mutations: this.#mutations,
      enqueues: this.#enqueues,
      dequeues: this.#dequeues,
    });
  }
}
