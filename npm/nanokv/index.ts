import { packKey, unpackKey } from "./kv_key";
import { MutationType } from "./packet";
import {
  decodeAtomicWriteOutput,
  decodeListenOutputRaw,
  decodeSnapshotReadOutput,
  decodeWatchOutput,
  decodeWatchOutputRaw,
  encodeAtomicWrite,
  encodeListen,
  encodeSnapshotRead,
  encodeWatch,
  type RawCheck,
  type RawDequeue,
  type RawEnqueue,
  type RawKvEntry,
  type RawMutation,
  type RawReadRange,
} from "./protocol";
import { Reactor } from "./reactor";
import type {
  KvCheck,
  KvEntry,
  KvEntryMaybe,
  KvKey,
  KvListOptions,
  KvListSelector,
  KvQueueEntry,
} from "./types";
import { WebSocketConnection } from "./ws";
import { randomBytes } from "node:crypto";

const START = new Uint8Array([0x01]);
const END = new Uint8Array([0xff]);

type WithKey<
  X extends KvEntry | KvQueueEntry,
  K extends X["key"]
> = X extends unknown ? (K extends X["key"] ? X : never) : never;
type Prefix<X extends readonly unknown[]> = X extends [unknown, ...infer Xs]
  ? [] | [X[0]] | [X[0], ...Prefix<Xs>]
  : [];
type FindPrefix<
  X extends KvEntry | KvQueueEntry,
  K extends Prefix<X["key"]>
> = X extends unknown ? (K extends Prefix<X["key"]> ? X : never) : never;

type EntryNotFound<X extends KvEntry> = {
  key: X["key"];
  value: undefined;
  version: undefined;
};

type MaybeEntry<X extends KvEntry> = X | EntryNotFound<X>;

class WatchState {
  entry: KvEntryMaybe<any, any>;
  #reactors: Reactor<void>[] = [];
  constructor(key: KvKey, public packed: Uint8Array, reactor: Reactor<void>) {
    this.entry = { key, value: undefined, version: 0n };
    this.#reactors.push(reactor);
  }
  addReactor(reactor: Reactor<void>) {
    this.#reactors.push(reactor);
  }
  removeReactor(reactor: Reactor<void>) {
    this.#reactors = this.#reactors.filter((r) => r !== reactor);
    return this.#reactors.length === 0;
  }
  notify(value: KvEntryMaybe<any, any>) {
    if (this.entry.version === value.version) return;
    this.entry = value;
    for (const reactor of this.#reactors) {
      reactor.continue();
    }
  }
}

class ListenState {
  localqueue: KvQueueEntry<any, any>[] = [];
  reactor = new Reactor<void>();

  take() {
    return this.localqueue.splice(0);
  }

  enqueue(entry: KvQueueEntry<any, any>) {
    this.localqueue.push(entry);
    this.reactor.continue();
  }
}

export class NanoKV<
  E extends KvEntry<any, any> = KvEntry<any, any>,
  Q extends KvQueueEntry<any, any> = KvQueueEntry<any, any>
> {
  #watch: WebSocketConnection;
  #subscriptions = new Map<string, WatchState>();
  #firstWatch = new Map<number, Reactor<void>>();
  #listen: WebSocketConnection;
  #queues = new Map<string, ListenState>();
  constructor(public readonly endpoint: string) {
    this.#watch = new WebSocketConnection(
      endpoint + "/watch",
      (send) => {
        const keys = [...this.#subscriptions.values()].map(
          ({ packed }) => packed
        );
        send(encodeWatch({ id: -1, keys }));
      },
      (data) => {
        const { id, values } = decodeWatchOutputRaw(data);
        for (const { key, value, version } of values) {
          const stringified = Buffer.from(key).toString("base64");
          const subscription = this.#subscriptions.get(stringified);
          if (subscription) {
            if (version)
              subscription.notify({
                key: unpackKey(key),
                value,
                version,
              });
            else
              subscription.notify({
                key: unpackKey(key),
                value: undefined,
                version,
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
        send(encodeListen({ added }));
      },
      (data) => {
        const entries = decodeListenOutputRaw(data);
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
    const request = encodeSnapshotRead(ranges);
    const res = await fetch(`${this.endpoint}/snapshot_read`, {
      method: "POST",
      body: request,
    });
    if (res.status === 200) {
      const response = await res.arrayBuffer();
      return decodeSnapshotReadOutput(new Uint8Array(response));
    } else {
      throw new Error(await res.text());
    }
  }
  async #atomic_write(param: {
    checks?: RawCheck[];
    mutations?: RawMutation[];
    enqueues?: RawEnqueue[];
    dequeues?: RawDequeue[];
  }): Promise<{
    ok: boolean;
    version: bigint;
  }> {
    const request = encodeAtomicWrite(param);
    const res = await fetch(`${this.endpoint}/atomic_write`, {
      method: "POST",
      body: request,
    });
    if (res.status === 200) {
      const response = await res.arrayBuffer();
      return decodeAtomicWriteOutput(new Uint8Array(response));
    } else {
      throw new Error(await res.text());
    }
  }

  async get<K extends E["key"]>(key: K): Promise<MaybeEntry<WithKey<E, K>>> {
    const [[entry]] = await this.#snapshot_read([{ start: key, exact: true }]);
    if (entry)
      return {
        key,
        value: entry.value as any,
        version: entry.version as bigint,
      } as WithKey<E, K>;
    return {
      key,
      value: undefined,
      version: undefined,
    } as EntryNotFound<WithKey<E, K>>;
  }

  async getMany<const Ks extends readonly E["key"][]>(
    ...keys: Ks
  ): Promise<{ [N in keyof Ks]: MaybeEntry<WithKey<E, Ks[N]>> }> {
    const result = await this.#snapshot_read(
      keys.map((key) => ({ start: key, exact: true } as const))
    );
    return result.map(([entry], i) => {
      if (!entry)
        return {
          key: keys[i],
          value: undefined,
          version: undefined,
        };
      return {
        key: entry.key,
        value: entry.value,
        version: entry.version,
      };
    }) as any;
  }

  async set<K extends E["key"]>(
    key: K,
    value: WithKey<E, K>["value"],
    { expireIn }: { expireIn?: number } = {}
  ): Promise<{ ok: boolean; version: bigint }> {
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

  async delete<K extends E["key"]>(
    key: K
  ): Promise<{ ok: boolean; version: bigint }> {
    return await this.#atomic_write({
      mutations: [{ key, type: MutationType.DELETE }],
    });
  }

  list<K extends Prefix<E["key"]>>(
    selector: KvListSelector<K>,
    {
      limit = 500,
      reverse = false,
      batchSize = 128,
      cursor,
    }: KvListOptions = {}
  ): ReadableStream<FindPrefix<E, K>[]> {
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
    return new ReadableStream<FindPrefix<E, K>[]>(
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
          controller.enqueue(values as any);
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
    return new AtomicOperation(this.#atomic_write.bind(this));
  }

  #genWatchId() {
    while (true) {
      const value = randomBytes(4).readInt32BE(0);
      if (value && value != -1 && !this.#firstWatch.has(value)) return value;
    }
  }

  watch<const Ks extends readonly E["key"][]>(
    ...keys: Ks
  ): ReadableStream<{ [N in keyof Ks]: MaybeEntry<WithKey<E, Ks[N]>> }> {
    const reactor = new Reactor<void>();
    const cached = new Map<string, Uint8Array>();
    const id = this.#genWatchId();
    this.#firstWatch.set(id, reactor);
    return new ReadableStream<{
      [N in keyof Ks]: MaybeEntry<WithKey<E, Ks[N]>>;
    }>(
      {
        start: () => {
          const packeds: Uint8Array[] = [];
          for (const key of keys) {
            const packed = packKey(key);
            packeds.push(packed);
            const stringified = Buffer.from(packed).toString("base64");
            cached.set(stringified, packed);
            const state = this.#subscriptions.get(stringified);
            if (state) {
              state.addReactor(reactor);
            } else {
              this.#subscriptions.set(
                stringified,
                new WatchState(key, packed, reactor)
              );
            }
          }
          this.#watch.open();
          this.#watch.trySend(() => encodeWatch({ id, keys: packeds }));
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
            this.#watch.trySend(() => encodeWatch({ id: 0, keys: packeds }));
          }
          if (this.#subscriptions.size === 0) {
            this.#watch.close();
          }
        },
      },
      { highWaterMark: 0 }
    );
  }

  listen<const Ks extends readonly Q["key"][]>(
    ...keys: Ks
  ): ReadableStream<{ [N in keyof Ks]: WithKey<Q, Ks[N]> }[keyof Ks & number]> {
    const state = new ListenState();
    const cached = new Map<string, Uint8Array>();
    const packeds: Uint8Array[] = [];
    return new ReadableStream<
      { [N in keyof Ks]: WithKey<Q, Ks[N]> }[keyof Ks & number]
    >(
      {
        start: () => {
          for (const key of keys) {
            const packed = packKey(key);
            packeds.push(packed);
            const stringified = Buffer.from(packed).toString("base64");
            cached.set(stringified, packed);
            if (this.#queues.has(stringified)) {
              throw new Error("cannot listen to an already listened queue");
            }
            this.#queues.set(stringified, state);
          }
          this.#listen.open();
          this.#listen.trySend(() => encodeListen({ added: packeds }));
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
            this.#watch.trySend(() => encodeListen({ removed: packeds }));
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

export class AtomicOperation<
  E extends KvEntry<any, any>,
  Q extends KvQueueEntry<any, any>
> {
  #checks: RawCheck[] = [];
  #mutations: RawMutation[] = [];
  #enqueues: RawEnqueue[] = [];
  #dequeues: RawDequeue[] = [];
  #committer: (param: {
    checks: RawCheck[];
    mutations: RawMutation[];
    enqueues: RawEnqueue[];
    dequeues: RawDequeue[];
  }) => Promise<{
    ok: boolean;
    version: bigint;
  }>;

  constructor(
    committer: (param: {
      checks?: RawCheck[];
      mutations?: RawMutation[];
      enqueues?: RawEnqueue[];
      dequeues?: RawDequeue[];
    }) => Promise<{
      ok: boolean;
      version: bigint;
    }>
  ) {
    this.#committer = committer;
  }

  check(...entries: KvCheck<E["key"]>[]): this {
    this.#checks.push(...entries);
    return this;
  }

  set<K extends E["key"]>(
    key: K,
    value: WithKey<E, K>["value"],
    { expireIn }: { expireIn?: number; withVersionstampSuffix?: boolean } = {}
  ): this {
    this.#mutations.push({
      key,
      type: MutationType.SET,
      value,
      expired_at: expireIn ? Date.now() + expireIn : undefined,
    });
    return this;
  }

  delete<K extends E["key"]>(key: K): this {
    this.#mutations.push({ key, type: MutationType.DELETE });
    return this;
  }

  enqueue<K extends Q["key"]>(
    key: K,
    value: WithKey<Q, K>["value"],
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

  dequeue(...keys: { key: Q["key"]; schedule: number; sequence: bigint }[]) {
    this.#dequeues.push(...keys);
    return this;
  }

  commit(): Promise<{ ok: boolean; version: bigint }> {
    return this.#committer({
      checks: this.#checks,
      mutations: this.#mutations,
      enqueues: this.#enqueues,
      dequeues: this.#dequeues,
    });
  }
}

const kv = new NanoKV<
  KvEntry<["test", number], string>,
  KvQueueEntry<["test-queue"], string>
>("http://127.0.0.1:2256");

async function watch(signal: AbortSignal) {
  const watcher = kv.watch(["test", 1], ["test", 2]);
  const reader = watcher.getReader();
  signal.addEventListener("abort", () => {
    reader.cancel();
  });
  (async () => {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      console.log("update", value);
    }
  })();
}

async function listen(signal: AbortSignal) {
  const reader = kv.listen(["test-queue"]).getReader();
  signal.addEventListener("abort", () => {
    reader.cancel();
  });
  (async () => {
    while (true) {
      const { done, value } = await reader.readMany();
      if (done) break;
      console.log("listen", value);
      await kv
        .atomic()
        .dequeue(...value)
        .commit();
    }
  })();
}

async function read() {
  console.log(await kv.getMany(["test", 1], ["test", 2]));
}

const controller = new AbortController();
await watch(controller.signal);
await listen(controller.signal);
await kv
  .atomic()
  .set(["test", 2], "123", { expireIn: 900 })
  .set(["test", 1], "123", { expireIn: 100 })
  .enqueue(["test-queue"], "boom", { delay: 700 })
  .commit();
await read();
await Bun.sleep(500);
await kv.atomic().enqueue(["test-queue"], "boom2").commit();
await read();
await Bun.sleep(1000);
await read();
await Bun.sleep(1000);
controller.abort();

// console.log(res.status, res.statusText);
