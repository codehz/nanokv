import { MutationType } from "./packet";
import type { RawCheck, RawDequeue, RawEnqueue, RawMutation } from "./protocol";
import type {
  DistributiveProp,
  KvKeyToObject,
  ValueFotKvPair,
} from "./type_helpers";
import type {
  AtomicCheck,
  KvCommitError,
  KvCommitResult,
  KvEntry,
  KvKey,
  KvQueueEntry,
} from "./types";

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
 */
export interface AtomicOperation<
  E extends KvEntry = KvEntry,
  Q extends KvQueueEntry = KvQueueEntry
> {
  /**
   * Add to the operation a check that ensures that the versionstamp of the key-value pair in the KV store matches the given versionstamp.
   * If the check fails, the entire operation will fail and no mutations will be performed during the commit.
   */
  check(...entries: AtomicCheck<E["key"]>[]): this;
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
    value: ValueFotKvPair<E, K>,
    options?: { expireIn?: number }
  ): this;
  /** Add to the operation a mutation that deletes the specified key if all checks pass during the commit. */
  delete<K extends E["key"]>(key: K): this;
  /** Add to the operation a mutation that enqueues a value into the queue if all checks pass during the commit. */
  enqueue<K extends Q["key"]>(
    key: K,
    value: ValueFotKvPair<Q, K>,
    options?: { delay?: number; schedule?: number }
  ): this;
  /** Add to the operation a mutation that dequeues a value from the queue if all checks pass during the commit. */
  dequeue(
    ...keys: { key: Q["key"]; schedule: number; sequence: bigint }[]
  ): this;
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
  commit(): Promise<KvCommitResult | KvCommitError>;

  merge(operation: AtomicOperation): this;

  /** @hidden */
  dump(): {
    checks: RawCheck[];
    mutations: RawMutation[];
    enqueues: RawEnqueue[];
    dequeues: RawDequeue[];
  };
}

export class AtomicOperationImpl<E extends KvEntry, Q extends KvQueueEntry>
  implements AtomicOperation<E, Q>
{
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

  check(...entries: AtomicCheck<E["key"]>[]): this {
    this.#checks.push(...entries);
    return this;
  }

  set<K extends E["key"]>(
    key: K,
    value: ValueFotKvPair<E, K>,
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

  delete<K extends E["key"]>(key: K): this {
    this.#mutations.push({ key, type: MutationType.DELETE });
    return this;
  }

  enqueue<K extends Q["key"]>(
    key: K,
    value: ValueFotKvPair<Q, K>,
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

  commit(): Promise<KvCommitResult | KvCommitError> {
    return this.#committer({
      checks: this.#checks,
      mutations: this.#mutations,
      enqueues: this.#enqueues,
      dequeues: this.#dequeues,
    });
  }
  merge(operation: AtomicOperation): this {
    const { checks, mutations, enqueues, dequeues } = operation.dump();
    this.#checks.push(...checks);
    this.#mutations.push(...mutations);
    this.#enqueues.push(...enqueues);
    this.#dequeues.push(...dequeues);
    return this;
  }
  dump(): {
    checks: RawCheck[];
    mutations: RawMutation[];
    enqueues: RawEnqueue[];
    dequeues: RawDequeue[];
  } {
    return {
      checks: this.#checks,
      mutations: this.#mutations,
      enqueues: this.#enqueues,
      dequeues: this.#dequeues,
    };
  }
}

export class AtomicOperationProxy<E extends KvEntry, Q extends KvQueueEntry>
  implements AtomicOperation<E, Q>
{
  #operation: AtomicOperation;
  #prefix: KvKey;

  constructor(operation: AtomicOperation, prefix: KvKey) {
    this.#operation = operation;
    this.#prefix = prefix;
  }
  check(...entries: AtomicCheck<E["key"]>[]): this {
    this.#operation.check(
      ...entries.map((entry) => ({
        key: [...this.#prefix, ...entry.key],
        versionstamp: entry.versionstamp,
      }))
    );
    return this;
  }
  set<K extends E["key"]>(
    key: K,
    value: DistributiveProp<Extract<E, { key: KvKeyToObject<K> }>, "value">,
    options?: { expireIn?: number }
  ): this {
    this.#operation.set([...this.#prefix, ...key], value, options);
    return this;
  }
  delete<K extends E["key"]>(key: K): this {
    this.#operation.delete([...this.#prefix, ...key]);
    return this;
  }
  enqueue<K extends Q["key"]>(
    key: K,
    value: DistributiveProp<Extract<Q, { key: KvKeyToObject<K> }>, "value">,
    options?: { delay?: number; schedule?: number }
  ): this {
    this.#operation.enqueue([...this.#prefix, ...key], value, options);
    return this;
  }
  dequeue(
    ...keys: { key: Q["key"]; schedule: number; sequence: bigint }[]
  ): this {
    this.#operation.dequeue(
      ...keys.map((key) => ({ ...key, key: [...this.#prefix, ...key.key] }))
    );
    return this;
  }
  commit(): Promise<KvCommitResult | KvCommitError> {
    return this.#operation.commit();
  }
  merge(operation: AtomicOperation): this {
    this.#operation.merge(operation);
    return this;
  }
  dump(): {
    checks: RawCheck[];
    mutations: RawMutation[];
    enqueues: RawEnqueue[];
    dequeues: RawDequeue[];
  } {
    return this.#operation.dump();
  }
}
