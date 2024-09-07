import type { RemoveTuplePrefix, TuplePrefix } from "./type_helpers";

/**
 * A key to be persisted in a {@link NanoKV}. A key is a sequence of {@link KvKeyPart}s.
 *
 * Keys are ordered lexicographically by their parts.
 * The first part is the most significant, and the last part is the least significant.
 * The order of the parts is determined by both the type and the value of the part.
 * The relative significance of the types can be found in documentation for the {@link KvKeyPart} type.
 *
 * Keys have a maximum size of 2048 bytes serialized.
 * If the size of the key exceeds this limit, an error will be thrown on the operation that this key was passed to.
 */
export type KvKey = KvKeyPart[];
/**
 * A single part of a {@link KvKey}.
 * Parts are ordered lexicographically, first by their type, and within a given type by their value.
 *
 * The ordering of types is as follows:
 * 1. Uint8Array
 * 2. string
 * 3. number
 * 4. bigint
 * 5. boolean
 *
 * Within a given type, the ordering is as follows:
 * * Uint8Array is ordered by the byte ordering of the array
 * * string is ordered by the byte ordering of the UTF-8 encoding of the string
 * * number is ordered following this pattern: -NaN < -Infinity < -100.0 < -1.0 < -0.5 < -0.0 < 0.0 < 0.5 < 1.0 < 100.0 < Infinity < NaN
 * * bigint is ordered by mathematical ordering, with the largest negative number being the least first value, and the largest positive number being the last value
 * * boolean is ordered by false < true
 *
 * This means that the part 1.0 (a number) is ordered before the part 2.0 (also a number),
 * but is greater than the part 0n (a bigint), because 1.0 is a number and 0n is a bigint,
 * and type ordering has precedence over the ordering of values within a type.
 */
export type KvKeyPart = Uint8Array | string | number | bigint | boolean;
/**
 * An optional versioned pair of key and value in a {@link NanoKV}.
 * This is the same as a KvEntry, but the value and versionstamp fields may be null if no value exists for the given key in the KV store.
 */
export type KvEntryMaybe =
  | KvEntry
  | { key: KvKey; value: null; versionstamp: null };

export type KvPair<K = KvKey, V = any> = { key: K; value: V };

/**
 * A versioned pair of key and value in a {@link NanoKV}.
 *
 * The versionstamp is a string that represents the current version of the key-value pair.
 * It can be used to perform atomic operations on the KV store by passing it to the check method of a {@link AtomicOperation}.
 */
export type KvEntry<K = KvKey, V = any> = KvPair<K, V> & {
  versionstamp: bigint;
};
/**
 * A pair of key and value in a {@link NanoKV} for queue operations.
 *
 * Returned from listenQueue method, you should not manually construct values ​​of this type.
 */
export type KvQueueEntry<K = KvKey, V = any> = KvPair<K, V> & {
  schedule: number;
  sequence: bigint;
};

/**
 * A selector that selects the range of data returned by a list operation on a {@link NanoKV}.
 *
 * The selector can either be a prefix selector or a range selector.
 * A prefix selector selects all keys that start with the given prefix (optionally starting at a given key).
 * A range selector selects all keys that are lexicographically between the given start and end keys.
 */
export type KvListSelector<K extends KvKey, P extends TuplePrefix<K>> =
  | { prefix: P }
  | { prefix: P; start: TuplePrefix<RemoveTuplePrefix<K, P>> }
  | { prefix: P; end: TuplePrefix<RemoveTuplePrefix<K, P>> }
  | {
      start: TuplePrefix<RemoveTuplePrefix<K, P>>;
      end: TuplePrefix<RemoveTuplePrefix<K, P>>;
    };

/** Options for listing key-value pairs in a {@link NanoKV}. */
export type KvListOptions = {
  /**
   * The maximum number of key-value pairs to return.
   * If not specified, all matching key-value pairs will be returned.
   */
  limit?: number;
  /**
   * Whether to reverse the order of the returned key-value pairs.
   * If not specified, the order will be ascending from the start of the range as per the lexicographical ordering of the keys.
   * If true, the order will be descending from the end of the range.
   * @default false. */
  reverse?: boolean;
  /**
   * The size of the batches in which the list operation is performed.
   * Larger or smaller batch sizes may positively or negatively affect the performance of a list operation depending on the specific use case and iteration behavior.
   * Slow iterating queries may benefit from using a smaller batch size for increased overall consistency, while fast iterating queries may benefit from using a larger batch size for better performance.
   *
   * The default batch size is equal to the limit option, or 128 if this is unset.
   * The maximum value for this option is 1024. Larger values will be clamped.
   */
  batchSize?: number;
  /**
   * The cursor to resume the iteration from.
   * If not specified, the iteration will start from the beginning.
   */
  cursor?: KvKey;
};

export type AtomicCheck<K extends KvKey> = {
  key: K;
  versionstamp: bigint | null;
};

export type KvCommitResult = {
  ok: true;
  versionstamp: bigint;
};
export type KvCommitError = {
  ok: false;
  versionstamp: null;
};
