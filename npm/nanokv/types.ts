export type KvKey = readonly KvKeyPart[];
export type KvKeyPart = Uint8Array | string | number | bigint | boolean;
export type KvEntryMaybe<K extends KvKey = KvKey, V = any> =
  | KvEntry<K, V>
  | { key: K; value: undefined; version: undefined };
export type KvEntry<K extends KvKey = KvKey, V = any> = {
  key: K;
  value: V;
  version: bigint;
};
export type KvQueueEntry<K extends KvKey = KvKey, V = any> = {
  key: K;
  value: V;
  schedule: number;
  sequence: bigint;
};
export type KvListSelector<K extends KvKey = KvKey> =
  | { prefix: K }
  | { prefix: K; start: K }
  | { prefix: K; end: K }
  | { start: K; end: K };

export type KvListOptions = {
  limit?: number;
  reverse?: boolean;
  batchSize?: number;
  cursor?: KvKey;
};

export type KvCheck<K extends KvKey> = { key: K; version: bigint | undefined };
