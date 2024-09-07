import type { KvPair, KvKey } from "./types";

type Ints = `${number}`;

/** Convert ["a", "b"] in {0: "a", 1: "b"} so that we can use Extract to match tuple prefixes. */
type TupleToObject<T extends any[]> = Pick<T, Extract<keyof T, Ints>>;

export type FilterTupleByPrefix<S extends KvKey, P extends KvKey> = Extract<
  S,
  TupleToObject<P>
>;

export type FilterTupleValuePairByPrefix<
  S extends KvPair,
  P extends KvKey
> = Extract<S, { key: TupleToObject<P> }>;

export type FilterTupleValuePair<S extends KvPair, P extends KvKey> = Extract<
  S,
  { key: P }
>;

export type DistributiveProp<T, K extends keyof T> = T extends unknown
  ? T[K]
  : never;

export type ValueForTuple<S extends KvPair, P extends KvKey> = DistributiveProp<
  FilterTupleValuePairByPrefix<S, P>,
  "value"
>;

type IsTuple = [] | { 0: any };

export type TuplePrefix<T extends unknown[]> = T extends IsTuple
  ? T extends [any, ...infer U]
    ? [] | [T[0]] | [T[0], ...TuplePrefix<U>]
    : []
  : T | [];

export type TupleRest<T extends unknown[]> = T extends [any, ...infer U]
  ? U
  : never;
export type RemoveTuplePrefix<T, P extends any[]> = T extends IsTuple
  ? T extends [...P, ...infer U]
    ? U
    : never
  : T;

export type RemoveTupleValuePairPrefix<
  T extends KvPair,
  P extends any[]
> = T extends { key: [...P, ...infer U]; value: infer V }
  ? { key: U; value: V }
  : never;

export type SchemaSubspace<
  P extends KvKey,
  T extends KvPair
> = T extends unknown ? { key: [...P, ...T["key"]]; value: T["value"] } : never;
