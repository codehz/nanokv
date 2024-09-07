/**
 * This file contains type helpers for the NanoKV library.
 *
 * Note: This module was not exported from the library.
 * This docs exists purely for documentation purposes.
 * @module
 */

import type { KvPair, KvKey } from "./types";

export type Ints = `${number}`;

/** Convert ["a", "b"] in {0: "a", 1: "b"} so that we can use Extract to match tuple prefixes. */
export type KvKeyToObject<T extends KvKey> = Pick<T, Extract<keyof T, Ints>>;

export type SelectKvKeyByPrefix<S extends KvKey, P extends KvKey> = Extract<
  S,
  KvKeyToObject<P>
>;

export type SelectKvPairByPrefix<S extends KvPair, P extends KvKey> = Extract<
  S,
  { key: KvKeyToObject<P> }
>;

export type SelectKvPair<S extends KvPair, P extends KvKey> = Extract<
  S,
  { key: P }
>;

export type DistributiveProp<T, K extends keyof T> = T extends unknown
  ? T[K]
  : never;

export type ValueFotKvPair<
  S extends KvPair,
  P extends KvKey
> = DistributiveProp<SelectKvPairByPrefix<S, P>, "value">;

export type IsTuple = [] | { 0: any };

export type KvKeyPrefix<T extends KvKey> = T extends IsTuple
  ? T extends [any, ...infer U extends KvKey]
    ? [] | [T[0]] | [T[0], ...KvKeyPrefix<U>]
    : []
  : T | [];

export type KvKeyRest<T extends KvKey> = T extends [any, ...infer U]
  ? U
  : never;
export type RemoveKvKeyPrefix<T, P extends KvKey> = T extends IsTuple
  ? T extends [...P, ...infer U]
    ? U
    : never
  : T;

export type RemoveKvPairPrefix<T extends KvPair, P extends KvKey> = T extends {
  key: [...P, ...infer U];
  value: infer V;
}
  ? { key: U; value: V } & Omit<T, "key" | "value">
  : never;

export type KvSubspace<P extends KvKey, T extends KvPair> = T extends unknown
  ? { key: [...P, ...T["key"]]; value: T["value"] } & Omit<T, "key" | "value">
  : never;
