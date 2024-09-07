import { NanoKV } from ".";
import type { KvSubspace } from "./type_helpers";
import type { KvEntry } from "./types";

type KVType = KvSubspace<
  ["base"],
  KvEntry<["number", number], number> | KvEntry<["string", string], string>
>;

const kv = new NanoKV<KVType>("http://127.0.0.1:2256");

const sub = kv.subspace(["base"]);

await sub.set(["number", 1], 2);
const value = await sub.get(["number", 0]);
console.log(value);
