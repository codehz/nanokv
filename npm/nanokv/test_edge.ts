import { NanoKV, type KvEntry } from ".";

const kv = new NanoKV<KvEntry<[number, number], number>>("http://127.0.0.1:2256");

await kv.set([1, 2], 1, { expireIn: 3000 });
await kv.set([1, 3], 2, { expireIn: 3000 });
await kv.set([1, 4], 2, { expireIn: 3000 });

for await (const item of kv.list(
  { start: [1], end: [1, 4] },
  { reverse: false }
)) {
  console.log(item);
}
