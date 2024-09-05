import { NanoKV, type KvEntry } from ".";

const kv = new NanoKV<KvEntry<[number, number], number>>(
  "http://127.0.0.1:2256"
);

for (let i = 0; i < 100; i++) {
  const atomic = kv.atomic();
  for (let j = 0; j < 1000; j++) {
    atomic.set([i, j], 1, { expireIn: Math.ceil(10 * Math.random()) });
  }
  await atomic.commit();
  await Bun.sleep(20);
}
