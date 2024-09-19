import { NanoKV, type KvEntry, type KvQueueEntry, type KvSubspace } from ".";

const kv = new NanoKV<
  KvSubspace<["test"], KvEntry<[number], string>>,
  KvQueueEntry<["test-queue"], string>
>("http://127.0.0.1:2256");

async function watch(signal: AbortSignal) {
  const watcher = kv.watch([
    ["test", 1],
    ["test", 2],
  ]);
  signal.addEventListener("abort", () => {
    watcher.close();
  });
  (async () => {
    while (true) {
      const value = await watcher.read();
      console.log("update", value);
    }
  })().catch(() => {});
}

async function listen(signal: AbortSignal) {
  const watcher = kv.listenQueue(["test-queue"]);
  signal.addEventListener("abort", () => {
    watcher.close();
  });
  (async () => {
    while (true) {
      const value = await watcher.readMany();
      console.log("listen", value);
      await kv
        .atomic()
        .dequeue(...value)
        .commit();
    }
  })();
}

async function read() {
  console.log(
    await kv.getMany([
      ["test", 1],
      ["test", 2],
    ])
  );
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
