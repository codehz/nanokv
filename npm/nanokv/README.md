# NanoKV

A Minimal and persistent key-value database.

> [!WARNING]
> Note: Since there is currently no security mechanism, it is not recommended to use NanoKV directly on the front end

## Example usage for js

```ts
import { NanoKV, type KvEntry, type KvQueueEntry } from "nanokv";

type EntryType =
  | KvEntry<["user", id: number, "name"], string>
  | KvEntry<["user", id: number, "age"], number>;
type QueueType =
  | KvQueueEntry<["user-joined"], number>
  | KvQueueEntry<["user-left"], number>;
const kv = new NanoKV<EntryType, QueueType>("http://127.0.0.1:2256");

await kv.set(["user", 1, "name"], "admin");
```