# NanoKV

[![typedoc](https://github.com/codehz/nanokv/actions/workflows/typedoc.yml/badge.svg)](https://github.com/codehz/nanokv/actions/workflows/typedoc.yml)

A Minimal and persistent key-value database. Inspired by denokv, so it will has some similar api (but not 1-to-1 copy).

DOCS: <https://codehz.github.io/nanokv>

## Deploy guide

Use docker: ghcr.io/codehz/nanokv
```shell
docker run -p 2256:2256 -v /data/db:db ghcr.io/codehz/nanokv
```

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

## Why I build this?

I am a user of denokv, and I think denokv is a great product, especially when used in the hosting environment provided by deno.com. Compared to other similar key-value products, it has many excellent features (such as support for atomic operations on one side).

However, I have also found some limitations of denokv, which means it does not meet my needs for some projects:
1. The limit on the number of atomic operation checks is 10. I understand that this limit might benefit performance, but 10 is too small, and there is no way to increase it in the self-hosted version.
2. The limit on the value size is 64KiB, which is unacceptable for my use case. My requirement for a KV is to be able to store values of at least 20MiB or more without needing to be sharded.
3. The enqueue and listenQueue functions are not available in the self-hosted version. I think this is a very useful feature, so I have re-implemented them in my projects (and at the same time changed the API to support the use of multiple queues, which I also believe is a deficiency in denokv).

I feel that my needs may not align with the direction of denokv's product development, so I have decided to implement a substitute with similar APIs to meet my own needs.
