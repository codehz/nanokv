import * as flatbuffers from "flatbuffers";
import { packKey, unpackKey } from "./kv_key";
import { decodeKvValue, encodeKvValue } from "./kv_value";
import {
  AtomicWrite,
  AtomicWriteOutput,
  Check,
  Dequeue,
  Enqueue,
  Listen,
  ListenKey,
  ListenOutput,
  Mutation,
  ReadRange,
  SnapshotRead,
  SnapshotReadOutput,
  Watch,
  WatchKey,
  WatchOutput,
  type MutationType,
} from "./packet";
import type { KvKey } from "./types";

export type RawReadRange = {
  start: KvKey | Uint8Array;
  end?: KvKey | Uint8Array;
  limit?: number;
  exact?: boolean;
  reverse?: boolean;
};

export type RawKvEntry = {
  key: KvKey;
  value: any;
  version: bigint | undefined;
};

export type RawCheck = { key: KvKey; version?: bigint };
export type RawMutation = {
  type: MutationType;
  key: KvKey;
  value?: any;
  expired_at?: number;
};
export type RawEnqueue = { key: KvKey; value: any; schedule?: number };
export type RawDequeue = { key: KvKey; schedule: number; sequence: bigint };

export function encodeSnapshotRead(ranges: RawReadRange[]): Uint8Array {
  const builder = new flatbuffers.Builder();
  builder.finish(
    SnapshotRead.createSnapshotRead(
      builder,
      SnapshotRead.createRequestsVector(
        builder,
        ranges.map((range) => {
          const start = createKvKey(builder, range.start);
          const end = range.end ? createKvKey(builder, range.end) : undefined;
          ReadRange.startReadRange(builder);
          ReadRange.addStart(builder, start);
          if (end) ReadRange.addEnd(builder, end);
          if (range.limit) ReadRange.addLimit(builder, range.limit);
          if (range.exact) ReadRange.addExact(builder, range.exact);
          if (range.reverse) ReadRange.addReverse(builder, range.reverse);
          return ReadRange.endReadRange(builder);
        })
      )
    )
  );
  return builder.asUint8Array();
}

export function decodeSnapshotReadOutput(data: Uint8Array): RawKvEntry[][] {
  const buffer = new flatbuffers.ByteBuffer(data);
  const output = SnapshotReadOutput.getRootAsSnapshotReadOutput(buffer);
  const length = output.rangesLength();
  return Array.from({ length }, (_, i) => {
    const range = output.ranges(i)!;
    const length = range.valuesLength();
    return Array.from({ length }, (_, i) => {
      const item = range.values(i)!;
      const keyArray = item.keyArray();
      const key = keyArray ? unpackKey(keyArray) : [];
      const valueArray = item.valueArray();
      const value = valueArray
        ? decodeKvValue(valueArray, item.encoding())
        : undefined;
      return {
        key,
        value,
        version: item.version() == 0n ? undefined : item.version(),
      };
    });
  });
}

export function encodeAtomicWrite({
  checks = [],
  mutations = [],
  enqueues = [],
  dequeues = [],
}: {
  checks?: RawCheck[];
  mutations?: RawMutation[];
  enqueues?: RawEnqueue[];
  dequeues?: RawDequeue[];
}): Uint8Array {
  const builder = new flatbuffers.Builder();
  builder.finish(
    AtomicWrite.createAtomicWrite(
      builder,
      AtomicWrite.createChecksVector(
        builder,
        checks.map((check) =>
          Check.createCheck(
            builder,
            createKvKey(builder, check.key),
            check.version ?? 0n
          )
        )
      ),
      AtomicWrite.createMutationsVector(
        builder,
        mutations.map((mutation) => {
          const key = createKvKey(builder, mutation.key);
          let value = null;
          let encoding = null;
          if (mutation.value != null) {
            let valueBytes;
            [valueBytes, encoding] = encodeKvValue(mutation.value);
            value = builder.createByteVector(valueBytes);
          }
          Mutation.startMutation(builder);
          Mutation.addType(builder, mutation.type);
          Mutation.addKey(builder, key);
          if (value) Mutation.addValue(builder, value);
          if (encoding != null) Mutation.addEncoding(builder, encoding);
          if (mutation.expired_at != null)
            Mutation.addExpiredAt(builder, BigInt(mutation.expired_at));
          return Mutation.endMutation(builder);
        })
      ),
      AtomicWrite.createEnqueuesVector(
        builder,
        enqueues.map((enqueue) => {
          const [valueBytes, encoding] = encodeKvValue(enqueue.value);
          return Enqueue.createEnqueue(
            builder,
            createKvKey(builder, enqueue.key),
            builder.createByteVector(valueBytes),
            encoding,
            BigInt(enqueue.schedule ?? 0)
          );
        })
      ),
      AtomicWrite.createDequeuesVector(
        builder,
        dequeues.map((dequeue) =>
          Dequeue.createDequeue(
            builder,
            createKvKey(builder, dequeue.key),
            BigInt(dequeue.schedule),
            dequeue.sequence
          )
        )
      )
    )
  );
  return builder.asUint8Array();
}

export function decodeAtomicWriteOutput(data: Uint8Array): {
  ok: boolean;
  version: bigint;
} {
  const buffer = new flatbuffers.ByteBuffer(data);
  const output = AtomicWriteOutput.getRootAsAtomicWriteOutput(buffer);
  return { ok: output.ok(), version: output.version() };
}

export function encodeWatch({
  id,
  keys,
}: {
  id: number;
  keys: (KvKey | Uint8Array)[];
}): Uint8Array {
  const builder = new flatbuffers.Builder();
  builder.finish(
    Watch.createWatch(
      builder,
      id,
      Watch.createKeysVector(
        builder,
        keys.map((key) =>
          WatchKey.createWatchKey(builder, createKvKey(builder, key))
        )
      )
    )
  );
  return builder.asUint8Array();
}

export function decodeWatchOutput(data: Uint8Array) {
  const buffer = new flatbuffers.ByteBuffer(data);
  const output = WatchOutput.getRootAsWatchOutput(buffer);
  const id = output.id();
  const values = Array.from({ length: output.valuesLength() }, (_, i) => {
    const item = output.values(i)!;
    const keyArray = item.keyArray();
    const key = keyArray ? unpackKey(keyArray) : [];
    const valueArray = item.valueArray();
    const value = valueArray
      ? decodeKvValue(valueArray, item.encoding())
      : undefined;
    return {
      key,
      value,
      version: item.version() == 0n ? undefined : item.version(),
    };
  });
  return { id, values };
}

export function decodeWatchOutputRaw(data: Uint8Array) {
  const buffer = new flatbuffers.ByteBuffer(data);
  const output = WatchOutput.getRootAsWatchOutput(buffer);
  const id = output.id();
  const values = Array.from({ length: output.valuesLength() }, (_, i) => {
    const item = output.values(i)!;
    const keyArray = item.keyArray();
    const valueArray = item.valueArray();
    const value = valueArray
      ? decodeKvValue(valueArray, item.encoding())
      : undefined;
    return {
      key: keyArray!,
      value,
      version: item.version() == 0n ? undefined : item.version(),
    };
  });
  return { id, values };
}

export function encodeListen({
  added = [],
  removed = [],
}: {
  added?: (KvKey | Uint8Array)[];
  removed?: (KvKey | Uint8Array)[];
}) {
  const builder = new flatbuffers.Builder();
  builder.finish(
    Listen.createListen(
      builder,
      Listen.createAddedVector(
        builder,
        added.map((key) =>
          ListenKey.createListenKey(builder, createKvKey(builder, key))
        )
      ),
      Listen.createRemovedVector(
        builder,
        removed.map((key) =>
          ListenKey.createListenKey(builder, createKvKey(builder, key))
        )
      )
    )
  );
  return builder.asUint8Array();
}

export function decodeListenOutputRaw(data: Uint8Array): {
  key: Uint8Array;
  value?: any;
  schedule: number;
  sequence: bigint;
}[] {
  const buffer = new flatbuffers.ByteBuffer(data);
  const output = ListenOutput.getRootAsListenOutput(buffer);
  return Array.from({ length: output.entriesLength() }, (_, i) => {
    const item = output.entries(i)!;
    const keyArray = item.keyArray();
    const valueArray = item.valueArray();
    const value = valueArray
      ? decodeKvValue(valueArray, item.encoding())
      : undefined;
    return {
      key: keyArray!,
      value,
      schedule: Number(item.schedule()),
      sequence: item.sequence(),
    };
  });
}

function createKvKey(builder: flatbuffers.Builder, key: KvKey | Uint8Array) {
  return builder.createByteVector(
    key instanceof Uint8Array ? key : packKey(key)
  );
}
