// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { ArrayBufferSink } from "bun";
import {
  checkEnd,
  computeBigintMinimumNumberOfBytes,
  flipBytes,
} from "./bytes";
import type { KvKey, KvKeyPart } from "./types";

export function packKey(kvKey: KvKey): Uint8Array {
  const sink = new ArrayBufferSink();
  sink.start({ asUint8Array: true });
  for (const kvKeyPart of kvKey) {
    writeKeyPart(sink, kvKeyPart);
    // sink.write(packKeyPart(kvKeyPart));
  }
  return sink.end() as Uint8Array;
}
function writeKeyPart(sink: ArrayBufferSink, kvKeyPart: KvKeyPart) {
  if (kvKeyPart instanceof Uint8Array) {
    sink.write(single(Typecode.ByteString));
    writeZeroWithZeroFF(sink, kvKeyPart);
    sink.write(single(0));
  } else if (typeof kvKeyPart === "string") {
    sink.write(single(Typecode.UnicodeString));
    writeZeroWithZeroFF(sink, new TextEncoder().encode(kvKeyPart));
    sink.write(single(0));
  } else if (kvKeyPart === false) {
    sink.write(single(Typecode.False));
  } else if (kvKeyPart === true) {
    sink.write(single(Typecode.True));
  } else if (typeof kvKeyPart === "bigint") {
    writeBigint(sink, kvKeyPart);
  } else if (typeof kvKeyPart === "number") {
    const sub = new Uint8Array(8);
    new DataView(sub.buffer).setFloat64(0, -Math.abs(kvKeyPart), false);
    if (kvKeyPart < 0) flipBytes(sub);
    sink.write(single(Typecode.FloatingPointDouble));
    sink.write(sub);
  }
}
function writeBigint(sink: ArrayBufferSink, bigint: bigint) {
  const neg = bigint < 0;
  const abs = neg ? -bigint : bigint;
  const numBytes = BigInt(computeBigintMinimumNumberOfBytes(abs));

  const typecode = neg
    ? numBytes <= 8n
      ? Typecode.IntegerOneByteNegative - Number(numBytes) + 1
      : Typecode.IntegerArbitraryByteNegative
    : numBytes <= 8n
    ? Typecode.IntegerOneBytePositive + Number(numBytes) - 1
    : Typecode.IntegerArbitraryBytePositive;
  sink.write(single(typecode));
  const char = neg ? negative : single;
  if (numBytes > 8n) sink.write(char(Number(numBytes)));
  for (let i = 0n; i < numBytes; i++) {
    const mask = 0xffn << (8n * (numBytes - i - 1n));
    const byte = Number((abs & mask) >> (8n * (numBytes - i - 1n)));
    sink.write(char(byte));
  }
}
const shared = new Uint8Array(1);
function single(char: number) {
  shared[0] = char;
  return shared;
}
function negative(char: number) {
  shared[0] = 0xff - char;
  return shared;
}
function writeZeroWithZeroFF(sink: ArrayBufferSink, bytes: Uint8Array) {
  const index = bytes.indexOf(0);
  if (index < 0) {
    sink.write(bytes);
    return;
  }
  const zeroff = new Uint8Array([0, 0xff]);
  for (const byte of bytes) {
    if (byte === 0) {
      sink.write(zeroff);
    } else {
      sink.write(single(byte));
    }
  }
}
export function packKeyPart(kvKeyPart: KvKeyPart): Uint8Array {
  if (kvKeyPart instanceof Uint8Array) {
    return new Uint8Array([
      Typecode.ByteString,
      ...encodeZeroWithZeroFF(kvKeyPart),
      0,
    ]);
  }
  if (typeof kvKeyPart === "string") {
    return new Uint8Array([
      Typecode.UnicodeString,
      ...encodeZeroWithZeroFF(new TextEncoder().encode(kvKeyPart)),
      0,
    ]);
  }
  if (kvKeyPart === false) return new Uint8Array([Typecode.False]);
  if (kvKeyPart === true) return new Uint8Array([Typecode.True]);
  if (typeof kvKeyPart === "bigint") {
    const neg = kvKeyPart < 0;
    const abs = neg ? -kvKeyPart : kvKeyPart;
    const numBytes = BigInt(computeBigintMinimumNumberOfBytes(abs));

    const typecode = neg
      ? numBytes <= 8n
        ? Typecode.IntegerOneByteNegative - Number(numBytes) + 1
        : Typecode.IntegerArbitraryByteNegative
      : numBytes <= 8n
      ? Typecode.IntegerOneBytePositive + Number(numBytes) - 1
      : Typecode.IntegerArbitraryBytePositive;
    const bytes: number[] = [typecode];
    if (numBytes > 8n) bytes.push(Number(numBytes));
    for (let i = 0n; i < numBytes; i++) {
      const mask = 0xffn << (8n * (numBytes - i - 1n));
      const byte = Number((abs & mask) >> (8n * (numBytes - i - 1n)));
      bytes.push(byte);
    }
    if (neg) flipBytes(bytes, 1);
    return new Uint8Array(bytes);
  }
  if (typeof kvKeyPart === "number") {
    const sub = new Uint8Array(8);
    new DataView(sub.buffer).setFloat64(0, -Math.abs(kvKeyPart), false);
    if (kvKeyPart < 0) flipBytes(sub);
    return new Uint8Array([Typecode.FloatingPointDouble, ...sub]);
  }
  throw new Error(`Unsupported keyPart: ${typeof kvKeyPart} ${kvKeyPart}`);
}

export function unpackKey(bytes: Uint8Array): KvKey {
  const rt: KvKeyPart[] = [];
  let pos = 0;
  while (pos < bytes.length) {
    const typecode = bytes[pos++];
    if (
      typecode === Typecode.ByteString ||
      typecode === Typecode.UnicodeString
    ) {
      // Uint8Array or string
      const newBytes: number[] = [];
      while (pos < bytes.length) {
        const byte = bytes[pos++];
        if (byte === 0 && bytes[pos] === 0xff) {
          pos++;
        } else if (byte === 0) {
          break;
        }
        newBytes.push(byte);
      }
      rt.push(
        typecode === Typecode.UnicodeString
          ? decoder.decode(new Uint8Array(newBytes))
          : new Uint8Array(newBytes)
      );
    } else if (
      typecode >= Typecode.IntegerArbitraryByteNegative &&
      typecode <= Typecode.IntegerArbitraryBytePositive
    ) {
      // bigint
      const neg = typecode < Typecode.IntegerZero;
      const numBytes = BigInt(
        typecode === Typecode.IntegerArbitraryBytePositive ||
          typecode === Typecode.IntegerArbitraryByteNegative
          ? neg
            ? 0xff - bytes[pos++]
            : bytes[pos++]
          : Math.abs(typecode - Typecode.IntegerZero)
      );
      let val = 0n;
      for (let i = 0n; i < numBytes; i++) {
        let byte = bytes[pos++];
        if (neg) byte = 0xff - byte;
        val += BigInt(byte) << ((numBytes - i - 1n) * 8n);
      }
      rt.push(neg ? -val : val);
    } else if (typecode === Typecode.FloatingPointDouble) {
      // number
      const sub = new Uint8Array(bytes.subarray(pos, pos + 8));
      const neg = sub[0] < 128;
      if (neg) flipBytes(sub);
      const num = -new DataView(sub.buffer).getFloat64(0, false);
      pos += 8;
      rt.push(neg ? -num : num);
    } else if (typecode === Typecode.False) {
      // boolean false
      rt.push(false);
    } else if (typecode === Typecode.True) {
      // boolean true
      rt.push(true);
    } else {
      throw new Error(
        `Unsupported typecode: ${typecode} in key: [${bytes.join(
          ", "
        )}] after ${rt.join(", ")}`
      );
    }
  }
  checkEnd(bytes, pos);
  return rt;
}

const decoder = new TextDecoder();

const enum Typecode {
  ByteString = 0x01,
  UnicodeString = 0x02,
  IntegerArbitraryByteNegative = 0x0b,
  IntegerEightByteNegative = 0x0c,
  IntegerSevenByteNegative = 0x0d,
  IntegerSixByteNegative = 0x0e,
  IntegerFiveByteNegative = 0x0f,
  IntegerFourByteNegative = 0x10,
  IntegerThreeByteNegative = 0x11,
  IntegerTwoByteNegative = 0x12,
  IntegerOneByteNegative = 0x13,
  IntegerZero = 0x14,
  IntegerOneBytePositive = 0x15,
  IntegerTwoBytePositive = 0x16,
  IntegerThreeBytePositive = 0x17,
  IntegerFourBytePositive = 0x18,
  IntegerFiveBytePositive = 0x19,
  IntegerSixBytePositive = 0x1a,
  IntegerSevenBytePositive = 0x1b,
  IntegerEightBytePositive = 0x1c,
  IntegerArbitraryBytePositive = 0x1d,
  FloatingPointDouble = 0x21, // IEEE Binary Floating Point double (64 bits)
  False = 0x26,
  True = 0x27,
}

function encodeZeroWithZeroFF(bytes: Uint8Array): Uint8Array {
  const index = bytes.indexOf(0);
  return index < 0
    ? bytes
    : new Uint8Array([...bytes].flatMap((v) => (v === 0 ? [0, 0xff] : [v])));
}
