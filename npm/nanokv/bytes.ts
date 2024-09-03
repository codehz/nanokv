// Copyright 2023 the Deno authors. All rights reserved. MIT license.

export function checkEnd(bytes: Uint8Array, pos: number) {
  const extra = bytes.length - pos;
  if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function flipBytes(arr: Uint8Array | number[], start = 0): void {
  for (let i = start; i < arr.length; i++) {
    arr[i] = 0xff - arr[i];
  }
}

export function computeBigintMinimumNumberOfBytes(val: bigint): number {
  let n = 0;
  while (val !== 0n) {
    val >>= 8n;
    n++;
  }
  return n;
}
