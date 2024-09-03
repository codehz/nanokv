import { serialize, deserialize } from "bun:jsc";
import { ValueEncoding } from "./shared/value-encoding";

export function encodeKvValue(value: unknown): [Uint8Array, ValueEncoding] {
  if (value instanceof Uint8Array) {
    return [value, ValueEncoding.BYTES];
  }
  return [
    serialize(value, { binaryType: "nodebuffer" }),
    ValueEncoding.SERIALIZED,
  ];
}

export function decodeKvValue(
  value: Uint8Array,
  encoding: ValueEncoding
): unknown {
  if (encoding === ValueEncoding.BYTES) {
    return value;
  }
  return deserialize(value);
}
