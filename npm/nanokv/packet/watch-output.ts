// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

import { KvEntry } from '../../nanokv/packet/kv-entry.js';


export class WatchOutput {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):WatchOutput {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsWatchOutput(bb:flatbuffers.ByteBuffer, obj?:WatchOutput):WatchOutput {
  return (obj || new WatchOutput()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsWatchOutput(bb:flatbuffers.ByteBuffer, obj?:WatchOutput):WatchOutput {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new WatchOutput()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

id():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.readInt32(this.bb_pos + offset) : 0;
}

values(index: number, obj?:KvEntry):KvEntry|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? (obj || new KvEntry()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

valuesLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startWatchOutput(builder:flatbuffers.Builder) {
  builder.startObject(2);
}

static addId(builder:flatbuffers.Builder, id:number) {
  builder.addFieldInt32(0, id, 0);
}

static addValues(builder:flatbuffers.Builder, valuesOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, valuesOffset, 0);
}

static createValuesVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startValuesVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endWatchOutput(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createWatchOutput(builder:flatbuffers.Builder, id:number, valuesOffset:flatbuffers.Offset):flatbuffers.Offset {
  WatchOutput.startWatchOutput(builder);
  WatchOutput.addId(builder, id);
  WatchOutput.addValues(builder, valuesOffset);
  return WatchOutput.endWatchOutput(builder);
}
}
