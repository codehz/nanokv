// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

import { WatchKey } from '../../nanokv/packet/watch-key.js';


export class Watch {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):Watch {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsWatch(bb:flatbuffers.ByteBuffer, obj?:Watch):Watch {
  return (obj || new Watch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsWatch(bb:flatbuffers.ByteBuffer, obj?:Watch):Watch {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new Watch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

id():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.readInt32(this.bb_pos + offset) : 0;
}

keys(index: number, obj?:WatchKey):WatchKey|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? (obj || new WatchKey()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

keysLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startWatch(builder:flatbuffers.Builder) {
  builder.startObject(2);
}

static addId(builder:flatbuffers.Builder, id:number) {
  builder.addFieldInt32(0, id, 0);
}

static addKeys(builder:flatbuffers.Builder, keysOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, keysOffset, 0);
}

static createKeysVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startKeysVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endWatch(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createWatch(builder:flatbuffers.Builder, id:number, keysOffset:flatbuffers.Offset):flatbuffers.Offset {
  Watch.startWatch(builder);
  Watch.addId(builder, id);
  Watch.addKeys(builder, keysOffset);
  return Watch.endWatch(builder);
}
}
