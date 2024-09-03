// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

import { ReadRange } from '../../nanokv/packet/read-range.js';


export class SnapshotRead {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):SnapshotRead {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsSnapshotRead(bb:flatbuffers.ByteBuffer, obj?:SnapshotRead):SnapshotRead {
  return (obj || new SnapshotRead()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsSnapshotRead(bb:flatbuffers.ByteBuffer, obj?:SnapshotRead):SnapshotRead {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new SnapshotRead()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

requests(index: number, obj?:ReadRange):ReadRange|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? (obj || new ReadRange()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

requestsLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startSnapshotRead(builder:flatbuffers.Builder) {
  builder.startObject(1);
}

static addRequests(builder:flatbuffers.Builder, requestsOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, requestsOffset, 0);
}

static createRequestsVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startRequestsVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endSnapshotRead(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static finishSnapshotReadBuffer(builder:flatbuffers.Builder, offset:flatbuffers.Offset) {
  builder.finish(offset);
}

static finishSizePrefixedSnapshotReadBuffer(builder:flatbuffers.Builder, offset:flatbuffers.Offset) {
  builder.finish(offset, undefined, true);
}

static createSnapshotRead(builder:flatbuffers.Builder, requestsOffset:flatbuffers.Offset):flatbuffers.Offset {
  SnapshotRead.startSnapshotRead(builder);
  SnapshotRead.addRequests(builder, requestsOffset);
  return SnapshotRead.endSnapshotRead(builder);
}
}
