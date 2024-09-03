// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

export class Dequeue {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):Dequeue {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsDequeue(bb:flatbuffers.ByteBuffer, obj?:Dequeue):Dequeue {
  return (obj || new Dequeue()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsDequeue(bb:flatbuffers.ByteBuffer, obj?:Dequeue):Dequeue {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new Dequeue()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

key(index: number):number|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.readUint8(this.bb!.__vector(this.bb_pos + offset) + index) : 0;
}

keyLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

keyArray():Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? new Uint8Array(this.bb!.bytes().buffer, this.bb!.bytes().byteOffset + this.bb!.__vector(this.bb_pos + offset), this.bb!.__vector_len(this.bb_pos + offset)) : null;
}

schedule():bigint {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readUint64(this.bb_pos + offset) : BigInt('0');
}

sequence():bigint {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? this.bb!.readUint64(this.bb_pos + offset) : BigInt('0');
}

static startDequeue(builder:flatbuffers.Builder) {
  builder.startObject(3);
}

static addKey(builder:flatbuffers.Builder, keyOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, keyOffset, 0);
}

static createKeyVector(builder:flatbuffers.Builder, data:number[]|Uint8Array):flatbuffers.Offset {
  builder.startVector(1, data.length, 1);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addInt8(data[i]!);
  }
  return builder.endVector();
}

static startKeyVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(1, numElems, 1);
}

static addSchedule(builder:flatbuffers.Builder, schedule:bigint) {
  builder.addFieldInt64(1, schedule, BigInt('0'));
}

static addSequence(builder:flatbuffers.Builder, sequence:bigint) {
  builder.addFieldInt64(2, sequence, BigInt('0'));
}

static endDequeue(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createDequeue(builder:flatbuffers.Builder, keyOffset:flatbuffers.Offset, schedule:bigint, sequence:bigint):flatbuffers.Offset {
  Dequeue.startDequeue(builder);
  Dequeue.addKey(builder, keyOffset);
  Dequeue.addSchedule(builder, schedule);
  Dequeue.addSequence(builder, sequence);
  return Dequeue.endDequeue(builder);
}
}
