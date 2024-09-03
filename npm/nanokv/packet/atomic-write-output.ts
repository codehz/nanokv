// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

export class AtomicWriteOutput {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):AtomicWriteOutput {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsAtomicWriteOutput(bb:flatbuffers.ByteBuffer, obj?:AtomicWriteOutput):AtomicWriteOutput {
  return (obj || new AtomicWriteOutput()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsAtomicWriteOutput(bb:flatbuffers.ByteBuffer, obj?:AtomicWriteOutput):AtomicWriteOutput {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new AtomicWriteOutput()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

ok():boolean {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? !!this.bb!.readInt8(this.bb_pos + offset) : false;
}

version():bigint {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readUint64(this.bb_pos + offset) : BigInt('0');
}

static startAtomicWriteOutput(builder:flatbuffers.Builder) {
  builder.startObject(2);
}

static addOk(builder:flatbuffers.Builder, ok:boolean) {
  builder.addFieldInt8(0, +ok, +false);
}

static addVersion(builder:flatbuffers.Builder, version:bigint) {
  builder.addFieldInt64(1, version, BigInt('0'));
}

static endAtomicWriteOutput(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createAtomicWriteOutput(builder:flatbuffers.Builder, ok:boolean, version:bigint):flatbuffers.Offset {
  AtomicWriteOutput.startAtomicWriteOutput(builder);
  AtomicWriteOutput.addOk(builder, ok);
  AtomicWriteOutput.addVersion(builder, version);
  return AtomicWriteOutput.endAtomicWriteOutput(builder);
}
}
