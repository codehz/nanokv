// automatically generated by the FlatBuffers compiler, do not modify

/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/no-explicit-any, @typescript-eslint/no-non-null-assertion */

import * as flatbuffers from 'flatbuffers';

export class ReadRange {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):ReadRange {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsReadRange(bb:flatbuffers.ByteBuffer, obj?:ReadRange):ReadRange {
  return (obj || new ReadRange()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsReadRange(bb:flatbuffers.ByteBuffer, obj?:ReadRange):ReadRange {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new ReadRange()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

start(index: number):number|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.readUint8(this.bb!.__vector(this.bb_pos + offset) + index) : 0;
}

startLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

startArray():Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? new Uint8Array(this.bb!.bytes().buffer, this.bb!.bytes().byteOffset + this.bb!.__vector(this.bb_pos + offset), this.bb!.__vector_len(this.bb_pos + offset)) : null;
}

end(index: number):number|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readUint8(this.bb!.__vector(this.bb_pos + offset) + index) : 0;
}

endLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

endArray():Uint8Array|null {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? new Uint8Array(this.bb!.bytes().buffer, this.bb!.bytes().byteOffset + this.bb!.__vector(this.bb_pos + offset), this.bb!.__vector_len(this.bb_pos + offset)) : null;
}

limit():number {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? this.bb!.readUint32(this.bb_pos + offset) : 0;
}

exact():boolean {
  const offset = this.bb!.__offset(this.bb_pos, 10);
  return offset ? !!this.bb!.readInt8(this.bb_pos + offset) : false;
}

reverse():boolean {
  const offset = this.bb!.__offset(this.bb_pos, 12);
  return offset ? !!this.bb!.readInt8(this.bb_pos + offset) : false;
}

static startReadRange(builder:flatbuffers.Builder) {
  builder.startObject(5);
}

static addStart(builder:flatbuffers.Builder, startOffset:flatbuffers.Offset) {
  builder.addFieldOffset(0, startOffset, 0);
}

static createStartVector(builder:flatbuffers.Builder, data:number[]|Uint8Array):flatbuffers.Offset {
  builder.startVector(1, data.length, 1);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addInt8(data[i]!);
  }
  return builder.endVector();
}

static startStartVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(1, numElems, 1);
}

static addEnd(builder:flatbuffers.Builder, endOffset:flatbuffers.Offset) {
  builder.addFieldOffset(1, endOffset, 0);
}

static createEndVector(builder:flatbuffers.Builder, data:number[]|Uint8Array):flatbuffers.Offset {
  builder.startVector(1, data.length, 1);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addInt8(data[i]!);
  }
  return builder.endVector();
}

static startEndVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(1, numElems, 1);
}

static addLimit(builder:flatbuffers.Builder, limit:number) {
  builder.addFieldInt32(2, limit, 0);
}

static addExact(builder:flatbuffers.Builder, exact:boolean) {
  builder.addFieldInt8(3, +exact, +false);
}

static addReverse(builder:flatbuffers.Builder, reverse:boolean) {
  builder.addFieldInt8(4, +reverse, +false);
}

static endReadRange(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createReadRange(builder:flatbuffers.Builder, startOffset:flatbuffers.Offset, endOffset:flatbuffers.Offset, limit:number, exact:boolean, reverse:boolean):flatbuffers.Offset {
  ReadRange.startReadRange(builder);
  ReadRange.addStart(builder, startOffset);
  ReadRange.addEnd(builder, endOffset);
  ReadRange.addLimit(builder, limit);
  ReadRange.addExact(builder, exact);
  ReadRange.addReverse(builder, reverse);
  return ReadRange.endReadRange(builder);
}
}
