/**
 * Copyright (c) 2019 Eric Thill
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this getFile except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import io.thill.jacoio.function.BiParametizedWriteFunction;
import io.thill.jacoio.function.ParametizedWriteFunction;
import io.thill.jacoio.function.TriParametizedWriteFunction;
import io.thill.jacoio.function.WriteFunction;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements {@link ConcurrentFile} to provide multi-process writing using a 32-byte header for offset coordination. Multiple processes can write to the getFile
 * at one time, and it is able to re-open and continue appending to an existing getFile.
 *
 * @author Eric Thill
 */
class MultiProcessConcurrentFile implements MappedConcurrentFile {

  public static final int HEADER_SIZE = 32;

  private static final int OFFSET_DATA_START = 0;
  private static final int OFFSET_FILE_SIZE = 8;
  private static final int OFFSET_NEXT_WRITE = 16;
  private static final int OFFSET_WRITE_COMPLETE = 24;

  static MultiProcessConcurrentFile map(File file, int capacity, boolean fillWithZeros) throws IOException {
    if(file.exists()) {
      return mapExistingFile(file);
    } else {
      return mapNewFile(file, capacity, fillWithZeros);
    }
  }

  private static MultiProcessConcurrentFile mapExistingFile(File file) throws IOException {
    final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    final int fileSize = (int)fileChannel.size();
    final long address = IoUtil.map(fileChannel, MapMode.READ_WRITE, 0, fileSize);
    final AtomicBuffer buffer = new UnsafeBuffer();
    buffer.wrap(address, fileSize);
    return new MultiProcessConcurrentFile(file, fileChannel, buffer, fileSize);
  }

  private static MultiProcessConcurrentFile mapNewFile(File file, int capacity, boolean fillWithZeros) {
    final int fileSize = HEADER_SIZE + capacity;
    final FileChannel fileChannel = IoUtil.createEmptyFile(file, fileSize, fillWithZeros);
    final long address = IoUtil.map(fileChannel, MapMode.READ_WRITE, 0, fileSize);
    final AtomicBuffer buffer = new UnsafeBuffer();
    buffer.wrap(address, fileSize);

    // ensure getFile header is filled with zeros
    if(!fillWithZeros) {
      buffer.putLongVolatile(OFFSET_DATA_START, 0);
      buffer.putLongVolatile(OFFSET_FILE_SIZE, 0);
      buffer.putLongVolatile(OFFSET_NEXT_WRITE, 0);
      buffer.putLongVolatile(OFFSET_WRITE_COMPLETE, 0);
    }

    return new MultiProcessConcurrentFile(file, fileChannel, buffer, fileSize);
  }

  private final AtomicLong numLocalWrites = new AtomicLong(0);
  private final AtomicLong numLocalWritesComplete = new AtomicLong(0);
  private final AtomicLong truncateSize = new AtomicLong(-1);
  private final File file;
  private final FileChannel fileChannel;
  private final AtomicBuffer buffer;
  private final long fileSize;

  MultiProcessConcurrentFile(File file, FileChannel fileChannel, AtomicBuffer buffer, int fileSize) {
    this.file = file;
    this.fileChannel = fileChannel;
    this.buffer = buffer;
    this.fileSize = fileSize;

    // populate header as needed
    if(buffer.compareAndSetLong(OFFSET_DATA_START, 0, HEADER_SIZE)) {
      if(buffer.compareAndSetLong(OFFSET_NEXT_WRITE, 0, HEADER_SIZE)) {
        buffer.compareAndSetLong(OFFSET_WRITE_COMPLETE, 0, HEADER_SIZE);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if(fileChannel.isOpen()) {
      if(isPending())
        throw new IOException("There are pending writes");
      if(truncateSize.get() >= 0)
        fileChannel.truncate(truncateSize.get());
      fileChannel.close();
      IoUtil.unmap(fileChannel, buffer.addressOffset(), fileSize);
    }
  }

  @Override
  public boolean isPending() {
    return numLocalWritesComplete.get() != numLocalWrites.get();
  }

  @Override
  public void finish() {
    // this will happen automatically if we reserve more bytes than can fit in the int32 (minus header) worth of data
    reserve(Integer.MAX_VALUE);
  }

  @Override
  public boolean isFinished() {
    final long writeComplete = buffer.getLongVolatile(OFFSET_WRITE_COMPLETE);
    final long nextOffset = buffer.getLongVolatile(OFFSET_NEXT_WRITE);
    // check that writeComplete is caught up to nextOffset, that writeComplete exceeds the getFile size, and that the fileSize field is populated
    return writeComplete == nextOffset && writeComplete >= fileSize && buffer.getLongVolatile(OFFSET_FILE_SIZE) > 0;
  }

  @Override
  public File getFile() {
    return file;
  }

  @Override
  public int write(final byte[] srcBytes, final int srcOffset, final int length) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putBytes(dstOffset, srcBytes, srcOffset, length);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int write(final DirectBuffer srcBuffer, final int srcOffset, final int length) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putBytes(dstOffset, srcBuffer, srcOffset, length);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int write(final ByteBuffer srcByteBuffer) {
    final int length = srcByteBuffer.remaining();
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putBytes(dstOffset, srcByteBuffer, srcByteBuffer.position(), srcByteBuffer.remaining());
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeAscii(final CharSequence srcCharSequence) {
    final int length = srcCharSequence.length();
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      for(int i = 0; i < srcCharSequence.length(); i++) {
        final char c = srcCharSequence.charAt(i);
        buffer.putByte(dstOffset + i, c > 127 ? (byte)'?' : (byte)c);
      }
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeChars(final CharSequence srcCharSequence, ByteOrder byteOrder) {
    final int length = srcCharSequence.length() * 2;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      for(int i = 0; i < srcCharSequence.length(); i++) {
        buffer.putChar(dstOffset + (i * 2), srcCharSequence.charAt(i), byteOrder);
      }
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int write(final int length, final WriteFunction writeFunction) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      writeFunction.write(buffer, dstOffset, length);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public <P> int write(final int length, final P parameter, final ParametizedWriteFunction<P> writeFunction) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      writeFunction.write(buffer, dstOffset, length, parameter);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public <P1, P2> int write(final int length, final P1 parameter1, final P2 parameter2, final BiParametizedWriteFunction<P1, P2> writeFunction) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      writeFunction.write(buffer, dstOffset, length, parameter1, parameter2);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public <P1, P2, P3> int write(final int length, final P1 parameter1, final P2 parameter2, P3 parameter3, final TriParametizedWriteFunction<P1, P2, P3> writeFunction) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      writeFunction.write(buffer, dstOffset, length, parameter1, parameter2, parameter3);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeLong(final long value, final ByteOrder byteOrder) {
    final int length = 8;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putLong(dstOffset, value, byteOrder);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeLongs(final long value1, final long value2, final ByteOrder byteOrder) {
    final int length = 16;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putLong(dstOffset, value1, byteOrder);
      buffer.putLong(dstOffset + 8, value2, byteOrder);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeLongs(final long value1, final long value2, final long value3, final ByteOrder byteOrder) {
    final int length = 24;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putLong(dstOffset, value1, byteOrder);
      buffer.putLong(dstOffset + 8, value2, byteOrder);
      buffer.putLong(dstOffset + 16, value3, byteOrder);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public int writeLongs(final long value1, final long value2, final long value3, final long value4, final ByteOrder byteOrder) {
    final int length = 32;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      buffer.putLong(dstOffset, value1, byteOrder);
      buffer.putLong(dstOffset + 8, value2, byteOrder);
      buffer.putLong(dstOffset + 16, value3, byteOrder);
      buffer.putLong(dstOffset + 24, value4, byteOrder);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  @Override
  public AtomicBuffer getBuffer() {
    return buffer;
  }

  @Override
  public int reserve(int length) {
    numLocalWrites.incrementAndGet();

    long offset;
    do {
      offset = buffer.getLongVolatile(OFFSET_NEXT_WRITE);
      if(offset >= fileSize) {
        // offset exceeded capacity field, do not attempt to increment nextWriteOffset field, nothing more can ever be written
        // no outside write cycle, increment local writes complete now
        numLocalWritesComplete.incrementAndGet();
        return NULL_OFFSET;
      }
    } while(!buffer.compareAndSetLong(OFFSET_NEXT_WRITE, offset, offset + length));

    if(offset + length > fileSize) {
      // first message that will not fit
      // increment writeComplete so it will still eventually match nextWriteOffset at exceeded capacity value
      wrote(length);
      // set this instance to do the truncation since it did the last write
      truncateSize.set(offset);
      // set fileSize field
      buffer.putLongVolatile(OFFSET_FILE_SIZE, offset);
      return NULL_OFFSET;
    }

    // return offset to write bytes
    return (int)offset;
  }

  @Override
  public void wrote(int length) {
    long lastVal;
    do {
      lastVal = buffer.getLongVolatile(OFFSET_WRITE_COMPLETE);
    } while(!buffer.compareAndSetLong(OFFSET_WRITE_COMPLETE, lastVal, lastVal + length));
    numLocalWritesComplete.incrementAndGet();
  }

  @Override
  public int capacity() {
    return (int)(fileSize - HEADER_SIZE);
  }

  @Override
  public boolean hasAvailableCapacity() {
    return buffer.getLongVolatile(OFFSET_NEXT_WRITE) < fileSize;
  }

}
