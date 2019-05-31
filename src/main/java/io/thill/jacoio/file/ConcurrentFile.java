/**
 * Copyright (c) 2019 Eric Thill
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package io.thill.jacoio.file;

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
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maps an underlying file to provide concurrent, multi-process, lock-free write operations.
 *
 * @author Eric Thill
 */
public class ConcurrentFile implements AutoCloseable {

  public static final int HEADER_SIZE = 32;
  public static final int NULL_OFFSET = -1;

  private static final int OFFSET_DATA_START = 0;
  private static final int OFFSET_FILE_SIZE = 8;
  private static final int OFFSET_NEXT_WRITE = 16;
  private static final int OFFSET_WRITE_COMPLETE = 24;
  private static final int CHAR_SIZE = 2;
  private static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * Map an existing {@link ConcurrentFile}
   *
   * @param file The existing file to wrap
   * @return the {@link ConcurrentFile}
   */
  public static ConcurrentFile mapExistingFile(File file) throws IOException {
    final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
    final int fileSize = (int)fileChannel.size();
    final long address = IoUtil.map(fileChannel, MapMode.READ_WRITE, 0, fileSize);
    final AtomicBuffer buffer = new UnsafeBuffer();
    buffer.wrap(address, fileSize);
    return new ConcurrentFile(file, fileChannel, buffer, fileSize);
  }

  /**
   * Create a new {@link ConcurrentFile}
   *
   * @param file          The new file to create
   * @param capacity      The capacity of the
   * @param fillWithZeros Fill the new file with 0s
   * @return the {@link ConcurrentFile}
   */
  public static ConcurrentFile mapNewFile(File file, int capacity, boolean fillWithZeros) {
    final int fileSize = HEADER_SIZE + capacity;
    final FileChannel fileChannel = IoUtil.createEmptyFile(file, fileSize, fillWithZeros);
    final long address = IoUtil.map(fileChannel, MapMode.READ_WRITE, 0, fileSize);
    final AtomicBuffer buffer = new UnsafeBuffer();
    buffer.wrap(address, fileSize);

    // ensure file header is filled with zeros
    if(!fillWithZeros) {
      buffer.putLongVolatile(OFFSET_DATA_START, 0);
      buffer.putLongVolatile(OFFSET_FILE_SIZE, 0);
      buffer.putLongVolatile(OFFSET_NEXT_WRITE, 0);
      buffer.putLongVolatile(OFFSET_WRITE_COMPLETE, 0);
    }

    return new ConcurrentFile(file, fileChannel, buffer, fileSize);
  }

  private final AtomicLong numLocalWrites = new AtomicLong(0);
  private final AtomicLong numLocalWritesComplete = new AtomicLong(0);
  private final File file;
  private final FileChannel fileChannel;
  private final AtomicBuffer buffer;
  private final long fileSize;

  ConcurrentFile(File file, FileChannel fileChannel, AtomicBuffer buffer, int fileSize) {
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
    if(isPending())
      throw new IOException("There are pending writes");
    fileChannel.close();
    IoUtil.unmap(fileChannel, buffer.addressOffset(), fileSize);
  }

  /**
   * Check if there are pending local writes to be completed
   *
   * @return true if there are pending local writes, false otherwise
   */
  public boolean isPending() {
    return numLocalWritesComplete.get() != numLocalWrites.get();
  }

  /**
   * Mark the file as finished, so no more writes can ever be performed. This will populate the fileSize field in the file header.
   */
  public void finish() {
    // this will happen automatically if we reserve more bytes than can fit in the int32 (minus header) worth of data
    reserve(Integer.MAX_VALUE);
  }

  /**
   * Check if all pending writes have completed and no more writes can ever be written
   *
   * @return true if no writes can ever be performed
   */
  public boolean isFinished() {
    final long writeComplete = buffer.getLongVolatile(OFFSET_WRITE_COMPLETE);
    final long nextOffset = buffer.getLongVolatile(OFFSET_NEXT_WRITE);
    // check that writeComplete is caught up to nextOffset, that writeComplete exceeds the file size, and that the fileSize field is populated
    return writeComplete == nextOffset && writeComplete >= fileSize && buffer.getLongVolatile(OFFSET_FILE_SIZE) > 0;
  }

  /**
   * Get the underlying buffer
   *
   * @return The buffer
   */
  public AtomicBuffer getBuffer() {
    return buffer;
  }

  /**
   * Get the underlying {@link File}
   *
   * @return the underlying file
   */
  public File getFile() {
    return file;
  }

  /**
   * Reserve space for a write. A corresponding call to {@link ConcurrentFile#wrote(long)} must be made after the write is complete. Do not call {@link
   * ConcurrentFile#wrote(long)} when -1 is returned.
   *
   * @param length The length of bytes to write
   * @return The offset the bytes should be written. -1 if there is no space left.
   */
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
      // set fileSize field
      buffer.putLongVolatile(OFFSET_FILE_SIZE, offset);
      return NULL_OFFSET;
    }

    // return offset to write bytes
    return (int)offset;
  }

  /**
   * Finalize a write
   *
   * @param length The length of bytes written
   */
  public void wrote(long length) {
    long lastVal;
    do {
      lastVal = buffer.getLongVolatile(OFFSET_WRITE_COMPLETE);
    } while(!buffer.compareAndSetLong(OFFSET_WRITE_COMPLETE, lastVal, lastVal + length));
    numLocalWritesComplete.incrementAndGet();
  }

  /**
   * Write the given bytes from the given offset to the given length
   *
   * @param srcBytes  the source byte array
   * @param srcOffset the offset in the source byte array
   * @param length    the number of bytes to write
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
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

  /**
   * Write the given buffer from the given offset to the given length
   *
   * @param srcBuffer the source buffer
   * @param srcOffset the offset in the source buffer
   * @param length    the number of bytes to write
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
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

  /**
   * Write the given ByteBuffer from {@link ByteBuffer#position()} with length={@link ByteBuffer#remaining()}. The position of the {@link ByteBuffer} will not
   * be changed.
   *
   * @param srcByteBuffer the source byte buffer
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
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

  /**
   * Write the given CharSequence as ascii characters
   *
   * @param srcCharSequence the source character sequence
   * @return the offset at which the characters were written, -1 if it could not fit
   */
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

  /**
   * Write the given CharSequence as 2-byte characters
   *
   * @param srcCharSequence the source character sequence
   * @param byteOrder       the destination byte-order
   * @return the offset at which the characters were written, -1 if it could not fit
   */
  public int writeChars(final CharSequence srcCharSequence, ByteOrder byteOrder) {
    final int length = srcCharSequence.length() * CHAR_SIZE;
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      for(int i = 0; i < srcCharSequence.length(); i++) {
        buffer.putChar(dstOffset + (i * CHAR_SIZE), srcCharSequence.charAt(i), byteOrder);
      }
    } finally {
      wrote(length);
    }

    return dstOffset;
  }

  /**
   * Write to the underlying buffer using the given {@link WriteFunction}
   *
   * @param length        the total number of bytes that will be written by the {@link WriteFunction}
   * @param writeFunction the write function
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
  public int write(final int length, final WriteFunction writeFunction) {
    final int dstOffset = reserve(length);
    if(dstOffset < 0)
      return NULL_OFFSET;

    try {
      writeFunction.write(buffer, dstOffset);
    } finally {
      wrote(length);
    }

    return dstOffset;
  }
}
