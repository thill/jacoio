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
package io.thill.jacoio;

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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Extends {@link ConcurrentFile} to provide single-process writing. There is no file header, and a file cannot be reopened after it has been closed.
 *
 * @author Eric Thill
 */
class SingleProcessConcurrentFile implements ConcurrentFile {

  static SingleProcessConcurrentFile map(File file, int capacity, boolean fillWithZeros) throws IOException {
    if(file.exists())
      throw new IOException("File Exists. SingleProcessConcurrentFile cannot modify an existing file.");
    final int fileSize = capacity;
    final FileChannel fileChannel = IoUtil.createEmptyFile(file, fileSize, fillWithZeros);
    final long address = IoUtil.map(fileChannel, MapMode.READ_WRITE, 0, fileSize);
    final AtomicBuffer buffer = new UnsafeBuffer();
    buffer.wrap(address, fileSize);
    return new SingleProcessConcurrentFile(file, fileChannel, buffer, fileSize);
  }

  private final AtomicLong nextWriteOffset = new AtomicLong(0);
  private final AtomicLong writeComplete = new AtomicLong(0);
  private final AtomicLong finalFileSize = new AtomicLong(0);
  private final File file;
  private final FileChannel fileChannel;
  private final AtomicBuffer buffer;
  private final long fileSize;

  SingleProcessConcurrentFile(File file, FileChannel fileChannel, AtomicBuffer buffer, int fileSize) {
    this.file = file;
    this.fileChannel = fileChannel;
    this.buffer = buffer;
    this.fileSize = fileSize;
  }

  @Override
  public void close() throws IOException {
    if(isPending())
      throw new IOException("There are pending writes");
    if(finalFileSize.get() > 0)
      fileChannel.truncate(finalFileSize.get());
    fileChannel.close();
    IoUtil.unmap(fileChannel, buffer.addressOffset(), fileSize);
  }

  @Override
  public boolean isPending() {
    return nextWriteOffset.get() != writeComplete.get();
  }

  @Override
  public void finish() {
    // this will happen automatically if we reserve more bytes than can fit in the int32 (minus header) worth of data
    reserve(Integer.MAX_VALUE);
  }

  @Override
  public boolean isFinished() {
    final long writeComplete = this.writeComplete.get();
    final long nextOffset = this.nextWriteOffset.get();
    return writeComplete == nextOffset && writeComplete >= fileSize && finalFileSize.get() > 0;
  }

  @Override
  public File file() {
    return file;
  }

  private int reserve(int length) {
    long offset;
    do {
      offset = nextWriteOffset.get();
      if(offset >= fileSize) {
        // offset exceeded capacity field, do not attempt to increment nextWriteOffset field, nothing more can ever be written
        // no outside write cycle, increment local writes complete now
        return NULL_OFFSET;
      }
    } while(!nextWriteOffset.compareAndSet(offset, offset + length));

    if(offset + length > fileSize) {
      // first message that will not fit
      // increment writeComplete so it will still eventually match nextWriteOffset at exceeded capacity value
      wrote(length);
      // set final file size
      finalFileSize.set(offset);
      return NULL_OFFSET;
    }

    // return offset to write bytes
    return (int)offset;
  }

  private void wrote(long length) {
    long lastVal;
    do {
      lastVal = writeComplete.get();
    } while(!writeComplete.compareAndSet(lastVal, lastVal + length));
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


}
