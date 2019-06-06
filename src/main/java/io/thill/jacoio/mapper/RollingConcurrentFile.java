/**
 * Copyright (c) 2019 Eric Thill
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this getFile except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import io.thill.jacoio.function.BiParametizedWriteFunction;
import io.thill.jacoio.function.FileProvider;
import io.thill.jacoio.function.ParametizedWriteFunction;
import io.thill.jacoio.function.WriteFunction;
import org.agrona.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ConcurrentFile} implementation that continuously rolls to new files using the underlying {@link RollingCoordinator}
 *
 * @author Eric Thill
 */
class RollingConcurrentFile implements ConcurrentFile {

  private final RollingCoordinator rollingCoordinator;
  private final int capacity;

  RollingConcurrentFile(RollingCoordinator rollingCoordinator) throws IOException {
    this.rollingCoordinator = rollingCoordinator;
    this.capacity = rollingCoordinator.currentFile().capacity();
  }

  @Override
  public boolean isPending() {
    return rollingCoordinator.currentFile().isPending();
  }

  @Override
  public boolean isFinished() {
    // never finished, always rolling to a new mapper
    return false;
  }

  @Override
  public void finish() {
    // finish the current mapper, force a roll
    rollingCoordinator.currentFile().finish();
  }

  @Override
  public File getFile() {
    return rollingCoordinator.currentFile().getFile();
  }

  @Override
  public int write(final byte[] srcBytes, final int srcOffset, final int length) throws IOException {
    checkLength(length);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(srcBytes, srcOffset, length);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public int write(final DirectBuffer srcBuffer, final int srcOffset, final int length) throws IOException {
    checkLength(length);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(srcBuffer, srcOffset, length);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public int write(final ByteBuffer srcByteBuffer) throws IOException {
    checkLength(srcByteBuffer.remaining());
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(srcByteBuffer);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public int writeAscii(final CharSequence srcCharSequence) throws IOException {
    checkLength(srcCharSequence.length());
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().writeAscii(srcCharSequence);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public int writeChars(final CharSequence srcCharSequence, final ByteOrder byteOrder) throws IOException {
    checkLength(srcCharSequence.length() * 2);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().writeChars(srcCharSequence, byteOrder);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public int write(final int length, final WriteFunction writeFunction) throws IOException {
    checkLength(length);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(length, writeFunction);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public <P> int write(final int length, final ParametizedWriteFunction<P> writeFunction, final P parameter) throws IOException {
    checkLength(length);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(length, writeFunction, parameter);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  @Override
  public <P1, P2> int write(final int length, final BiParametizedWriteFunction<P1, P2> writeFunction, final P1 parameter1, final P2 parameter2) throws IOException {
    checkLength(length);
    int offset;
    do {
      offset = rollingCoordinator.fileForWrite().write(length, writeFunction, parameter1, parameter2);
    } while(offset == NULL_OFFSET);
    return offset;
  }

  private void checkLength(int length) throws IOException {
    if(length > capacity)
      throw new IOException("length=" + length + " exceeds capacity=" + capacity);
  }

  @Override
  public void close() throws IOException {
    rollingCoordinator.close();
  }
}
