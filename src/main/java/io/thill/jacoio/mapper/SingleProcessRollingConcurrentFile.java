package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import io.thill.jacoio.function.WriteFunction;
import org.agrona.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class SingleProcessRollingConcurrentFile implements ConcurrentFile {

  private final SingleProcessRollingCoordinator rollingCoordinator;
  private final int capacity;

  SingleProcessRollingConcurrentFile(SingleProcessRollingCoordinator rollingCoordinator) throws IOException {
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

  private void checkLength(int length) throws IOException {
    if(length > capacity)
      throw new IOException("length=" + length + " exceeds capacity=" + capacity);
  }

  @Override
  public void close() throws Exception {
    rollingCoordinator.close();
  }
}
