package io.thill.jacoio;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FramedConcurrentFile implements ConcurrentFile {

  private static final int FRAME_HEADER_SIZE = 4;
  private final ConcurrentFile underlyingFile;

  public FramedConcurrentFile(ConcurrentFile underlyingFile) {
    this.underlyingFile = underlyingFile;
  }

  @Override
  public void close() throws Exception {
    underlyingFile.close();
  }

  @Override
  public boolean isPending() {
    return underlyingFile.isPending();
  }

  @Override
  public void finish() {
    underlyingFile.finish();
  }

  @Override
  public boolean isFinished() {
    return underlyingFile.isFinished();
  }

  @Override
  public File file() {
    return underlyingFile.file();
  }

  @Override
  public int write(final byte[] srcBytes, final int srcOffset, final int srcLength) {
    return underlyingFile.write(FRAME_HEADER_SIZE + srcLength, (final AtomicBuffer buffer, final int offset, final int length) -> {
      buffer.putBytes(offset + FRAME_HEADER_SIZE, srcBytes, srcOffset, srcLength);
      buffer.putInt(offset, length);
    });
  }

  @Override
  public int write(final DirectBuffer srcBuffer, final int srcOffset, final int srcLength) {
    return underlyingFile.write(FRAME_HEADER_SIZE + srcLength, (final AtomicBuffer buffer, final int offset, final int length) -> {
      buffer.putBytes(offset + FRAME_HEADER_SIZE, srcBuffer, srcOffset, srcLength);
      buffer.putInt(offset, length);
    });
  }

  @Override
  public int write(final ByteBuffer srcByteBuffer) {
    return underlyingFile.write(FRAME_HEADER_SIZE + srcByteBuffer.remaining(), (final AtomicBuffer buffer, final int offset, final int length) -> {
      buffer.putBytes(offset + FRAME_HEADER_SIZE, srcByteBuffer, srcByteBuffer.position(), srcByteBuffer.remaining());
      buffer.putInt(offset, length);
    });
  }

  @Override
  public int writeAscii(final CharSequence srcCharSequence) {
    return underlyingFile.write(FRAME_HEADER_SIZE + srcCharSequence.length(), (final AtomicBuffer buffer, final int offset, final int length) -> {
      for(int i = 0; i < srcCharSequence.length(); i++) {
        final char c = srcCharSequence.charAt(i);
        buffer.putByte(FRAME_HEADER_SIZE + offset + i, c > 127 ? (byte)'?' : (byte)c);
      }
      buffer.putInt(offset, length);
    });
  }

  @Override
  public int writeChars(final CharSequence srcCharSequence, final ByteOrder byteOrder) {
    return underlyingFile.write(FRAME_HEADER_SIZE + srcCharSequence.length() * 2, (final AtomicBuffer buffer, final int offset, final int length) -> {
      for(int i = 0; i < srcCharSequence.length(); i++) {
        buffer.putChar(FRAME_HEADER_SIZE + offset + (i * 2), srcCharSequence.charAt(i), byteOrder);
      }
      buffer.putInt(offset, length);
    });
  }

  @Override
  public int write(final int dataLength, final WriteFunction writeFunction) {
    return underlyingFile.write(FRAME_HEADER_SIZE + dataLength, (final AtomicBuffer buffer, final int offset, final int length) -> {
      writeFunction.write(buffer, offset + FRAME_HEADER_SIZE, dataLength);
      buffer.putInt(offset, length);
    });
  }


}
