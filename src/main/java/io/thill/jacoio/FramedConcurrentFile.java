package io.thill.jacoio;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An implementation of {@link ConcurrentFile} that wraps an underlying {@link ConcurrentFile} to provide write framing. Each write will be prepended with a
 * 32-bit length field. The length field is populated after the corresponding write. This allows concurrent readers to be able to read the getFile by waiting for
 * the length field of each frame to be populated before reading the corresponding data.
 *
 * @author Eric Thill
 */
public class FramedConcurrentFile implements ConcurrentFile {

  private static final int FRAME_HEADER_SIZE = 4;
  private final ConcurrentFile underlyingFile;

  FramedConcurrentFile(ConcurrentFile underlyingFile) {
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
  public File getFile() {
    return underlyingFile.getFile();
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
