package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import io.thill.jacoio.function.BiParametizedWriteFunction;
import io.thill.jacoio.function.ParametizedWriteFunction;
import io.thill.jacoio.function.WriteFunction;
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
public class FramedConcurrentFile implements MappedConcurrentFile {

  private static final int FRAME_HEADER_SIZE = 4;
  private final MappedConcurrentFile underlyingFile;

  FramedConcurrentFile(MappedConcurrentFile underlyingFile) {
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
    final int length = FRAME_HEADER_SIZE + srcLength;
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        getBuffer().putBytes(offset + FRAME_HEADER_SIZE, srcBytes, srcOffset, srcLength);
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public int write(final DirectBuffer srcBuffer, final int srcOffset, final int srcLength) {
    final int length = FRAME_HEADER_SIZE + srcLength;
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        getBuffer().putBytes(offset + FRAME_HEADER_SIZE, srcBuffer, srcOffset, srcLength);
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public int write(final ByteBuffer srcByteBuffer) {
    final int length = FRAME_HEADER_SIZE + srcByteBuffer.remaining();
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        getBuffer().putBytes(offset + FRAME_HEADER_SIZE, srcByteBuffer, srcByteBuffer.position(), srcByteBuffer.remaining());
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public int writeAscii(final CharSequence srcCharSequence) {
    final int length = FRAME_HEADER_SIZE + srcCharSequence.length();
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        for(int i = 0; i < srcCharSequence.length(); i++) {
          final char c = srcCharSequence.charAt(i);
          getBuffer().putByte(FRAME_HEADER_SIZE + offset + i, c > 127 ? (byte)'?' : (byte)c);
        }
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public int writeChars(final CharSequence srcCharSequence, final ByteOrder byteOrder) {
    final int length = FRAME_HEADER_SIZE + srcCharSequence.length();
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        for(int i = 0; i < srcCharSequence.length(); i++) {
          getBuffer().putChar(FRAME_HEADER_SIZE + offset + (i * 2), srcCharSequence.charAt(i), byteOrder);
        }
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public int write(final int dataLength, final WriteFunction writeFunction) {
    final int length = FRAME_HEADER_SIZE + dataLength;
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        writeFunction.write(getBuffer(), offset + FRAME_HEADER_SIZE, dataLength);
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public <P> int write(final int dataLength, final ParametizedWriteFunction<P> writeFunction, final P parameter) {
    final int length = FRAME_HEADER_SIZE + dataLength;
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        writeFunction.write(getBuffer(), offset + FRAME_HEADER_SIZE, dataLength, parameter);
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public <P1, P2> int write(final int dataLength, final BiParametizedWriteFunction<P1, P2> writeFunction,
                            final P1 parameter1, final P2 parameter2) {
    final int length = FRAME_HEADER_SIZE + dataLength;
    final int offset = reserve(length);
    if(offset != ConcurrentFile.NULL_OFFSET) {
      try {
        writeFunction.write(getBuffer(), offset + FRAME_HEADER_SIZE, dataLength, parameter1, parameter2);
        getBuffer().putInt(offset, length);
      } finally {
        wrote(length);
      }
    }
    return offset;
  }

  @Override
  public AtomicBuffer getBuffer() {
    return underlyingFile.getBuffer();
  }

  @Override
  public int reserve(int length) {
    return underlyingFile.reserve(length);
  }

  @Override
  public void wrote(int length) {
    underlyingFile.wrote(length);
  }

  @Override
  public int capacity() {
    return underlyingFile.capacity();
  }

  @Override
  public boolean hasAvailableCapacity() {
    return underlyingFile.hasAvailableCapacity();
  }
}
