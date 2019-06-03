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
package io.thill.jacoio;

import io.thill.jacoio.function.WriteFunction;
import org.agrona.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Maps an underlying getFile to provide concurrent, lock-free write operations.
 *
 * @author Eric Thill
 */
public interface ConcurrentFile extends AutoCloseable {

  static ConcurrentFileMapper map() {
    return new ConcurrentFileMapper();
  }

  int NULL_OFFSET = -1;

  /**
   * Check if there are pending local writes to be completed
   *
   * @return true if there are pending local writes, false otherwise
   */
  boolean isPending();

  /**
   * Check if all pending writes have completed and no more writes can ever be written
   *
   * @return true if no writes can ever be performed
   */
  boolean isFinished();

  /**
   * Mark the getFile as finished, so no more writes can ever be performed. This will populate the fileSize field in the getFile header.
   */
  void finish();

  /**
   * Get the underlying {@link File}
   *
   * @return the underlying getFile
   */
  File getFile();

  /**
   * Write the given bytes from the given offset to the given length
   *
   * @param srcBytes  the source byte array
   * @param srcOffset the offset in the source byte array
   * @param length    the number of bytes to write
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
  int write(final byte[] srcBytes, final int srcOffset, final int length) throws IOException;

  /**
   * Write the given buffer from the given offset to the given length
   *
   * @param srcBuffer the source buffer
   * @param srcOffset the offset in the source buffer
   * @param length    the number of bytes to write
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
  int write(final DirectBuffer srcBuffer, final int srcOffset, final int length) throws IOException;

  /**
   * Write the given ByteBuffer from {@link ByteBuffer#position()} with length={@link ByteBuffer#remaining()}. The position of the {@link ByteBuffer} will not
   * be changed.
   *
   * @param srcByteBuffer the source byte buffer
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
  int write(final ByteBuffer srcByteBuffer) throws IOException;

  /**
   * Write the given CharSequence as ascii characters
   *
   * @param srcCharSequence the source character sequence
   * @return the offset at which the characters were written, -1 if it could not fit
   */
  int writeAscii(final CharSequence srcCharSequence) throws IOException;

  /**
   * Write the given CharSequence as 2-byte characters
   *
   * @param srcCharSequence the source character sequence
   * @param byteOrder       the destination byte-order
   * @return the offset at which the characters were written, -1 if it could not fit
   */
  int writeChars(final CharSequence srcCharSequence, ByteOrder byteOrder) throws IOException;

  /**
   * Write to the underlying buffer using the given {@link WriteFunction}
   *
   * @param length        the total number of bytes that will be written by the {@link WriteFunction}
   * @param writeFunction the write function
   * @return the offset at which the bytes were written, -1 if it could not fit
   */
  int write(final int length, final WriteFunction writeFunction) throws IOException;

}
