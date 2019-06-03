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
import org.agrona.concurrent.AtomicBuffer;

/**
 * Used internally for direct buffer access. This is package-only, as all public API calls must be atomic.
 *
 * @author Eric Thill
 */
interface MappedConcurrentFile extends ConcurrentFile {

  /**
   * Get the underlying mapper buffer
   *
   * @return The underlying mapper buffer
   */
  AtomicBuffer getBuffer();

  /**
   * Reserve the given number of bytes for writing. For non-negative return values, {@link MappedConcurrentFile#wrote(int)} must be called to finish the write
   * process.
   *
   * @param length the number of bytes to reserve for writing
   * @return the offset to start writing in the buffer, or -1 if it could not fit
   */
  int reserve(int length);

  /**
   * Finish writing bytes that were reserved in a {@link MappedConcurrentFile#reserve(int)} call.
   *
   * @param length The number of bytes that were reserved for writing. This must be the same value that was passed to {@link MappedConcurrentFile#reserve(int)}
   */
  void wrote(int length);

  /**
   * Return the total write capacity of the mapper, not including any mapper headers
   *
   * @return the capacity
   */
  int capacity();

  /**
   * Check if there is any current write capacity
   *
   * @return true if there is write capacity, false otherwise.
   */
  boolean hasAvailableCapacity();

}
