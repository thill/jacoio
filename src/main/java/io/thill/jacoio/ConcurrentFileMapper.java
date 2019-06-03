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

import java.io.File;
import java.io.IOException;

/**
 * Factory to map ConcurrentFile
 *
 * @author Eric Thill
 */
public class ConcurrentFileMapper {

  private File location;
  private int capacity;
  private boolean fillWithZeros = false;
  private boolean multiProcess = false;
  private boolean framed = false;

  ConcurrentFileMapper() {
    // not public, use ConcurrentFile.map()
  }

  public ConcurrentFileMapper location(File location) {
    this.location = location;
    return this;
  }

  public ConcurrentFileMapper capacity(int capacity) {
    this.capacity = capacity;
    return this;
  }

  public ConcurrentFileMapper fillWithZeros(boolean fillWithZeros) {
    this.fillWithZeros = fillWithZeros;
    return this;
  }

  public ConcurrentFileMapper multiProcess(boolean multiProcess) {
    this.multiProcess = multiProcess;
    return this;
  }

  public ConcurrentFileMapper framed(boolean framed) {
    this.framed = framed;
    return this;
  }

  public ConcurrentFile map() throws IOException {
    if(location == null)
      throw new IllegalArgumentException("location cannot be null");
    if(capacity <= 0)
      throw new IllegalArgumentException("capacity must be non-zero");

    MappedConcurrentFile file;
    if(multiProcess)
      file = MultiProcessConcurrentFile.map(location, capacity, fillWithZeros);
    else
      file = SingleProcessConcurrentFile.map(location, capacity, fillWithZeros);

    if(framed)
      file = new FramedConcurrentFile(file);

    return file;
  }
}
