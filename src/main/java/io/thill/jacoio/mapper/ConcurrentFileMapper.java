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
import io.thill.jacoio.function.FileCompleteFunction;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Factory to map ConcurrentFile
 *
 * @author Eric Thill
 */
public class ConcurrentFileMapper {

  private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");

  private final RollParameters roll = new RollParameters();
  private File location;
  private int capacity;
  private boolean fillWithZeros = false;
  private boolean multiProcess = false;
  private boolean framed = false;

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

  public RollParameters roll() {
    return roll;
  }

  public ConcurrentFileMapper roll(RollParameterSetter setter) {
    setter.set(roll);
    return this;
  }

  public ConcurrentFile map() throws IOException {
    if(location == null)
      throw new IllegalArgumentException("location cannot be null");
    if(capacity <= 0)
      throw new IllegalArgumentException("capacity must be non-zero");
    if(roll.enabled && multiProcess)
      throw new IllegalArgumentException("multi-process roll is not currently supported");


    if(roll.enabled) {
      location.mkdirs();
      final SingleProcessRollingCoordinator coordinator = new SingleProcessRollingCoordinator(location, capacity, fillWithZeros, framed, roll.fileNamePrefix,
              roll.dateFormat, roll.fileNameSuffix, roll.yieldOnAllocateContention, roll.asyncClose, roll.fileCompleteFunction);
      return new SingleProcessRollingConcurrentFile(coordinator);
    } else {
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

  @FunctionalInterface
  public interface RollParameterSetter {
    void set(RollParameters rollParameters);
  }

  public class RollParameters {
    private boolean enabled;
    private String fileNamePrefix;
    private String fileNameSuffix;
    private DateFormat dateFormat = DEFAULT_DATE_FORMAT;
    private boolean yieldOnAllocateContention;
    private boolean asyncClose;
    private FileCompleteFunction fileCompleteFunction;

    public RollParameters enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public RollParameters fileNamePrefix(String fileNamePrefix) {
      this.fileNamePrefix = fileNamePrefix;
      return this;
    }

    public RollParameters fileNameSuffix(String fileNameSuffix) {
      this.fileNameSuffix = fileNameSuffix;
      return this;
    }

    public RollParameters dateFormat(DateFormat dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public RollParameters dateFormat(String dateFormat) {
      this.dateFormat = new SimpleDateFormat(dateFormat);
      return this;
    }

    public RollParameters yieldOnAllocateContention(boolean yieldOnAllocateContention) {
      this.yieldOnAllocateContention = yieldOnAllocateContention;
      return this;
    }

    public RollParameters asyncClose(boolean asyncClose) {
      this.asyncClose = asyncClose;
      return this;
    }

    public RollParameters fileCompleteFunction(FileCompleteFunction fileCompleteFunction) {
      this.fileCompleteFunction = fileCompleteFunction;
      return this;
    }

    public ConcurrentFileMapper mapper() {
      return ConcurrentFileMapper.this;
    }
  }
}
