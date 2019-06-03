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
import io.thill.jacoio.function.DefaultFileProvider;
import io.thill.jacoio.function.FileCompleteFunction;
import io.thill.jacoio.function.FileProvider;

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
  private boolean fillWithZeros = true;
  private boolean multiProcess = false;
  private boolean framed = false;

  /**
   * Set the location of the {@link ConcurrentFile}. When rolling is enabled and {@link RollParameters#fileProvider(FileProvider)} is null, this will be used as
   * the parent directory for created files.
   *
   * @param location
   * @return
   */
  public ConcurrentFileMapper location(File location) {
    this.location = location;
    return this;
  }

  /**
   * Set the capacity of the created file.
   *
   * @param capacity
   * @return
   */
  public ConcurrentFileMapper capacity(int capacity) {
    this.capacity = capacity;
    return this;
  }

  /**
   * Indicates if newly created files should be filled with zeros on creation. Default is true.
   *
   * @param fillWithZeros
   * @return
   */
  public ConcurrentFileMapper fillWithZeros(boolean fillWithZeros) {
    this.fillWithZeros = fillWithZeros;
    return this;
  }

  /**
   * Indicates if the underlying file should allow for multi-process writes. Setting this to true will crate a 32-byte header at the beginning of the file for
   * atomic offset coordination. Defaults to false.
   *
   * @param multiProcess
   * @return
   */
  public ConcurrentFileMapper multiProcess(boolean multiProcess) {
    this.multiProcess = multiProcess;
    return this;
  }

  /**
   * Indicates if writes should be automatically framed using a 4-byte little endian header representing the write length for each and every write.
   *
   * @param framed
   * @return
   */
  public ConcurrentFileMapper framed(boolean framed) {
    this.framed = framed;
    return this;
  }

  /**
   * Get the underlying {@link RollParameters} to set prior to creating the file
   *
   * @return
   */
  public RollParameters roll() {
    return roll;
  }

  /**
   * Functionally write to the underlying {@link RollParameters} using the given {@link RollParameterSetter}. Allows for builder code that's easier to read.
   *
   * @param setter
   * @return
   */
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
      if(roll.fileProvider == null)
        roll.fileProvider = new DefaultFileProvider(location, roll.fileNamePrefix, roll.dateFormat, roll.fileNameSuffix);
      final SingleProcessRollingCoordinator coordinator = new SingleProcessRollingCoordinator(capacity, fillWithZeros, framed, roll.fileProvider,
              roll.yieldOnAllocateContention, roll.asyncClose, roll.preallocate, roll.preallocateCheckMillis, roll.fileCompleteFunction);
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
    private FileProvider fileProvider;
    private String fileNamePrefix;
    private String fileNameSuffix;
    private DateFormat dateFormat = DEFAULT_DATE_FORMAT;
    private boolean yieldOnAllocateContention = true;
    private boolean asyncClose = false;
    private boolean preallocate = false;
    private long preallocateCheckMillis = 100;
    private FileCompleteFunction fileCompleteFunction;

    /**
     * Set true to enable automatic file rolling, false otherwise
     *
     * @param enabled
     * @return
     */
    public RollParameters enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Set the {@link FileProvider} that will generate new files for file rolling. If none is provided, it will default to creating a {@link
     * DefaultFileProvider} using the given {@link RollParameters#fileNamePrefix(String)}, {@link RollParameters#fileNameSuffix(String)}, and {@link
     * RollParameters#dateFormat(DateFormat)}
     *
     * @param fileProvider
     * @return
     */
    public RollParameters fileProvider(FileProvider fileProvider) {
      this.fileProvider = fileProvider;
      return this;
    }

    /**
     * Set the filename prefix for created files. Only used when {@link RollParameters#fileProvider} is null.
     *
     * @param fileNamePrefix
     * @return
     */
    public RollParameters fileNamePrefix(String fileNamePrefix) {
      this.fileNamePrefix = fileNamePrefix;
      return this;
    }

    /**
     * Set the filename suffix for created files. Only used when {@link RollParameters#fileProvider} is null.
     *
     * @param fileNameSuffix
     * @return
     */
    public RollParameters fileNameSuffix(String fileNameSuffix) {
      this.fileNameSuffix = fileNameSuffix;
      return this;
    }

    /**
     * Set the filename date format for created files. Only used when {@link RollParameters#fileProvider} is null.
     *
     * @param dateFormat
     * @return
     */
    public RollParameters dateFormat(DateFormat dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    /**
     * Set the filename date format for created files by creating a {@link SimpleDateFormat} with the given value. Only used when {@link
     * RollParameters#fileProvider} is null.
     *
     * @param dateFormat
     * @return
     */
    public RollParameters dateFormat(String dateFormat) {
      this.dateFormat = new SimpleDateFormat(dateFormat);
      return this;
    }

    /**
     * Indicates that contention on file rolling should result in the losing threads calling {@link Thread#yield()} while waiting for the contention to resolve.
     * False will busy spin. Defaults to true.
     *
     * @param yieldOnAllocateContention
     * @return
     */
    public RollParameters yieldOnAllocateContention(boolean yieldOnAllocateContention) {
      this.yieldOnAllocateContention = yieldOnAllocateContention;
      return this;
    }

    /**
     * Flag to close files asynchronously. This will result in a new thread being created to close individual underlying {@link ConcurrentFile}s. False will
     * close them inline. Defaults to false.
     *
     * @param asyncClose
     * @return
     */
    public RollParameters asyncClose(boolean asyncClose) {
      this.asyncClose = asyncClose;
      return this;
    }

    /**
     * Flag to preallocate files asynchronously. This will result in a new thread being created that will attempt to stay one file ahead of allocation at all
     * times. Defaults to false.
     *
     * @param preallocate
     * @return
     */
    public RollParameters preallocate(boolean preallocate) {
      this.preallocate = preallocate;
      return this;
    }

    /**
     * When {@link RollParameters#preallocate(boolean)} is set to true, this is the interval, measured in milliseconds, at which the preallocation thread will
     * check if it should create new files. It should be sufficiently small enough where your application will never exceed the file capacity within the
     * interval.  The default is 100 milliseconds.
     *
     * @param preallocateCheckMillis
     * @return
     */
    public RollParameters preallocateCheckMillis(long preallocateCheckMillis) {
      this.preallocateCheckMillis = preallocateCheckMillis;
      return this;
    }

    /**
     * Optional function to run on every file after it has been completed and closed.
     *
     * @param fileCompleteFunction
     * @return
     */
    public RollParameters fileCompleteFunction(FileCompleteFunction fileCompleteFunction) {
      this.fileCompleteFunction = fileCompleteFunction;
      return this;
    }

    /**
     * Return the parent {@link ConcurrentFileMapper}
     *
     * @return
     */
    public ConcurrentFileMapper mapper() {
      return ConcurrentFileMapper.this;
    }
  }
}
