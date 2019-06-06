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

import io.thill.jacoio.function.FileProvider;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link MappedFileProvider} that uses the underling {@link FileProvider} to map each and every new file
 *
 * @author Eric Thill
 */
class SingleProcessMappedFileProvider implements MappedFileProvider {

  private static final AtomicLong THREADNAME_INSTANCE = new AtomicLong();

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final AtomicReference<MappedConcurrentFile> curFileRef = new AtomicReference<>();
  private final AtomicReference<MappedConcurrentFile> preallocatedFileRef = new AtomicReference<>();

  private final int fileCapacity;
  private final boolean fillWithZeros;
  private final FileProvider underlyingFileProvider;
  private final boolean yieldOnAllocateContention;
  private final boolean preallocate;
  private final long preallocateCheckMillis;
  private final Thread preallocateThread;

  SingleProcessMappedFileProvider(final int fileCapacity,
                                  final boolean fillWithZeros,
                                  final FileProvider underlyingFileProvider,
                                  final boolean yieldOnAllocateContention,
                                  final boolean preallocate,
                                  final long preallocateCheckMillis) throws IOException {
    this.fileCapacity = fileCapacity;
    this.fillWithZeros = fillWithZeros;
    this.underlyingFileProvider = underlyingFileProvider;
    this.yieldOnAllocateContention = yieldOnAllocateContention;
    this.preallocate = preallocate;
    this.preallocateCheckMillis = preallocateCheckMillis;

    if(preallocate) {
      preallocateThread = new Thread(this::preallocateLoop, getClass().getSimpleName() + "-Preallocator-" + THREADNAME_INSTANCE.getAndIncrement());
      preallocateThread.start();
    } else {
      preallocateThread = null;
    }
  }

  @Override
  public void close() throws IOException {
    keepRunning.set(false);
    if(preallocate) {
      // kill the preallocate thread immediately
      preallocateThread.interrupt();

      // delete the preallocated file if it exists
      final MappedConcurrentFile preallocatedFile = preallocatedFileRef.getAndSet(null);
      if(preallocatedFile != null) {
        preallocatedFile.close();
        // preallocated file was never used. delete it.
        preallocatedFile.getFile().delete();
      }
    }
  }

  @Override
  public MappedConcurrentFile nextFile() throws IOException {
    MappedConcurrentFile curFile;
    if(preallocate) {
      // swap to preallocated file
      curFile = preallocatedFileRef.getAndSet(null);
      while(curFile == null) {
        // file is not preallocated yet, yield and try again
        if(yieldOnAllocateContention)
          Thread.yield();
        curFile = preallocatedFileRef.getAndSet(null);
      }
    } else {
      // allocate inline
      curFile = mapFile(underlyingFileProvider.nextFile());
    }
    curFileRef.set(curFile);
    return curFile;
  }

  private MappedConcurrentFile mapFile(File file) throws IOException {
    return SingleProcessConcurrentFile.map(file, fileCapacity, fillWithZeros);
  }

  private void preallocateLoop() {
    try {
      while(keepRunning.get()) {
        if(preallocatedFileRef.get() == null) {
          final File file = underlyingFileProvider.nextFile();
          try {
            final MappedConcurrentFile nextFile = mapFile(file);
            preallocatedFileRef.set(nextFile);
          } catch(Throwable t) {
            if(keepRunning.get()) {
              t.printStackTrace();
            }
            file.delete();
          }
        } else {
          Thread.sleep(preallocateCheckMillis);
        }
      }
    } catch(InterruptedException e) {
      // ignore
    }
  }

}
