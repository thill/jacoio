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
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link MappedFileProvider} that uses a {@link CoordinationFile} to coordinate the next file to use
 *
 * @author Eric Thill
 */
class MultiProcessMappedFileProvider implements MappedFileProvider {

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final AtomicReference<String> curCoordinationContents = new AtomicReference<>();
  private final AtomicReference<MappedConcurrentFile> curFileRef = new AtomicReference<>();
  private final AtomicReference<MappedConcurrentFile> preallocatedFileRef = new AtomicReference<>();

  private final CoordinationFile coordinationFile;
  private final int fileCapacity;
  private final boolean fillWithZeros;
  private final boolean yieldOnAllocateContention;
  private final boolean preallocate;
  private final long preallocateCheckMillis;

  MultiProcessMappedFileProvider(final File coordinationFile,
                                 final int fileCapacity,
                                 final boolean fillWithZeros,
                                 final FileProvider underlyingFileProvider,
                                 final boolean yieldOnAllocateContention,
                                 final boolean preallocate,
                                 final long preallocateCheckMillis) throws IOException {
    this.coordinationFile = new CoordinationFile(coordinationFile, underlyingFileProvider, preallocate, yieldOnAllocateContention);
    this.fileCapacity = fileCapacity;
    this.fillWithZeros = fillWithZeros;
    this.yieldOnAllocateContention = yieldOnAllocateContention;
    this.preallocate = preallocate;
    this.preallocateCheckMillis = preallocateCheckMillis;

    final String coordinationContents = this.coordinationFile.next("");
    final File initialCurFile = CoordinationFile.curFile(coordinationContents);
    final File initialPreallocatedFile = CoordinationFile.curFile(coordinationContents);

    this.curFileRef.set(mapFile(initialCurFile));
    if(initialPreallocatedFile != null)
      this.preallocatedFileRef.set(mapFile(initialPreallocatedFile));

    if(preallocate) {
      new Thread(this::preallocateLoop, getClass().getSimpleName() + "-Preallocator").start();
    }
  }

  @Override
  public void close() throws IOException {
    keepRunning.set(false);
    final MappedConcurrentFile preallocatedFile = preallocatedFileRef.getAndSet(null);
    if(preallocatedFile != null) {
      preallocatedFile.close();
    }
  }

  @Override
  public MappedConcurrentFile nextFile() throws IOException {
    MappedConcurrentFile curFile;
    if(preallocate) {
      // swap to preallocated file
      curFile = preallocatedFileRef.get();
      while(curFile == null) {
        // file is not preallocated yet, yield and try again
        if(yieldOnAllocateContention)
          Thread.yield();
        curFile = preallocatedFileRef.get();
      }
    } else {
      // allocate inline
      final String lastCoordinationContents = curCoordinationContents.get();
      final String newCoordinationContents = coordinationFile.next(lastCoordinationContents);
      curCoordinationContents.set(newCoordinationContents);
      curFile = mapFile(CoordinationFile.curFile(newCoordinationContents));
    }
    curFileRef.set(curFile);
    preallocatedFileRef.set(null); // this must happen last, or else there can be race conditions with the preallocator
    return curFile;
  }

  private MappedConcurrentFile mapFile(File file) throws IOException {
    return MultiProcessConcurrentFile.map(file, fileCapacity, fillWithZeros);
  }

  private void preallocateLoop() {
    while(keepRunning.get()) {
      try {
        if(preallocatedFileRef.get() == null) {
          final File curFile = curFileRef.get().getFile();

          // read/update coordination file
          final String lastCoordinationContents = curCoordinationContents.get();
          final String newCoordinationContents = coordinationFile.next(lastCoordinationContents);

          // update our local copy of coordination file contents
          curCoordinationContents.set(newCoordinationContents);

          // parse coordination file contents
          final File readCurFile = CoordinationFile.curFile(newCoordinationContents);
          final File readPreallocatedFile = CoordinationFile.preallocatedFile(newCoordinationContents);
          // set preallocation based on coordination file contents
          if(readPreallocatedFile == null) {
            // another thread won and does preallocate -> set our local preallocation to the new allocation to be swapped
            preallocatedFileRef.set(mapFile(readCurFile));
          } else if(!curFile.equals(readCurFile)) {
            // the read current file does not match the actual current file -> we are more than 1 file behind, set our local preallocation to the new current file to be swapped
            preallocatedFileRef.set(mapFile(readCurFile));
          } else {
            // a new preallocation was set, and current files match -> preallocate the new coordinated preallocation file
            preallocatedFileRef.set(mapFile(readPreallocatedFile));
          }
        } else {
          Thread.sleep(preallocateCheckMillis);
        }
      } catch(Throwable t) {
        t.printStackTrace();
      }
    }
  }

}
