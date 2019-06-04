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

import io.thill.jacoio.function.FileCompleteFunction;
import io.thill.jacoio.function.FileProvider;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates rolling to a new file using the underling {@link MappedFileProvider}.
 *
 * @author Eric Thill
 */
class RollingCoordinator implements AutoCloseable {

  private final AtomicBoolean allocating = new AtomicBoolean(false);
  private final AtomicReference<MappedConcurrentFile> curFileRef = new AtomicReference<>();
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);

  private final MappedFileProvider mappedFileProvider;
  private final boolean asyncClose;
  private final boolean yieldOnAllocateContention;
  private final FileCompleteFunction fileCompleteFunction;

  RollingCoordinator(final MappedFileProvider mappedFileProvider,
                     final boolean yieldOnAllocateContention,
                     final boolean asyncClose,
                     final FileCompleteFunction fileCompleteFunction) throws IOException {
    this.mappedFileProvider = mappedFileProvider;
    this.yieldOnAllocateContention = yieldOnAllocateContention;
    this.asyncClose = asyncClose;
    this.fileCompleteFunction = fileCompleteFunction;
    this.curFileRef.set(mappedFileProvider.nextFile());
  }

  @Override
  public void close() {
    keepRunning.set(false);
    close(currentFile(), false);
  }

  public MappedConcurrentFile currentFile() {
    return curFileRef.get();
  }

  public MappedConcurrentFile fileForWrite() throws IOException {
    final MappedConcurrentFile curFile = curFileRef.get();
    if(curFile.hasAvailableCapacity()) {
      return curFile;
    } else {
      allocateLock();
      try {
        if(curFileRef.get() == curFile) {
          // expected current mapper is actual current mapper -> this thread wins, close current file and set new file
          close(curFile, asyncClose);
          final MappedConcurrentFile newFile = mappedFileProvider.nextFile();
          curFileRef.set(newFile);
          return newFile;
        } else {
          // expected current mapper is not current mapper -> this thread did not win, return the updated curFileRef that has changed since the method was called
          return curFileRef.get();
        }
      } finally {
        allocateUnlock();
      }
    }
  }

  private void allocateLock() {
    while(!allocating.compareAndSet(false, true)) {
      if(yieldOnAllocateContention) {
        Thread.yield();
      }
    }
  }

  private void allocateUnlock() {
    allocating.set(false);
  }

  private void close(final MappedConcurrentFile concurrentFile, final boolean async) {
    final Runnable closeTask = () -> {
      try {
        while(concurrentFile.isPending()) {
          if(yieldOnAllocateContention)
            Thread.yield();
        }
        concurrentFile.close();
        if(fileCompleteFunction != null)
          fileCompleteFunction.onComplete(concurrentFile.getFile());
      } catch(Throwable t) {
        t.printStackTrace();
      }
    };
    if(async) {
      new Thread(closeTask, getClass().getSimpleName() + "-Close").start();
    } else {
      closeTask.run();
    }
  }

}
