package io.thill.jacoio.mapper;

import io.thill.jacoio.function.FileCompleteFunction;
import io.thill.jacoio.function.FileProvider;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class SingleProcessRollingCoordinator implements AutoCloseable {

  private final AtomicBoolean allocating = new AtomicBoolean(false);
  private final AtomicReference<MappedConcurrentFile> curFileRef = new AtomicReference<>();

  private final int fileCapacity;
  private final boolean fillWithZeros;
  private final boolean framed;
  private final FileProvider fileProvider;
  private final boolean asyncClose;
  private final boolean yieldOnAllocateContention;
  private final FileCompleteFunction fileCompleteFunction;

  SingleProcessRollingCoordinator(final int fileCapacity,
                                  final boolean fillWithZeros,
                                  final boolean framed,
                                  final FileProvider fileProvider,
                                  final boolean yieldOnAllocateContention,
                                  final boolean asyncClose,
                                  final FileCompleteFunction fileCompleteFunction) throws IOException {
    this.fileCapacity = fileCapacity;
    this.fillWithZeros = fillWithZeros;
    this.framed = framed;
    this.fileProvider = fileProvider;
    this.yieldOnAllocateContention = yieldOnAllocateContention;
    this.asyncClose = asyncClose;
    this.fileCompleteFunction = fileCompleteFunction;
    this.curFileRef.set(nextFile());
  }

  @Override
  public void close() {
    close(currentFile(), false);
  }

  public MappedConcurrentFile currentFile() {
    return curFileRef.get();
  }

  public MappedConcurrentFile fileForWrite() throws IOException {
    MappedConcurrentFile curFile = curFileRef.get();
    if(curFile.hasAvailableCapacity()) {
      return curFile;
    } else {
      while(!allocating.compareAndSet(false, true)) {
        if(yieldOnAllocateContention) {
          Thread.yield();
        }
      }

      try {
        if(curFileRef.get() == curFile) {
          // expected current mapper is actual current mapper -> this thread wins, create mapper
          close(curFile, asyncClose);
          curFile = nextFile();
          curFileRef.set(curFile);
          return curFile;
        } else {
          // expected current mapper is not current mapper -> this thread did not win, return the updated curFileRef that has changed since the method was called
          return curFileRef.get();
        }
      } finally {
        allocating.set(false);
      }
    }
  }

  private void close(final MappedConcurrentFile concurrentFile, final boolean async) {
    final Runnable closeTask = () -> {
      try {
        while(currentFile().isPending())
          Thread.sleep(10);
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

  private MappedConcurrentFile nextFile() throws IOException {
    final MappedConcurrentFile nextFile = SingleProcessConcurrentFile.map(fileProvider.nextFile(), fileCapacity, fillWithZeros);
    if(framed)
      return new FramedConcurrentFile(nextFile);
    else
      return nextFile;
  }

}
