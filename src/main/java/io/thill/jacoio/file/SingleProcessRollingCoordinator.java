package io.thill.jacoio.file;

import io.thill.jacoio.function.FileCompleteFunction;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class SingleProcessRollingCoordinator implements AutoCloseable {

  private final AtomicBoolean allocating = new AtomicBoolean(false);
  private final AtomicReference<MappedConcurrentFile> curFileRef = new AtomicReference<>();

  private final File parentDirectory;
  private final int fileCapacity;
  private final boolean fillWithZeros;
  private final boolean framed;
  private final String namePrefix;
  private final DateFormat dateFormat;
  private final String nameSuffix;
  private final boolean asyncClose;
  private final boolean yieldOnAllocateContention;
  private final FileCompleteFunction fileCompleteFunction;

  SingleProcessRollingCoordinator(final File parentDirectory,
                                  final int fileCapacity,
                                  final boolean fillWithZeros,
                                  final boolean framed,
                                  final String namePrefix,
                                  final DateFormat dateFormat,
                                  final String nameSuffix,
                                  final boolean yieldOnAllocateContention,
                                  final boolean asyncClose,
                                  final FileCompleteFunction fileCompleteFunction) throws IOException {
    this.parentDirectory = parentDirectory;
    this.fileCapacity = fileCapacity;
    this.fillWithZeros = fillWithZeros;
    this.framed = framed;
    this.namePrefix = namePrefix == null ? "" : namePrefix;
    this.dateFormat = dateFormat;
    this.nameSuffix = nameSuffix == null ? "" : nameSuffix;
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
          // expected current file is actual current file -> this thread wins, create file
          close(curFile, asyncClose);
          curFile = nextFile();
          curFileRef.set(curFile);
          return curFile;
        } else {
          // expected current file is not current file -> this thread did not win, return the updated curFileRef that has changed since the method was called
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
    int idx = 0;
    File file;
    do {
      String name = namePrefix + (dateFormat == null ? "" : dateFormat.format(new Date(System.currentTimeMillis()))) + (idx == 0 ? "" : "-" + idx) + nameSuffix;
      file = new File(parentDirectory, name);
      idx++;
    } while(file.exists());
    MappedConcurrentFile nextFile = SingleProcessConcurrentFile.map(file, fileCapacity, fillWithZeros);
    if(framed)
      nextFile = new FramedConcurrentFile(nextFile);
    return nextFile;
  }

}
