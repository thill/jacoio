package io.thill.jacoio.benchmark;

import io.thill.jacoio.ConcurrentFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentFileThroughput {

  public static void main(String[] args) {
    final File directory = new File("target/benchmark");
    directory.mkdirs();
    new ConcurrentFileThroughput(directory, 1024 * 1024 * 128, 128, 10).execute(1, 10);
  }

  private final Random random = new Random();
  private final File directory;
  private final int fileSize;
  private final int writeSize;
  private final int runtimeSeconds;
  private int fileNumber = 0;
  private volatile ConcurrentFile curFile;
  private volatile boolean keepRunning;

  public ConcurrentFileThroughput(File directory, int fileSize, int writeSize, int runtimeSeconds) {
    this.directory = directory;
    this.fileSize = fileSize;
    this.writeSize = writeSize;
    this.runtimeSeconds = runtimeSeconds;
  }

  public void execute(final int startNumThreads, final int endNumThreads) {
    for(int numThreads = startNumThreads; numThreads <= endNumThreads; numThreads++) {
      execute(numThreads);
    }
  }

  public void execute(final int numThreads) {
    try {
      System.out.print("numThreads: " + numThreads);

      // delete any existing files in the directory
      for(File f : directory.listFiles()) {
        while(!f.delete())
          Thread.sleep(10);
      }

      // create the writer threads
      List<WriterThread> writerThreads = new ArrayList<>();
      for(int i = 0; i < numThreads; i++)
        writerThreads.add(new WriterThread(i));

      // create the first file to write
      nextFile();

      // start the writer threads
      keepRunning = true;
      for(WriterThread writerThread : writerThreads)
        new Thread(writerThread).start();

      // wait for the test time
      Thread.sleep(TimeUnit.SECONDS.toMillis(runtimeSeconds));

      // stop running and wait for all threads to exit
      keepRunning = false;
      for(WriterThread writerThread : writerThreads) {
        while(!writerThread.isStopped())
          Thread.sleep(10);
      }

      // delete the last file
      while(curFile.isPending())
        Thread.sleep(10);
      curFile.close();
      curFile.file().delete();
      curFile = null;

      // aggregate results
      long totalWrites = 0;
      for(WriterThread writerThread : writerThreads)
        totalWrites += writerThread.getTotalWrites();

      // print the results
      System.out.println(" - MB/second:  " + (totalWrites * writeSize) / runtimeSeconds / (1024.0 * 1024.0));
    } catch(Throwable t) {
      t.printStackTrace();
    }
  }

  private void nextFile() {
    try {
      curFile = ConcurrentFile.map().multiProcess(false).location(new File(directory, Integer.toString(fileNumber++))).capacity(fileSize).map();
    } catch(Throwable t) {
      t.printStackTrace();
      curFile = null;
    }
  }

  private class WriterThread implements Runnable {
    private final int index;
    private final byte[] writeArray;
    private AtomicLong totalWrites = new AtomicLong();
    private volatile boolean stopped = false;

    public WriterThread(int index) {
      this.index = index;
      this.writeArray = new byte[writeSize];
      random.nextBytes(writeArray);
    }

    @Override
    public void run() {
      while(keepRunning) {
        final ConcurrentFile curFile = ConcurrentFileThroughput.this.curFile;
        final int offset = curFile.write(writeArray, 0, writeArray.length);
        if(offset >= 0) {
          totalWrites.incrementAndGet();
        } else if(index == 0) {
          final ConcurrentFile lastFile = curFile;
          nextFile();
          new Thread(() -> {
            try {
              while(curFile.isPending())
                Thread.sleep(10);
              lastFile.close();
              while(!lastFile.file().delete())
                Thread.sleep(10);
            } catch(Throwable t) {
              t.printStackTrace();
            }
          }).start();
        }
      }
      stopped = true;
    }

    public boolean isStopped() {
      return stopped;
    }

    public long getTotalWrites() {
      return totalWrites.get();
    }
  }
}
