package io.thill.jacoio.benchmark;

import io.thill.jacoio.ConcurrentFile;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentFileThroughput {

  public static void main(String[] args) throws Exception {
    final File directory = new File("target/benchmark");
    IoUtil.delete(directory, true);
    new ConcurrentFileThroughput(directory, 1024 * 1024 * 128, 128, 10).execute(1, 10);
    IoUtil.delete(directory, true);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Random random = new Random();
  private final int writeSize;
  private final int runtimeSeconds;
  private final ConcurrentFile concurrentFile;
  private volatile boolean keepRunning;

  public ConcurrentFileThroughput(File directory, int fileSize, int writeSize, int runtimeSeconds) throws IOException {
    this.writeSize = writeSize;
    this.runtimeSeconds = runtimeSeconds;
    this.concurrentFile = ConcurrentFile.map()
            .location(directory)
            .capacity(fileSize)
            .multiProcess(false)
            .fillWithZeros(false)
            .framed(false)
            .roll(r -> r
                    .enabled(true)
                    .asyncClose(true)
                    .fileNamePrefix("test-")
                    .fileNameSuffix(".bin")
                    .yieldOnAllocateContention(true)
                    .fileCompleteFunction(f -> f.delete()))
            .map();
  }

  public void execute(final int startNumThreads, final int endNumThreads) throws Exception {
    for(int numThreads = startNumThreads; numThreads <= endNumThreads; numThreads++) {
      execute(numThreads);
    }
    concurrentFile.close();
  }

  public void execute(final int numThreads) {
    try {
      System.out.print("numThreads: " + numThreads);

      // create the writer threads
      List<WriterThread> writerThreads = new ArrayList<>();
      for(int i = 0; i < numThreads; i++)
        writerThreads.add(new WriterThread(i));

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
        try {
          final int offset = concurrentFile.write(writeArray, 0, writeArray.length);
          if(offset >= 0) {
            totalWrites.incrementAndGet();
          } else {
            logger.error("Could not write");
          }
        } catch(IOException e) {
          e.printStackTrace();
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
