package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import io.thill.jacoio.function.FileCompleteListener;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Ignore // This takes a long time. I run it manually instead of at every build.
public class StressTest {

  private static final File LOCATION = new File("target/testdata");
  private static final int CAPACITY = 128 * 1024 * 1024; // 128 MB
  private static final int NUM_WRITERS = 3;
  private static final long RUNTIME_MILLIS = 3000;
  private static final String FILE_SUFFIX = ".bin";

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private void test(boolean multiProcess, boolean closeAsync, boolean preallocate) throws Exception {
    IoUtil.delete(LOCATION, true);
    final List<ConcurrentFile> concurrentFiles = new ArrayList<>();
    try {
      LOCATION.mkdirs();
      final List<ConcurrentFileWriter> writers = new ArrayList<>();

      if(multiProcess) {
        for(int id = 0; id < NUM_WRITERS; id++) {
          final ConcurrentFile file = ConcurrentFile.map()
                  .location(LOCATION)
                  .capacity(CAPACITY)
                  .multiProcess(true)
                  .roll(r -> r
                          .enabled(true)
                          .asyncClose(closeAsync)
                          .preallocate(preallocate)
                          .preallocateCheckMillis(5)
                          .fileNameSuffix(FILE_SUFFIX)
                          .fileNamePrefix("")
                          .fileCompleteListener(fileCompleteListener)
                  ).map();
          concurrentFiles.add(file);
          writers.add(new ConcurrentFileWriter(file, id));
        }
      } else {
        final ConcurrentFile file = ConcurrentFile.map()
                .location(LOCATION)
                .capacity(CAPACITY)
                .multiProcess(false)
                .roll(r -> r
                        .enabled(true)
                        .asyncClose(closeAsync)
                        .preallocate(preallocate)
                        .preallocateCheckMillis(5)
                        .fileNameSuffix(FILE_SUFFIX)
                        .fileNamePrefix("")
                        .fileCompleteListener(fileCompleteListener)
                ).map();
        concurrentFiles.add(file);

        for(int id = 0; id < NUM_WRITERS; id++)
          writers.add(new ConcurrentFileWriter(file, id));
      }

      logger.info("Starting {} Writers", NUM_WRITERS);
      for(ConcurrentFileWriter writer : writers)
        new Thread(writer, "Writer-" + writer.id).start();

      logger.info("Waiting {} Milliseconds", RUNTIME_MILLIS);
      Thread.sleep(RUNTIME_MILLIS);

      logger.info("Stopping Writers");
      for(ConcurrentFileWriter writer : writers)
        writer.close();

      for(ConcurrentFileWriter writer : writers) {
        logger.info("Waiting for Writer {} to Stop", writer.id);
        while(!writer.isClosed()) {
          Thread.yield();
        }
      }

      closeAndClear(concurrentFiles);

      if(closeAsync || preallocate) {
        Thread.sleep(100);
      }

      // load files and sort alphabetically
      File[] files = LOCATION.listFiles(f -> f.getName().endsWith(FILE_SUFFIX));
      Arrays.sort(files, Comparator.comparing(File::getName));

      logger.info("Validating {} Files: {}", files.length, files);
      final long[] lastSequences = new long[NUM_WRITERS];
      for(File file : files) {
        logger.info("Validating {}", file.getPath());
        if(checkFileHasData(file, multiProcess)) {
          validateSequences(file, multiProcess, lastSequences);
        } else {
          logger.info("{} does not contain any data", file.getPath());
        }
      }

      logger.info("Validating all writers wrote at least 1 record");
      for(int id = 0; id < NUM_WRITERS; id++) {
        final long numRecords = lastSequences[id];
        logger.info("Writer {} numRecords={}", id, numRecords);
        Assert.assertTrue(numRecords > 0);
      }

      for(int id = 0; id < NUM_WRITERS; id++) {
        final long lastReadSequence = lastSequences[id];
        logger.info("Validating Writer {} lastSequence={}", id, lastReadSequence);
        final long expectedLastSequence = writers.get(id).sequence.get();
        Assert.assertEquals(expectedLastSequence, lastReadSequence);
      }
    } finally {
      closeAndClear(concurrentFiles);
      logger.info("Deleting Test Files");
      IoUtil.delete(LOCATION, true);
    }

    logger.info("Complete");
  }

  private boolean checkFileHasData(File file, boolean multiProcess) throws IOException {
    try(final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
      // ignore header in multi-process files
      if(multiProcess) {
        dis.readFully(new byte[MultiProcessConcurrentFile.HEADER_SIZE]);
      }
      int numValidated = 0;
      while(dis.available() > 0) {
        // any non zero byte means there's data here
        if(dis.readByte() != 0) {
          return true;
        }
        numValidated++;
        if(numValidated >= 1024) {
          // after 1024 bytes of zeros, we call it quits. no data here.
          return false;
        }
      }
    }
    return false;
  }

  private void validateSequences(File file, boolean multiProcess, long[] lastSequences) throws IOException {
    try(final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
      boolean keepValidatingCurrentFile = true;

      // ignore header in multi-process files
      if(multiProcess) {
        dis.readFully(new byte[MultiProcessConcurrentFile.HEADER_SIZE]);
      }

      // validate all of the records are in order per writer
      final byte[] record = new byte[12];
      final DirectBuffer recordBuffer = new UnsafeBuffer(record);
      while(keepValidatingCurrentFile && dis.available() > 0) {
        dis.readFully(record);
        final int id = recordBuffer.getInt(0);
        final long seq = recordBuffer.getLong(4);
        long lastSeq = lastSequences[id];
        if(lastSeq == 0 && seq != 1)
          Assert.fail("Writer " + id + " first sequence was " + seq);
        else if(seq != (lastSeq + 1))
          Assert.fail("Writer " + id + " is missing sequences " + (lastSeq + 1) + " through " + (seq - 1));
        lastSequences[id] = seq;
      }
    }
  }

  private final FileCompleteListener fileCompleteListener = new FileCompleteListener() {
    @Override
    public void onComplete(ConcurrentFile concurrentFile) {
      logger.info("Complete: {}", concurrentFile.getFile().getPath());
      concurrentFile.finish();
    }
  };

  private void closeAndClear(List<ConcurrentFile> concurrentFiles) throws Exception {
    if(concurrentFiles.size() > 0) {
      logger.info("Closing {} Concurrent Files...", concurrentFiles.size());
      for(ConcurrentFile f : concurrentFiles)
        f.close();
      concurrentFiles.clear();
    }
  }

  @Test
  public void singleProcessStressTest_closeInline_noPreallocation() throws Exception {
    test(false, false, false);
  }

  @Test
  public void singleProcessStressTest_closeAsync_noPreallocation() throws Exception {
    test(false, true, false);
  }

  @Test
  public void singleProcessStressTest_closeInline_preallocation() throws Exception {
    test(false, false, true);
  }

  @Test
  public void singleProcessStressTest_closeAsync_preallocation() throws Exception {
    test(false, true, true);
  }

  @Test
  public void multiProcessStressTest_closeInline_noPreallocation() throws Exception {
    test(true, false, false);
  }

  @Test
  public void multiProcessStressTest_closeAsync_noPreallocation() throws Exception {
    test(true, true, false);
  }

  @Test
  public void multiProcessStressTest_closeInline_preallocation() throws Exception {
    test(true, false, true);
  }

  @Test
  public void multiProcessStressTest_closeAsync_preallocation() throws Exception {
    test(true, true, true);
  }

  private static class ConcurrentFileWriter implements Runnable, AutoCloseable {

    private final AtomicLong sequence = new AtomicLong(0);
    private final AtomicBoolean keepRunning = new AtomicBoolean(true);
    private final ConcurrentFile file;
    private final int id;
    private volatile boolean closed = false;

    public ConcurrentFileWriter(ConcurrentFile file, int id) {
      this.file = file;
      this.id = id;
    }

    @Override
    public void close() {
      keepRunning.set(false);
    }

    @Override
    public void run() {
      try {
        while(keepRunning.get()) {
          file.write(12, (buffer, offset, length) -> {
            buffer.putInt(offset, id);
            buffer.putLong(offset + 4, sequence.incrementAndGet());
          });
        }
      } catch(IOException e) {
        e.printStackTrace();
      }
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }
  }

}
