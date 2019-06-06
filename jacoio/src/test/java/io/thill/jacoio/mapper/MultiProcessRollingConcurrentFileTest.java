package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static io.thill.jacoio.mapper.MultiProcessConcurrentFile.HEADER_SIZE;

public class MultiProcessRollingConcurrentFileTest extends SingleProcessRollingConcurrentFileTest {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private File tmpDirectory;

  @After
  public void cleanup() throws Exception {
    if(file != null) {
      file.close();
      file = null;
    }
    if(tmpDirectory != null) {
      logger.info("Deleting {}", tmpDirectory.getAbsolutePath());
      IoUtil.delete(tmpDirectory, false);
      tmpDirectory = null;
    }
  }

  @Override
  protected void createFile(int capacity, boolean fillWithZeros) throws Exception {
    if(file != null)
      file.close();
    tmpDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();
    logger.info("Testing with directory at {}", tmpDirectory.getAbsolutePath());

    file = ConcurrentFile.map()
            .location(tmpDirectory)
            .capacity(capacity)
            .fillWithZeros(fillWithZeros)
            .multiProcess(true)
            .roll(roll -> roll
                    .enabled(true)
                    .fileNamePrefix("test-")
                    .fileNameSuffix(".bin")
                    .asyncClose(false)
                    .fileCompleteFunction(f -> logger.info("Complete: {}", f.getFile().getAbsolutePath()))
                    .yieldOnAllocateContention(true)
            )
            .map();

    Assert.assertEquals(RollingConcurrentFile.class, file.getClass());
  }

  @Test
  public void testMapExistingFile() throws Exception {
    createFile(128, false);
    file.write(ByteBuffer.wrap("Hello ".getBytes()));

    try(ConcurrentFile existing = MultiProcessConcurrentFile.map(file.getFile(), 128, false)) {
      existing.write(ByteBuffer.wrap("World!".getBytes()));
      assertBytesAt("Hello World!".getBytes(), existing, HEADER_SIZE);
    }

    assertBytesAt("Hello World!".getBytes(), file, HEADER_SIZE);
  }



  @Override
  protected int startOffset() {
    return MultiProcessConcurrentFile.HEADER_SIZE;
  }

}
