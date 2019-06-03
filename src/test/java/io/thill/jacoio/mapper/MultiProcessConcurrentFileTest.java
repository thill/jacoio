package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import static io.thill.jacoio.mapper.MultiProcessConcurrentFile.HEADER_SIZE;

public class MultiProcessConcurrentFileTest extends SingleProcessConcurrentFileTest {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  protected void createFile(int capacity, boolean fillWithZeros) throws Exception {
    if(file != null)
      file.close();
    File underlyingFile = File.createTempFile(getClass().getSimpleName(), ".bin");
    while(!underlyingFile.delete())
      Thread.sleep(10);
    logger.info("Testing with file at {}", underlyingFile.getAbsolutePath());

    file = ConcurrentFile.map()
            .location(underlyingFile)
            .capacity(capacity)
            .fillWithZeros(fillWithZeros)
            .multiProcess(true)
            .map();

    Assert.assertEquals(MultiProcessConcurrentFile.class, file.getClass());
  }

  @Override
  protected int startOffset() {
    return HEADER_SIZE;
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

}
