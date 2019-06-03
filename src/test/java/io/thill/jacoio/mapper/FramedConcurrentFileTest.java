package io.thill.jacoio.mapper;

import io.thill.jacoio.ConcurrentFile;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FramedConcurrentFileTest extends SingleProcessConcurrentFileTest {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  @After
  public void cleanup() throws Exception {
    if(file != null) {
      file.close();
      file = null;
    }
  }

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
            .multiProcess(false)
            .framed(true)
            .map();

    Assert.assertEquals(FramedConcurrentFile.class, file.getClass());
  }

  @Override
  protected int startOffset() {
    return 0;
  }

  @Override
  protected int frameHeaderSize() {
    return 4;
  }
}
