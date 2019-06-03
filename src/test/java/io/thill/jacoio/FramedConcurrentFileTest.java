package io.thill.jacoio;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import static io.thill.jacoio.MultiProcessConcurrentFile.HEADER_SIZE;

public class FramedConcurrentFileTest extends SingleProcessConcurrentFileTest {

  @After
  public void cleanup() throws Exception {
    if(file != null) {
      file.close();
      file = null;
    }
  }

  @Override
  protected void createFile(int capacity, boolean fillWithZeros) throws Exception {
    super.createFile(capacity, fillWithZeros);
    file = new FramedConcurrentFile((MappedConcurrentFile)file);
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
