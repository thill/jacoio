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

import io.thill.jacoio.function.FileProvider;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;

/**
 * Used for multi-process rolling file coordination
 *
 * @author Eric Thill
 */
class CoordinationFile implements AutoCloseable {


  public static File curFile(String contents) {
    return new File(contents.split(DELIMITER_SPLIT)[0]);
  }

  public static File preallocatedFile(String contents) {
    final String[] contentsArr = contents.split(DELIMITER_SPLIT);
    return contentsArr.length > 1 ? new File(contentsArr[1]) : null;
  }

  private static final int NEW_COORDINATOR_FILE_SIZE = 1024 * 256;
  private static final int LOCK_OFFSET = 0;
  private static final int CONTENTS_OFFSET = 8;
  private static final int UNLOCKED = 0;
  private static final int LOCKED = 0;
  private static final byte NULL_TERMINATOR = 0;
  private static final String DELIMITER = "|";
  private static final String DELIMITER_SPLIT = "\\|";

  private final FileProvider underlyingFileProvider;
  private final boolean preallocate;
  private final boolean yieldOnFileContention;
  private final FileChannel coordinationFileChannel;
  private final AtomicBuffer coordinationBuffer;
  private final int coordinationFileSize;

  CoordinationFile(final File coordinationFile,
                   final FileProvider underlyingFileProvider,
                   final boolean preallocate,
                   final boolean yieldOnFileContention) throws IOException {
    this.underlyingFileProvider = underlyingFileProvider;
    this.preallocate = preallocate;
    this.yieldOnFileContention = yieldOnFileContention;
    if(coordinationFile.exists()) {
      coordinationFileChannel = FileChannel.open(coordinationFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
      coordinationFileSize = (int)coordinationFileChannel.size();
      final long address = IoUtil.map(coordinationFileChannel, MapMode.READ_WRITE, 0, coordinationFileSize);
      coordinationBuffer = new UnsafeBuffer();
      coordinationBuffer.wrap(address, coordinationFileSize);
    } else {
      coordinationFileSize = NEW_COORDINATOR_FILE_SIZE;
      coordinationFileChannel = IoUtil.createEmptyFile(coordinationFile, coordinationFileSize, true);
      final long address = IoUtil.map(coordinationFileChannel, MapMode.READ_WRITE, 0, coordinationFileSize);
      coordinationBuffer = new UnsafeBuffer();
      coordinationBuffer.wrap(address, coordinationFileSize);
    }
  }

  @Override
  public void close() throws IOException {
    coordinationFileChannel.close();
    IoUtil.unmap(coordinationFileChannel, coordinationBuffer.addressOffset(), coordinationFileSize);
  }

  /**
   * Advance to next file
   *
   * @param localContents The last contents read from the file locally. Used to determine who wins the race to update.
   * @return New contents of coordination file. This can be parsed using the static utility methods of this class.
   */
  public String next(String localContents) {
    lockFile();
    try {
      final String readContents = readContents();
      if(readContents.equalsIgnoreCase(localContents)) {
        // file contents matches our internal state -> we need to update the coordination file

        // create the next file to use
        final File nextFile = underlyingFileProvider.nextFile();
        String newContents;
        if(preallocate) {
          // preallocation is configured -> rotate preallocated file to curFile, and set preallocated file to nextFile
          final String readContentsArr[] = readContents.split(DELIMITER);
          final String readPreallocatedFilepath = readContentsArr.length > 1 ? readContentsArr[1] : "";
          newContents = readPreallocatedFilepath + DELIMITER + nextFile.getAbsolutePath();
        } else {
          // preallocation is disabled -> set the nextFile as curFile
          newContents = nextFile.getAbsolutePath();
          writeContents(newContents);
        }

        writeContents(newContents);
        return newContents;
      } else {
        // file contents do not match our internal state -> we should update our internal state with the coordination file contents
        return readContents;
      }
    } finally {
      unlockFile();
    }
  }

  private String readContents() {
    final StringBuilder sb = new StringBuilder();
    int offset = CONTENTS_OFFSET;
    char curChar;
    while((curChar = (char)coordinationBuffer.getByteVolatile(offset)) != NULL_TERMINATOR) {
      sb.append(curChar);
      offset++;
    }
    return sb.toString();
  }

  private void writeContents(final String contents) {
    for(int i = 0; i < contents.length(); i++) {
      coordinationBuffer.putByteVolatile(CONTENTS_OFFSET + i, (byte)contents.charAt(i));
    }
    coordinationBuffer.putByteVolatile(CONTENTS_OFFSET + contents.length(), NULL_TERMINATOR);
  }

  private void lockFile() {
    while(!coordinationBuffer.compareAndSetInt(LOCK_OFFSET, UNLOCKED, LOCKED)) {
      if(yieldOnFileContention)
        Thread.yield();
    }
  }

  private void unlockFile() {
    coordinationBuffer.putIntVolatile(LOCK_OFFSET, UNLOCKED);
  }

}
