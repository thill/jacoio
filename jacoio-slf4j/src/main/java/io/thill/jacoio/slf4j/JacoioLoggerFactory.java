package io.thill.jacoio.slf4j;

import io.thill.jacoio.ConcurrentFile;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;

import static io.thill.jacoio.slf4j.JacoioLogger.*;

public class JacoioLoggerFactory implements ILoggerFactory {

  private static final String DEFAULT_LOG_LEVEL = "INFO";
  private static final String DEFAULT_LOG_DIRECTORY = "log";
  private static final String DEFAULT_LOG_FILENAME_PREFIX = "";
  private static final String DEFAULT_LOG_FILENAME_SUFFIX = ".log";
  private static final String DEFAULT_LOG_FILENAME_TIMESTAMP_FORMAT = "yyyyMMdd_HHmmss";
  private static final String DEFAULT_LOG_TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";
  private static final String DEFAULT_LOG_SIZE = Integer.toString(128 * 1024 * 1024);
  private static final String DEFAULT_LOG_PREALLOCATE = Boolean.FALSE.toString();

  private final LogWriter logWriter;

  public JacoioLoggerFactory() {
    LogWriter logWriter;
    try {
      final LogLevel logLevel = LogLevel.valueOf(System.getProperty(SYSKEY_LOG_LEVEL, DEFAULT_LOG_LEVEL));
      final File location = new File(System.getProperty(SYSKEY_LOG_DIRECTORY, DEFAULT_LOG_DIRECTORY));
      final int logSize = Integer.parseInt(System.getProperty(SYSKEY_LOG_SIZE, DEFAULT_LOG_SIZE));
      final boolean preallocate = Boolean.parseBoolean(System.getProperty(SYSKEY_LOG_PREALLOCATE, DEFAULT_LOG_PREALLOCATE));
      final String filenamePrefix = System.getProperty(SYSKEY_LOG_FILENAME_PREFIX, DEFAULT_LOG_FILENAME_PREFIX);
      final String filenameSuffix = System.getProperty(SYSKEY_LOG_FILENAME_SUFFIX, DEFAULT_LOG_FILENAME_SUFFIX);
      final String filenameTimestampFormat = System.getProperty(SYSKEY_LOG_FILENAME_TIMESTAMP_FORMAT, DEFAULT_LOG_FILENAME_TIMESTAMP_FORMAT);
      final String logTimestampFormat = System.getProperty(SYSKEY_LOG_TIMESTAMP_FORMAT, DEFAULT_LOG_TIMESTAMP_FORMAT);

      final StringBuilder stderr = new StringBuilder("Jacoio Logger:")
              .append("\n  Level: ").append(logLevel)
              .append("\n  Location: ").append(location.getAbsolutePath())
              .append("\n  Size: ").append(logSize)
              .append("\n  Preallocate: ").append(preallocate)
              .append("\n  Filename Prefix: ").append(filenamePrefix)
              .append("\n  Filename Suffix: ").append(filenameSuffix)
              .append("\n  Filename Timestamp Format: ").append(filenameTimestampFormat)
              .append("\n  Log Timestamp Format: ").append(logTimestampFormat);
      System.err.println(stderr.toString());

      location.mkdirs();

      final ConcurrentFile concurrentFile = ConcurrentFile.map()
              .location(location)
              .capacity(logSize)
              .framed(false)
              .fillWithZeros(true)
              .multiProcess(false)
              .roll(r -> r
                      .enabled(true)
                      .preallocate(false)
                      .asyncClose(true)
                      .preallocate(preallocate)
                      .fileNamePrefix(filenamePrefix)
                      .fileNameSuffix(filenameSuffix)
                      .dateFormat(filenameTimestampFormat)
              ).map();
      logWriter = new LogWriter(logLevel, concurrentFile, new SimpleDateFormat(logTimestampFormat));
    } catch(Throwable t) {
      System.err.println("Could not initialize Jacoio Logger");
      t.printStackTrace();
      logWriter = new LogWriter(LogLevel.OFF, null, null);
    }
    this.logWriter = logWriter;
  }

  @Override
  public Logger getLogger(String name) {
    return new JacoioLogger(name, logWriter);
  }

}
