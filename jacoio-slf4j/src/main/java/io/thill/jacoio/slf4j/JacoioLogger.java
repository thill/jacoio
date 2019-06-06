package io.thill.jacoio.slf4j;

import org.slf4j.helpers.MarkerIgnoringBase;

public class JacoioLogger extends MarkerIgnoringBase {

  public static final String SYSKEY_LOG_LEVEL = "slf4j.log.level";
  public static final String SYSKEY_LOG_DIRECTORY = "slf4j.log.dir";
  public static final String SYSKEY_LOG_FILENAME_PREFIX = "slf4j.log.name.prefix";
  public static final String SYSKEY_LOG_FILENAME_SUFFIX = "slf4j.log.name.suffix";
  public static final String SYSKEY_LOG_FILENAME_TIMESTAMP_FORMAT = "slf4j.log.name.timestamp";
  public static final String SYSKEY_LOG_TIMESTAMP_FORMAT = "slf4j.log.timestamp";
  public static final String SYSKEY_LOG_SIZE = "slf4j.log.size";
  public static final String SYSKEY_LOG_PREALLOCATE = "slf4j.log.preallocate";

  private final LogWriter logWriter;

  JacoioLogger(final String name, final LogWriter logWriter) {
    this.name = name;
    this.logWriter = logWriter;
  }

  @Override
  public boolean isTraceEnabled() {
    return logWriter.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    logWriter.trace(name, msg);
  }

  @Override
  public void trace(String format, Object arg) {
    logWriter.trace(name, format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    logWriter.trace(name, format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    logWriter.trace(name, format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    logWriter.trace(name, msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return logWriter.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    logWriter.debug(name, msg);
  }

  @Override
  public void debug(String format, Object arg) {
    logWriter.debug(name, format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    logWriter.debug(name, format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    logWriter.debug(name, format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    logWriter.debug(name, msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return logWriter.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    logWriter.info(name, msg);
  }

  @Override
  public void info(String format, Object arg) {
    logWriter.info(name, format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    logWriter.info(name, format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    logWriter.info(name, format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    logWriter.info(name, msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return logWriter.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    logWriter.warn(name, msg);
  }

  @Override
  public void warn(String format, Object arg) {
    logWriter.warn(name, format, arg);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    logWriter.warn(name, format, arg1, arg2);
  }

  @Override
  public void warn(String format, Object... arguments) {
    logWriter.warn(name, format, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    logWriter.warn(name, msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return logWriter.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    logWriter.error(name, msg);
  }

  @Override
  public void error(String format, Object arg) {
    logWriter.error(name, format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    logWriter.error(name, format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    logWriter.error(name, format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    logWriter.error(name, msg, t);
  }
}
