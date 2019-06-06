package io.thill.jacoio.slf4j;

import io.thill.jacoio.ConcurrentFile;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;

class LogWriter {

  private final LogLevel logLevel;
  private final ConcurrentFile concurrentFile;
  private final DateFormat dateFormat;

  LogWriter(LogLevel logLevel, ConcurrentFile concurrentFile, DateFormat dateFormat) {
    this.logLevel = logLevel;
    this.concurrentFile = concurrentFile;
    this.dateFormat = dateFormat;
  }

  private void log(LogLevel level, String name, String message, Throwable t) {
    if (level.isEnabled(logLevel)) {

      StringBuilder sb = new StringBuilder(32);
      sb.append(dateFormat.format(System.currentTimeMillis()));

      sb.append(" [");
      sb.append(Thread.currentThread().getName());
      sb.append("] ");

      sb.append(level.toString());
      sb.append(" ");

      sb.append(name);
      sb.append(" - ");

      sb.append(message);
      sb.append("\n");

      if (t != null) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        sb.append(sw.toString());
        sb.append("\n");
      }

      try {
        concurrentFile.writeAscii(sb);
      } catch (Throwable caught) {
        caught.printStackTrace();
      }
    }
  }

  public boolean isTraceEnabled() {
    return LogLevel.TRACE.isEnabled(logLevel);
  }

  public void trace(String name, String msg) {
    this.log(LogLevel.TRACE, name, msg, (Throwable) null);
  }

  public void trace(String name, String format, Object param1) {
    this.formatAndLog(LogLevel.TRACE, name, format, param1, (Object) null);
  }

  public void trace(String name, String format, Object param1, Object param2) {
    this.formatAndLog(LogLevel.TRACE, name, format, param1, param2);
  }

  public void trace(String name, String format, Object... argArray) {
    this.formatAndLog(LogLevel.TRACE, name, format, argArray);
  }

  public void trace(String name, String msg, Throwable t) {
    this.log(LogLevel.TRACE, name, msg, t);
  }

  public boolean isDebugEnabled() {
    return LogLevel.DEBUG.isEnabled(logLevel);
  }

  public void debug(String name, String msg) {
    this.log(LogLevel.DEBUG, name, msg, (Throwable) null);
  }

  public void debug(String name, String format, Object param1) {
    this.formatAndLog(LogLevel.DEBUG, name, format, param1, (Object) null);
  }

  public void debug(String name, String format, Object param1, Object param2) {
    this.formatAndLog(LogLevel.DEBUG, name, format, param1, param2);
  }

  public void debug(String name, String format, Object... argArray) {
    this.formatAndLog(LogLevel.DEBUG, name, format, argArray);
  }

  public void debug(String name, String msg, Throwable t) {
    this.log(LogLevel.DEBUG, name, msg, t);
  }

  public boolean isInfoEnabled() {
    return LogLevel.INFO.isEnabled(logLevel);
  }

  public void info(String name, String msg) {
    this.log(LogLevel.INFO, name, msg, (Throwable) null);
  }

  public void info(String name, String format, Object arg) {
    this.formatAndLog(LogLevel.INFO, name, format, arg, (Object) null);
  }

  public void info(String name, String format, Object arg1, Object arg2) {
    this.formatAndLog(LogLevel.INFO, name, format, arg1, arg2);
  }

  public void info(String name, String format, Object... argArray) {
    this.formatAndLog(LogLevel.INFO, name, format, argArray);
  }

  public void info(String name, String msg, Throwable t) {
    this.log(LogLevel.INFO, name, msg, t);
  }

  public boolean isWarnEnabled() {
    return LogLevel.WARN.isEnabled(logLevel);
  }

  public void warn(String name, String msg) {
    this.log(LogLevel.WARN, name, msg, (Throwable) null);
  }

  public void warn(String name, String format, Object arg) {
    this.formatAndLog(LogLevel.WARN, name, format, arg, (Object) null);
  }

  public void warn(String name, String format, Object arg1, Object arg2) {
    this.formatAndLog(LogLevel.WARN, name, format, arg1, arg2);
  }

  public void warn(String name, String format, Object... argArray) {
    this.formatAndLog(LogLevel.WARN, name, format, argArray);
  }

  public void warn(String name, String msg, Throwable t) {
    this.log(LogLevel.WARN, name, msg, t);
  }

  public boolean isErrorEnabled() {
    return LogLevel.ERROR.isEnabled(logLevel);
  }

  public void error(String name, String msg) {
    this.log(LogLevel.ERROR, name, msg, (Throwable) null);
  }

  public void error(String name, String format, Object arg) {
    this.formatAndLog(LogLevel.ERROR, name, format, arg, (Object) null);
  }

  public void error(String name, String format, Object arg1, Object arg2) {
    this.formatAndLog(LogLevel.ERROR, name, format, arg1, arg2);
  }

  public void error(String name, String format, Object... argArray) {
    this.formatAndLog(LogLevel.ERROR, name, format, argArray);
  }

  public void error(String name, String msg, Throwable t) {
    this.log(LogLevel.ERROR, name, msg, t);
  }

  private void formatAndLog(LogLevel level, String name, String format, Object arg1, Object arg2) {
    if (level.isEnabled(logLevel)) {
      final FormattingTuple tp = MessageFormatter.format(format, arg1, arg2);
      log(level, name, tp.getMessage(), tp.getThrowable());
    }
  }

  private void formatAndLog(LogLevel level, String name, String format, Object... arguments) {
    if (level.isEnabled(logLevel)) {
      final FormattingTuple tp = MessageFormatter.arrayFormat(format, arguments);
      log(level, name, tp.getMessage(), tp.getThrowable());
    }
  }


}
