package org.slf4j.impl;

import io.thill.jacoio.slf4j.JacoioLoggerFactory;
import org.slf4j.ILoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

public class StaticLoggerBinder implements LoggerFactoryBinder {

  public static String REQUESTED_API_VERSION = "1.7.25";

  private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();
  private static final String loggerFactoryClassStr = JacoioLoggerFactory.class.getName();
  private final ILoggerFactory loggerFactory = new JacoioLoggerFactory();

  public static final StaticLoggerBinder getSingleton() {
    return SINGLETON;
  }

  private StaticLoggerBinder() {
  }

  public ILoggerFactory getLoggerFactory() {
    return this.loggerFactory;
  }

  public String getLoggerFactoryClassStr() {
    return loggerFactoryClassStr;
  }
}

