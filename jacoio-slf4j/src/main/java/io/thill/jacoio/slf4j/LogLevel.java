package io.thill.jacoio.slf4j;

enum LogLevel {
  OFF(0), ERROR(1), WARN(2), INFO(3), DEBUG(4), TRACE(5);
  private final int value;

  LogLevel(int value) {
    this.value = value;
  }

  public boolean isEnabled(LogLevel configuredLevel) {
    return value <= configuredLevel.value;
  }
}
