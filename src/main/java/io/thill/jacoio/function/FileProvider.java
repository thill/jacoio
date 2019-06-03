package io.thill.jacoio.function;

import java.io.File;

@FunctionalInterface
public interface FileProvider {
  File nextFile();
}
