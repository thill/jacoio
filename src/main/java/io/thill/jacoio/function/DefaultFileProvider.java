package io.thill.jacoio.function;

import java.io.File;
import java.text.DateFormat;
import java.util.Date;

public class DefaultFileProvider implements FileProvider {

  private final File directory;
  private final String namePrefix;
  private final DateFormat dateFormat;
  private final String nameSuffix;

  public DefaultFileProvider(File directory, String namePrefix, DateFormat dateFormat, String nameSuffix) {
    this.directory = directory;
    this.namePrefix = namePrefix;
    this.dateFormat = dateFormat;
    this.nameSuffix = nameSuffix;
    this.directory.mkdirs();
  }

  @Override
  public File nextFile() {
    int idx = 0;
    File file;
    do {
      String name = namePrefix + (dateFormat == null ? "" : dateFormat.format(new Date(System.currentTimeMillis()))) + (idx == 0 ? "" : "-" + idx) + nameSuffix;
      file = new File(directory, name);
      idx++;
    } while(file.exists());
    return file;
  }
}
