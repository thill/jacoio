# jacoio
#### Java Concurrent I/O: Lock-Free and Multi-Process


## Modules
- `jacoio` - Lock-free concurrent file writing using `ConcurrentFile` implementations
- `jacoio-slf4j` - Implements an SLF4J binding that writes to a rolling `ConcurrentFile`


## How it works
Each process/thread is able to reserve space in the file atomically, then write to that reserved space. This means that all write contention is resolved with a single atomic operation before performing the underlying I/O.
  

## ConcurrentFile
The entire API centers around the `ConcurrentFile` interface. This interface provides the API for concurrent, lock-free writing to underlying files. Here are some of the primary method signatures:
```
int write      ( byte[] bytes, int offset, int length )
int write      ( DirectBuffer buffer, int offset, int length )
int write      ( ByteBuffer byteBuffer )
int write      ( int length, WriteFunction writeFunction )
int writeAscii ( CharSequence string )
int writeChars ( CharSequence string, ByteOrder byteOrder )
```
See [ConcurrentFile](jacoio/src/main/java/io/thill/jacoio/ConcurrentFile.java) for the full JavaDoc


## Single File

Create a single file using the following code:
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_file.bin")
    .capacity(4096)
    .map();
```

You may now treat it as any ConcurrentFile. When the file runs out of room, `write` calls will return -1.


## Rolling Files
Create rolling files using the following code:
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .roll(r -> r
        .enabled(true)
        .fileNamePrefix("my_file-")
        .fileNameSuffix(".bin")
    ).map();
```
You may now treat it as any ConcurrentFile. When the current file runs out of room, a new file will automatically be allocated in the `location` directory.


## Framing
By default, there is no framing around individual writes. To frame individual write calls, set `framed` to `true` when mapping the file. 
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_file.bin")
    .capacity(4096)
    .framed(true)
    .map();
```
All writes will be preceded with a 4-byte, little-endian integer representing the size of the write. 


## Multi-Process
By default, ConcurrentFiles can only be used by one process at a time. Optionally, a 32-byte header can be added to the underlying file to coordinate multi-process writes. To enable this, set `multiProcess` to true when mapping the file.
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .multiProcess(true)
    .map();
```
Multi-process file rolling is also supported by using a coordination file. See the following example:
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .multiProcess(true)
    .roll(r -> r
        .enabled(true)
        .fileNamePrefix("my_file-")
        .fileNameSuffix(".bin")
        .coordinationFile(new File("my_coordinator"))
    ).map();
```
Note: If the `coordinationFile` is not set, it will default to `${location}/roll.coordinator`


## Preallocation
When rolling is enabled, files are allocated inline by default. To spin up a separate thread that will preallocate files to be atomically swapped in, see the following example:
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .roll(r -> r
        .enabled(true)
        .fileNamePrefix("my_file-")
        .fileNameSuffix(".bin")
        .preallocate(true)
    ).map();
```
Note: You can change the preallocation monitoring interval using `.preallocateCheckMillis(long)`


## Asynchronous Closing
When rolling is enabled, files are closed inline by default. To close each file in a separate thread, see the following example:
```
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .roll(r -> r
        .enabled(true)
        .fileNamePrefix("my_file-")
        .fileNameSuffix(".bin")
        .closeAsync(true)
    ).map();
```
 

## Rolling FileProvider
By default, rolling files are named according to `${fileNamePrefix}${datetime}${fileNameSuffix}`. This can be overwritten with a `FileProvider`. See the following example:
```
final AtomicLong fileNumber = new AtomicLong();
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .roll(r -> r
        .enabled(true)
        .fileProvider(() -> new File(fileNumber.incrementAndGet() + ".bin"))
    ).map();
```


## Rolling FileCompleteListener
If you want to get a callback for a file when it is being closed/rolled, you may specify a `FileCompleteListener`. See the following example that prints filenames as they are rolled.
```
final AtomicLong fileNumber = new AtomicLong();
ConcurrentFile file = ConcurrentFile.map()
    .location(new File("my_directory/")
    .capacity(4096)
    .roll(r -> r
        .enabled(true)
        .fileCompleteListener(
            concurrentFile -> System.out.println("Complete: " + concurrentFile.getFile().getPath())
        )
    ).map();
```
See Also: `FileCreatedListener`, `FileMappedListener`, and `FileClosedListener`


## Quick Reference: Mapper Properties
```
final ConcurrentFile concurrentFile = ConcurrentFile.map()
    .location(location)   // The location of the file. When rolling is enabled, this is the directory to create files. Required.
    .capacity(logSize)    // The capacity of the new file. Required.
    .framed(false)        // Enable message framing via a leading 4-byte little-endian integer for every write. Defaults to false.
    .fillWithZeros(true)  // Fill new files with 0's. Setting to false will speed up allocation. Defaults to true.
    .multiProcess(false)  // Enable multi-process write compatibility using a 32-byte header in the file. Defaults to false.
    .roll(r -> r
        .enabled(true)                   // Enable file rolling. When true, location is used as a directory. Defaults to false.       
        .preallocate(true)               // Preallocate new files in a separate thread. Defaults to false. Defaults to false.
        .preallocateCheckMillis(100)     // The interval in which the preallocation thread checks to preallocate. Defaults to 100.
        .asyncClose(true)                // Set to true to close files in a separate thread. Otherwise they are closed inline. Defaults to false.
        .fileNamePrefix("test-")         // The prefix to add to all rolling filenames. Defaults to an empty string.
        .fileNameSuffix(".bin")          // The suffix to add to all rolling filename. Defaults to an empty string.
        .dateFormat("yyyyMMdd_HHmm")     // The format to use for the date in rolling filenames. Defaults to yyyyMMdd_HHmmss.
        .fileProvider(myFileProvider)    // Optionally used to override the default ${prefix}${datetime}${suffix} rolling file format.
        .yieldOnAllocateContention(true) // Flag to call Thread.yield() while waiting for another thread to finish allocating a new file. Defaults to true.
        .fileCreatedListener(myFunc)     // Function to handle files when they are created. This will be called from the thread that creates the file.
        .fileMappedListener(myFunc)      // Function to handle files when they are mapped to be used. This will be called from the thread that cycles the file in for use.
        .fileCompleteListener(myFunc)    // Function to handle files before they are closed and rolled. This will be called from the thread that is closing the file.
        .fileClosedListener(myFunc)      // Function to handle files after they have been unmapped and closed. This will be called from the thread that is closing the file.
    ).map();
```
