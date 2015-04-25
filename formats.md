# **File Formats**

All the file formats are outlined below including the header and record 
contents.

## **Log file**

Log files contain all the log records.

#### *Log File Header*

Log files are prefixed with an 8-byte header. The header contains the file
signature `LOG`, an 8-bit version followed by an unsigned 32-bit integer for 
boolean flags.

#### *Log Records*

Each log record has a 24-byte header followed by the record data. The header
consists of:

```
4-byte size
4-byte flags
8-byte time
8-byte index
data

0        8        16       24
+--------+--------+--------+--------+--------+--------+
| s + f  |  time  |  index |           data           |
+--------+--------+--------+--------+--------+--------+
```

- an unsigned 32-bit integer for the size (max 4GB) size
- an unsigned 32-bit integer for any boolean flags
- a signed 64-bit integer for the timestamp in nanoseconds
- an unsigned 64-bit record index

Record data immediately follows the header.

## **Index file**

Index files contain all the offsets for each record in the log. Index 
records are fixed-length and therefore the index is easy to search for specific
records.

#### *Index File Header*

Index files are prefixed by a 3-byte signature, `IDX`, an 8-bit version 
followed by an unsigned 32-bit integer for boolean flags.

#### *Index Records*

Index records are fixed-width at 32-bytes long. Each record consists of:

```
8-byte int64  timestamp
8-byte uint64 index
8-byte offset int64
8-byte ttl    int64

0        8        16       24       32
+--------+--------+--------+--------+
|  time  |  index | offset |   ttl  |
+--------+--------+--------+--------+
```

- a signed 64-bit integer for the timestamp
    - **Units:** nanoseconds since epoch began
- an unsigned 64-bit integer for the record index
- a signed 64-bit integer for the log file offset
- a signed 64-bit integer for the time to live
    - **Units:** nanoseconds

