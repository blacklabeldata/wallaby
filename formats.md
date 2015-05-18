# **File Formats**

All the file formats are outlined below including the header and record 
contents.

## **Log file**

Log files contain all the log records.

#### *Log File Header*

Log files are prefixed with an 8-byte header. The header contains the file signature `LOG`, an 8-bit version followed by an unsigned 32-bit integer for boolean flags. The file header also contains the TTL for all the records in the file. There are pros and cons in having the TTL in the file header. For instance, the individual records do not have separate TTLs. While this removes some flexibility in the data model, performance remains extremely high as each record does not need to be evaluated for expiration.

TTL is a duration specified in nanoseconds. 

```
3-byte signature
1-byte version
4-byte flags
8-byte expiration / ttl

0        1        2        3        4        5        6        7        8
+--------+--------+--------+--------+--------+--------+--------+--------+
|      File Signature      |   Ver  |               Flags               |
+--------+--------+--------+--------+--------+--------+--------+--------+
|                   Expiration Time / Time To Live                      |
+--------+--------+--------+--------+--------+--------+--------+--------+

```

- `IDX` file signature
- an unsigned 8-bit integer to represent the file version
- an unsigned 32-bit integer for boolean flags
- a signed 64-bit integer for time to live
  - **Units:** duration in nanoseconds

#### *Log Records*

Each log record has a 16-byte header followed by the record data. The header
consists of:

```
4-byte size
4-byte flags
8-byte time
data

0        8        16
+--------+--------+--------+--------+--------+
| s + f  |  time  |           data           |
+--------+--------+--------+--------+--------+
```

- an unsigned 32-bit integer for the size (max 4GB) size
- an unsigned 32-bit integer for any boolean flags
- a signed 64-bit integer for the timestamp in nanoseconds

Record data immediately follows the header.

## **Index file**

Index files contain all the offsets for each record in the log. Index 
records are fixed-length and therefore the index is easy to search for specific
records.

#### *Index File Header*

Index File headers are nearly the same as the Log file header. The only difference is the file signature (IDX).

TTL is a duration specified in nanoseconds. 

```
3-byte signature
1-byte version
4-byte boolean flags
8-byte expiration / ttl

0        1        2        3        4        5        6        7        8
+--------+--------+--------+--------+--------+--------+--------+--------+
|      File Signature      |   Ver  |               Flags               |
+--------+--------+--------+--------+--------+--------+--------+--------+
|                   Expiration Time / Time To Live                      |
+--------+--------+--------+--------+--------+--------+--------+--------+

```

- `IDX` file signature
- an unsigned 8-bit integer to represent the file version
- an unsigned 32-bit integer for boolean flags
- a signed 64-bit integer for time to live
  - **Units:** duration in nanoseconds

#### *Index Records*

Index records are fixed-width at 24-bytes long. Each record consists of:

```
8-byte int64  timestamp
8-byte uint64 index
8-byte offset int64

0        8        16       24
+--------+--------+--------+
|  time  |  index | offset |
+--------+--------+--------+
```

- a signed 64-bit integer for the timestamp
    - **Units:** nanoseconds since epoch began
- an unsigned 64-bit integer for the record index
- a signed 64-bit integer for the log file offset

