package common

import (
    "io"
    "os"

    "github.com/blacklabeldata/m3"
)

// WriteAheadLog implements an immutable write-ahead log file with indexes and
// snapshots.
type WriteAheadLog interface {
    io.WriteCloser

    // Recover should be called when the log is opened to verify consistency
    // of the log.
    Recover() error

    // ###### *Cursor*

    // Creates a new Cursor initialized at index 0
    Cursor() (LogCursor, error)

    // ###### *Pipe*

    // Pipe copies the raw byte stream into the given `io.Writer` starting at a
    // record offset and reading up until the given limit.
    // Pipe(offset, limit uint64, writer io.Writer) error

    // ###### *Snapshot*

    // Snapshot records the current position of the log file.
    Snapshot() (Snapshot, error)

    // ###### *Metadata*

    // Metadata returns metadata of the log file.
    Metadata() (Metadata, error)
}

// LogCursor allows for quite navigation through the log. All Cursor start at zero
//  and moves forward until EOF.
type LogCursor interface {

    // ###### *Seek*

    // Seek moves the Cursor to the given record index.
    Seek(offset uint64) (LogRecord, error)

    // ###### *Next*

    // Next moves the Cursor forward one record.
    Next() (LogRecord, error)

    // ###### *Close*

    // Close cursor and any associates file handles
    Close() error
}

// LogRecord describes a single item in the log file. It consists of a time, an
// index id, a length and the data.
type LogRecord interface {

    // Size returns the size of the record data
    Size() uint32

    // Flags returns any boolean flags associated
    Flags() uint32

    // Time returns the record timestamp
    Time() int64

    // IsExpired returns whether the record has expired based on the log's ttl
    IsExpired(now, ttl int64) bool

    // Data returns record data
    Data() []byte
}

// LogIndex maintains a list of all the records in the log file. IndexRecords
type LogIndex interface {
    io.WriteCloser

    Size() uint64
    Header() FileHeader
    // Slice(offset uint64, limit uint64) (IndexSlice, error)
}

// FileHeader describes which version the file was written with. Flags
// represents boolean flags.
type FileHeader interface {
    Version() uint8
    Flags() uint32
    Expiration() int64
}

// IndexSlice contains several buffered index records for fast access.
type IndexSlice interface {
    Get(index int) (IndexRecord, error)
    Size() int
}

// IndexRecord describes each item in an index file.
type IndexRecord interface {
    Time() int64
    Index() uint64
    Offset() int64
    IsExpired(now, ttl int64) bool
}

// IndexRecordEncoder writes an `IndexRecord` into a byte array.
type IndexRecordEncoder func(record IndexRecord) (int, error)

// IndexRecordDecoder reads an `IndexRecord` from a bute array.
type IndexRecordDecoder func() (IndexRecord, error)

// IndexFactory creates an index based on the filename, version and flags given
type IndexFactory func(filename string, version uint8, flags uint32) (LogIndex, error)

// LogRecordEncoder writes a new record into the given writer.
type LogRecordEncoder func(index uint64, flags uint32, timestamp int64, data []byte) (int, error)

// LogRecordDecoder reads a record from the given reader.
type LogRecordDecoder func() (LogRecord, error)

// Metadata simply contains descriptive information about the log
type Metadata struct {
    Size             int64
    LastModifiedTime int64
    FileName         string
    IndexFileName    string
}

// Config stores several log settings. This is used to describe how the log
// should be opened.
type Config struct {
    FileMode      os.FileMode
    MaxRecordSize int
    Flags         uint32
    Version       uint8
    Truncate      bool
    TimeToLive    int64
    Strategy      m3.WriteStrategy
}
