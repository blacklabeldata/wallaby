package wallaby

import (
	"io"
	"time"
)

// WriteAheadLog implements an immutable write-ahead log file with indexes and snapshots.
type WriteAheadLog interface {
	io.Closer

	Store() (LogStore, error)
	Snapshot() (Snapshot, error)
	Index() (LogIndex, error)
	Metadata() (Metadata, error)
	Recover() error
}

// LogStore contains all the records in the log.
type LogStore interface {
	Header() (LogHeader, error)

	// Allows for log records to be read from the log at specific byte offsets.
	LogReader

	// Creates a new record and appends it to the log.
	LogAppender
}

// LogAppender appends a new record to the end of the log.
type LogAppender interface {
	// Append wraps the given bytes array in a Record and returns the record index or an error
	Append(data []byte) (uint64, error)
}

// LogReader reads a Record at a specific offset
type LogReader interface {
	ReadAt(offset int64) (Record, error)
}

// LogHeader is at the front of the log file and describes which version the file was written with as well as any boolean flags associated with the file
type LogHeader interface {
	Version() uint8
	Flags() uint32
}

// Record describes a single item in the log file. It consists of a time, an index id, a length and the data.
type Record interface {
	Time() int64
	Index() uint64
	HeaderSize() uint64
	Size() uint64
	Data() ([]byte, error)
}

// IndexFactory creates an index. The reason for this is solely for future growth... perhaps a bit premature. The main reason is for future versions of log files or different backing stores.
type IndexFactory interface {
	GetOrCreateIndex(flags uint32) (*LogIndex, error)
}

// LogIndex maintains a list of all the records in the log file. IndexRecords
type LogIndex interface {
	Size() (uint64, error)
	Header() (IndexHeader, error)
	Append(record IndexRecord) (n int, err error)
	Slice(offset int64, whence int64) ([]IndexRecord, error)
	Close() error
}

// IndexHeader describes which version the index file was written with. Flags represents boolean flags.
type IndexHeader interface {
	Version() uint8
	Flags() uint32
}

// IndexRecord describes each item in an index file.
type IndexRecord interface {
	Time() int64
	Index() uint64
	Offset() int64
}

// Snapshot captures a specific state of the log. It consists of the time the snapshot was taken, the number of items in the log, and a CRC64 of all the log entries.
type Snapshot interface {
	Time() time.Time
	Size() uint64
	Hash() uint64
}

//
type Metadata struct {
	MaxIndex         int64
	IndexSize        int64
	StorageSize      int64
	LastModifiedTime int64
	CRC64            uint64
}
