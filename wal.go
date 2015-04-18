package wallaby

import (
	"io"
	"time"
)

// WriteAheadLog implements an immutable write-ahead log file with indexes and snapshots.
type WriteAheadLog interface {
	io.Closer

	// Allows for log records to be read from the log at specific byte offsets.
	LogReader

	// Creates a new record and appends it to the log.
	LogAppender

	// Snapshot records the current position of the log file.
	Snapshot() (Snapshot, error)

	// Metadata returns meta data of the log file.
	Metadata() (Metadata, error)

	// Recover should be called when the log is opened to verify consistency of the log.
	Recover() error
}

// // LogStore contains all the records in the log.
// type LogStore interface {
// 	Header() (LogHeader, error)
// }

// LogAppender appends a new record to the end of the log.
type LogAppender interface {
	// Append wraps the given bytes array in a Record and returns the record index or an error
	Append(data []byte) (Record, error)
}

// LogReader reads a Record at a specific offset
type LogReader interface {
	ReadAt(offset int64, limit int64) (Record, error)
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
