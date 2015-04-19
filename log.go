package wallaby

import (
	"errors"
	"io"

	"github.com/eliquious/xbinary"
)

var (
	ErrRecordTooLarge = errors.New("record is too large")

	ErrInvalidRecordSize = errors.New("invalid record size")
)

const (
	// DefaultRecordFlags represents the default boolean flags for each log record.
	DefaultRecordFlags uint32 = 0

	// DefaultMaxRecordSize is the default maximum size of a log record.
	DefaultMaxRecordSize = 0xffff
)

// WriteAheadLog implements an immutable write-ahead log file with indexes and snapshots.
type WriteAheadLog interface {
	io.Closer

	// Creates a new record and appends it to the log.
	LogAppender

	// Creates a new Cursor initialized at index 0
	Cursor() Cursor

	// Snapshot records the current position of the log file.
	Snapshot() (Snapshot, error)

	// Metadata returns meta data of the log file.
	Metadata() (Metadata, error)

	// Recover should be called when the log is opened to verify consistency 
	// of the log.
	Recover() error
}

// type wal struct {
// 	index *LogIndex
// 	writer *io.Writer
// }

// func (w *wal) Close() error {

// 	// close index file
// 	w.index.Close()
// }

// LogAppender appends a new record to the end of the log.
type LogAppender interface {

	// Append wraps the given bytes array in a Record and returns the record 
	// index or an error
	Append(data []byte) (LogRecord, error)
}

// Cursor allows for quite navigation through the log. All Cursor start at zero
//  and moves forward until EOF.
type Cursor interface {

	// Seek moves the Cursor to the given record index.
	Seek(offset uint64) (LogRecord, error)

	// Next moves the Cursor forward one record.
	Next() (LogRecord, error)

	// Close cursor and any associates file handles
	Close() error
}

// LogPipe copies the raw byte stream into the given io.Writer starting at a 
// record offset and reading up until the given limit.
type LogPipe interface {
	Pipe(offset, limit uint64, writer *io.Writer) error
}

// LogHeader is at the front of the log file and describes which version the 
// file was written with as well as any boolean flags associated with the file
type LogHeader interface {
	Version() uint8
	Flags() uint32
}

// LogRecord describes a single item in the log file. It consists of a time, an
// index id, a length and the data.
type LogRecord interface {

	// Returns LogRecord as a byte array
	encoding.BinaryMarshaler

	// Converts a byte array into a LogRecord
	encoding.BinaryUnmarshaler

	// Size returns the size of the record data
	Size() uint32

	// Flags returns any boolean flags associated
	Flags() uint32

	// Time returns the record timestamp
	Time() int64

	// Index returns the record offset in the log
	Index() uint64

	// Data returns record data
	Data() []byte
}

// VersionOneLogRecordFactory generates log records.
type VersionOneLogRecordFactory struct {
	MaxRecordSize int
}

// NewRecord creates a v1 log record. Each record is prefixed with a header of
// 20 bytes. It consists of an int64 timestamp (nanoseconds unixtime), the
// record index (uint64), boolean flags, a length and the data.
func (f *VersionOneLogRecordFactory) NewRecord(nanos int64, index uint64, flags uint32, data []byte) (LogRecord, error) {

	// checks if record is too large
	if len(data) > f.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}

	// return new log record
	return VersionOneLogRecord{uint32(len(data)), nanos, index, flags, data}, nil
}

// VersionOneLogRecord represents an element in the log. Each record has a
// timestamp, an index, boolean flags, a length and the data.
type VersionOneLogRecord struct {
	size  uint32
	nanos int64
	index uint64
	flags uint32
	data  []byte
}

// Time represents when the record was created.
func (r VersionOneLogRecord) Time() int64 {
	return r.nanos
}

// Index is where the record lives in the log.
func (r VersionOneLogRecord) Index() uint64 {
	return r.index
}

// Flags returns boolean fields associated with the record.
func (r VersionOneLogRecord) Flags() uint32 {
	return r.flags
}

// Size returns the length of the record's data.
func (r VersionOneLogRecord) Size() uint32 {
	return r.size
}

// Data returns the associated byte buffer.
func (r VersionOneLogRecord) Data() []byte {
	return r.data
}

// MarshalBinary encodes the entire record in a byte array.
func (r *VersionOneLogRecord) MarshalBinary() ([]byte, error) {

	// create header buffer
	buffer := make([]byte, 24+r.size)

	// write uint32 size
	xbinary.LittleEndian.PutUint32(buffer, 0, r.size)

	// write uint32 flags
	xbinary.LittleEndian.PutUint32(buffer, 4, r.flags)

	// write int64 timestamp
	xbinary.LittleEndian.PutInt64(buffer, 8, r.nanos)

	// write uint64 index
	xbinary.LittleEndian.PutUint64(buffer, 16, r.index)

	// write data
	buffer = append(buffer[:24], r.data...)

	return buffer, nil
}

func (r *VersionOneLogRecord) UnmarshalBinary(buffer []byte) error {
	if len(buffer) < 24 {
		return ErrInvalidRecordSize
	}

	// read and validate uint32 size
	size, _ := xbinary.LittleEndian.Uint32(buffer, 0)
	if size != uint32(len(buffer)-24) {
		return ErrInvalidRecordSize
	}
	r.size = size

	// read uint32 flags
	flags, _ := xbinary.LittleEndian.Uint32(buffer, 4)
	r.flags = flags

	// read int64 timestamp
	nanos, _ := xbinary.LittleEndian.Int64(buffer, 8)
	r.nanos = nanos

	// read uint64 index
	index, _ := xbinary.LittleEndian.Uint64(buffer, 16)
	r.index = index

	// read data
	r.data = buffer[24:]

	return nil
}
