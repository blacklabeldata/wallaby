package wallaby

import (
	"encoding"
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

// State is used to maintain the current status of the log.
type State uint8

// CLOSED represents a closed log file
const CLOSED State = 0

// OPEN signifies the log is currently open
const OPEN State = 1

// WriteAheadLog implements an immutable write-ahead log file with indexes and
// snapshots.
type WriteAheadLog interface {

	// Closer allows the log to be closed
	io.Closer

	// Creates a new record and appends it to the log.
	Appender

	// Recoverable allows the log to be recovered upon crash
	Recoverable

	// Cursable allows cursors to be created.
	Cursable

	// Stateful allows the state of the log to be retrieved.
	Stateful

	// Opener opens the log for appending. Prior to this call the log state
	// should be CLOSED. Once this is called State() should return OPEN.
	Opener

	// Use provides middleware for the internal io.Writer
	Use(...DecorativeWriteCloser)

	// Piper allows log records to be transferred to another writer.
	Piper

	// Snapshot records the current position of the log file.
	Snapshot() (Snapshot, error)

	// Metadata returns meta data of the log file.
	Metadata() (Metadata, error)
}

// DecorativeWriteCloser allows for middleware around the internal file writer.
type DecorativeWriteCloser func(io.WriteCloser) io.WriteCloser

// Opener is the interface for Open. Open makes the log available for writing.
type Opener interface {
	Open() error
}

// Stateful is the interface for the State method. State returns the current
// state of the log file.
type Stateful interface {
	State() State
}

// Recoverable is the interface which wraps the Recover method. Recover should
// return the log to a stable state.
type Recoverable interface {
	// Recover should be called when the log is opened to verify consistency
	// of the log.
	Recover() error
}

type Cursable interface {
	// Creates a new Cursor initialized at index 0
	Cursor() Cursor
}

// Appender appends a new record to the end of the log.
type Appender interface {

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

// Piper copies the raw byte stream into the given io.Writer starting at a
// record offset and reading up until the given limit.
type Piper interface {
	Pipe(offset, limit uint64, writer io.Writer) error
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
func (r VersionOneLogRecord) MarshalBinary() ([]byte, error) {

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

func (r VersionOneLogRecord) UnmarshalBinary(buffer []byte) error {
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
