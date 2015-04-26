// # Log Implementations
package wallaby

import (
	"encoding"
	"io"
	"os"
	"sync"
	"time"

	"github.com/eliquious/xbinary"
)

// ## **Log State**

// State is used to maintain the current status of the log.
type State uint8

// `CLOSED` represents a closed log file
const CLOSED State = 0

// `OPEN` signifies the log is currently open
const OPEN State = 1

// <br/>
// ## **Log Interfaces**

// ### **Write Ahead Log**

// WriteAheadLog implements an immutable write-ahead log file with indexes and
// snapshots.
type WriteAheadLog interface {

	// ###### *State*

	// `State` returns the current state of the log file.
	State() State

	// ###### *Open*

	// `Open` opens the log for appending. Prior to this call the log state
	// should be CLOSED. Once this is called State() should return OPEN.
	Open() error

	// ###### *Close*

	// `Close` allows the log to be closed. After this is called, `State`
	// should return `CLOSED` and future appends should fail.
	Close() error

	// ###### *Append*

	// `Append` wraps the given bytes array in a Record and returns the record
	// index or an error
	Append(data []byte) (LogRecord, error)

	// ###### *Recover*

	// Recover should be called when the log is opened to verify consistency
	// of the log.
	Recover() error

	// ###### *Cursor*

	// Creates a new Cursor initialized at index 0
	Cursor() LogCursor

	// ###### *Use*

	// Use provides middleware for the internal `io.WriteCloser`
	Use(...DecorativeWriteCloser)

	// ###### *Pipe*

	// Pipe copies the raw byte stream into the given `io.Writer` starting at a
	// record offset and reading up until the given limit.
	Pipe(offset, limit uint64, writer io.Writer) error

	// ###### *Snapshot*

	// Snapshot records the current position of the log file.
	Snapshot() (Snapshot, error)

	// ###### *Metadata*

	// Metadata returns metadata of the log file.
	Metadata() (Metadata, error)
}

// ### **Log Cursor**

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

// ### **Log Record**

// LogRecord describes a single item in the log file. It consists of a time, an
// index id, a length and the data.
type LogRecord interface {
	// ###### *BinaryMarshaler*

	// Converts a LogRecord as a byte array
	encoding.BinaryMarshaler

	// ###### *Size*

	// Size returns the size of the record data
	Size() uint32

	// ###### *Flags*

	// Flags returns any boolean flags associated
	Flags() uint32

	// ###### *Time*

	// Time returns the record timestamp
	Time() int64

	// ###### *Index*

	// Index returns the record offset in the log
	Index() uint64

	// ###### *Data*

	// Data returns record data
	Data() []byte
}

// ###### **LogRecordFactory**
// `LogRecordFactory` generates new `LogRecords` from the arguments.
type LogRecordFactory func(nanos int64, index uint64, flags uint32, data []byte) (LogRecord, error)

// ###### **BasicLogRecordFactory**

// `BasicLogRecordFactory` creates a v1 log record. Each record is prefixed
// with a header. If the record data exceeds the maximum record size, an
// `ErrRecordTooLarge` error is returned.
func BasicLogRecordFactory(max_size int) LogRecordFactory {
	return func(nanos int64, index uint64, flags uint32, data []byte) (LogRecord, error) {
		if len(data) > max_size {
			return nil, ErrRecordTooLarge
		}
		return BasicLogRecord{uint32(len(data)), nanos, index, flags, data}, nil
	}
}

// ### **BasicLogRecord**

// `BasicLogRecord` represents an element in the log. Each record has a
// timestamp, an index, boolean flags, a length and the data.
type BasicLogRecord struct {
	size  uint32
	nanos int64
	index uint64
	flags uint32
	data  []byte
}

// ###### *Time*

// Time represents when the record was created.
func (r BasicLogRecord) Time() int64 {
	return r.nanos
}

// ###### *Index*

// Index is where the record lives in the log.
func (r BasicLogRecord) Index() uint64 {
	return r.index
}

// ###### *Flags*

// Flags returns boolean fields associated with the record.
func (r BasicLogRecord) Flags() uint32 {
	return r.flags
}

// ###### *Size*

// Size returns the length of the record's data.
func (r BasicLogRecord) Size() uint32 {
	return r.size
}

// ###### *Data*

// Data returns the associated byte buffer.
func (r BasicLogRecord) Data() []byte {
	return r.data
}

// MarshalBinary encodes the entire record in a byte array.
func (r BasicLogRecord) MarshalBinary() ([]byte, error) {

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

func UnmarshalBasicLogRecord(buffer []byte) (*BasicLogRecord, error) {
	var r BasicLogRecord
	if len(buffer) < VersionOneLogRecordHeaderSize {
		return nil, ErrInvalidRecordSize
	}

	// read and validate uint32 size
	size, _ := xbinary.LittleEndian.Uint32(buffer, 0)
	if size != uint32(len(buffer)-24) {
		return nil, ErrInvalidRecordSize
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
	// r.data = buffer[24:]
	copy(r.data, buffer[24:])

	return &r, nil
}

// ## **Version One Log File**
//
// `versionOneLogFile` is obviously v1 of the WAL. It is the first class which
// implements the `WriteAheadLog` interface. However, it is not a public
// type. Wallaby strives to maintain a clean API and hence uses interfaces to
// abstract specific implementations.
type versionOneLogFile struct {
	lock        sync.Mutex
	fd          *os.File
	writeCloser io.WriteCloser
	header      FileHeader
	index       LogIndex
	factory     LogRecordFactory
	flags       uint32
	size        int64
	state       State
}

// ###### *Open*

// `Open` simply switches the state to `OPEN`. If the log is already open, the
// error `ErrLogAlreadyOpen` is returned.
func (v *versionOneLogFile) Open() error {
	if v.state == OPEN {
		return ErrLogAlreadyOpen
	}

	v.state = OPEN
	return nil
}

// ###### *Pipe*

// `Pipe` will read a `limited` number of the log records beginning at the
// `offset` given. All the these records will be serialized into a byte array
// and written to the given `io.Writer`. If there was an error either converting
// the record or writing to the given `io.Writer`, the encountered error will be
// returned.
func (v *versionOneLogFile) Pipe(offset, limit uint64, writer io.Writer) error {

	// Create a new record cursor, closing it when exiting the function.
	cur := v.Cursor()
	defer cur.Close()

	// Seek to the given `offset` and read the records until the number of
	// requested records are read or an error occurs.
	for record, err := cur.Seek(offset); err != nil && record.Index() < limit+offset; record, err = cur.Next() {

		// Marshal the record into a byte array returning the error if there
		// is one.
		data, e := record.MarshalBinary()
		if e != nil {
			return e
		}

		// Write the byte array to the given `writer` returning the error if
		// there is one.
		_, e = writer.Write(data)
		if e != nil {
			return e
		}
	}

	// Success. Return a `nil` error.
	return nil
}

// ###### *State*

// `State` allows the log state to be queried by returning the current state of
// the log.
func (v *versionOneLogFile) State() State {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.state
}

// ###### *Use*

// `Use` allows middleware to modify the log's behavior. `DecorativeWriteClosers`
// are the vehicles which make this possible. They wrap the internal writer
// with additional capabilities such as using different buffering strategies.
func (v *versionOneLogFile) Use(writers ...DecorativeWriteCloser) {
	v.lock.Lock()
	defer v.lock.Unlock()

	// Only apply the middleware is the log is `CLOSED`. The log remains
	// in this state until `Open` is called.
	if v.state == CLOSED {

		// Iterate over all the `DecorativeWriteClosers` replacing the internal
		// writer with the new one.
		for _, writer := range writers {
			v.writeCloser = writer(v.writeCloser)
		}
	}
}

// ###### *Append*

func (v *versionOneLogFile) Append(data []byte) (LogRecord, error) {

	v.lock.Lock()
	defer v.lock.Unlock()

	if v.state != OPEN {
		return nil, ErrLogClosed
	}

	index := v.index.Size()

	// create log record
	record, err := v.factory(time.Now().UnixNano(), index, v.flags, data)
	if err != nil {
		return nil, ErrWriteLogRecord
	}

	// binary record
	buffer, err := record.MarshalBinary()
	if err != nil {
		return nil, err
	}

	// _, err = v.fd.Write(buffer)
	v.writeCloser.Write(buffer)
	if err != nil {
		return nil, err
	}

	v.size += int64(len(buffer))
	v.index.Append(BasicIndexRecord{record.Time(), record.Index(), int64(v.size), 0})

	// return newly created record
	return record, nil
}

// ###### *Cursor*

func (v *versionOneLogFile) Cursor() LogCursor {
	return nil
}

// ###### *Snapshot*

func (v *versionOneLogFile) Snapshot() (Snapshot, error) {
	return nil, nil
}

// ###### *Metadata*

func (v *versionOneLogFile) Metadata() (Metadata, error) {
	meta := Metadata{}
	return meta, nil
}

// ###### *Recover*
func (v *versionOneLogFile) Recover() error {
	return nil
}

// ### Close

// `Close` changes the log state to `CLOSED`. The `io.WriterCloser` as well as
// the underlying file should not recieve any writes after this call has
// completed. `Append` will return an `ErrLogClosed` after this method is
// called.
// ##### Implementation
func (v *versionOneLogFile) Close() error {

	// The log is locked before closing the file. The log is then
	// unlocked on return. After the log has been locked the state is set to
	// `CLOSED`.
	v.lock.Lock()
	defer v.lock.Unlock()

	v.state = CLOSED

	// Finally, the record `io.WriteCloser` is closed. The `DecorativeWriteCloser`
	// is expected to flush all the remaining data. If there is an error during
	// close, the error bubbles up.
	return v.writeCloser.Close()
}
