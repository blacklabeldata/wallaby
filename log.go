// # Log Implementations
package wallaby

import (
	"encoding"
	"hash"
	"io"
	"os"
	"time"

	"github.com/swiftkick-io/xbinary"
)

// ## **Log State**

// State is used to maintain the current status of the log.
type State uint8

const UNOPENED State = 0

// `OPEN` signifies the log is currently open
const OPEN State = 1

// `CLOSED` represents a closed log file
const CLOSED State = 2

// <br/>
// ## **Log Interfaces**

// ### **Write Ahead Log**

// WriteAheadLog implements an immutable write-ahead log file with indexes and
// snapshots.
type WriteAheadLog interface {
	io.WriteCloser

	// ###### *State*

	// `State` returns the current state of the log file.
	State() State

	// ###### *Open*

	// `Open` opens the log for appending. Prior to this call the log state
	// should be CLOSED. Once this is called State() should return OPEN.
	Open() error

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

// func NewRecordWriter(maxSize int, startingIndex uint64, flags uint32, writer io.WriteCloser) io.WriteCloser {
// 	return v1LogRecordFactory{maxSize, 0, 0, make([]byte, 0, maxSize+24), writer}
// }

type v1LogRecordFactory struct {
	maxSize int
	index   uint64
	flags   uint32
	buffer  []byte
	parent  io.WriteCloser
}

func (b v1LogRecordFactory) Write(data []byte) (int, error) {
	if len(data) > b.maxSize {
		return 0, ErrRecordTooLarge
	}

	// write uint32 size
	xbinary.LittleEndian.PutUint32(data, 0, uint32(len(data)))

	// write uint32 flags
	xbinary.LittleEndian.PutUint32(data, 4, b.flags)

	// write int64 timestamp
	xbinary.LittleEndian.PutInt64(data, 8, time.Now().UnixNano())

	// write uint64 index
	xbinary.LittleEndian.PutUint64(data, 16, b.index)
	b.index++

	copy(b.buffer[24:len(data)+24], data[:])

	return b.parent.Write(b.buffer[:len(data)+24])
}

func (b v1LogRecordFactory) Close() error {
	return b.parent.Close()
}

// ### **BasicLogRecord**

// BasicLogRecord represents an element in the log. Each record has a timestamp, an index, boolean flags, a length and the data.
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

// ### **UnmarshalBasicLogRecord**

// UnmarshalBasicLogRecord is a utility method for converting a byte array to a LogRecord. If the record is too small to contain the record header, an `ErrInvalidRecordSize` error is returned. If the size outlined in the header does not equal the size of the given buffer and the header, an `ErrInvalidRecordSize` error is also returned.
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
	// lock        sync.Mutex
	fd            *os.File
	writeCloser   io.WriteCloser
	config        Config
	header        FileHeader
	index         LogIndex
	size          int64
	state         State
	hash          hash.Hash64
	indexBuffer   []byte
	ttl           int64
	lastWriteTime int64
}

// ###### *Open*

// `Open` simply switches the state to `OPEN`. If the log is already open, the
// error `ErrLogAlreadyOpen` is returned.
func (v *versionOneLogFile) Open() error {
	v.Use(func(writer io.WriteCloser) io.WriteCloser {
		return v1LogRecordFactory{v.config.MaxRecordSize, 0, 0, make([]byte, 0, v.config.MaxRecordSize+VersionOneLogRecordHeaderSize), writer}

	})

	if v.state == OPEN {
		return ErrLogAlreadyOpen
	}

	v.state = OPEN

	io.Copy(v.hash, v.fd)
	return nil
}

// ###### *State*

// `State` allows the log state to be queried by returning the current state of
// the log.
func (v *versionOneLogFile) State() State {
	return v.state
}

// ###### *Use*

// `Use` allows middleware to modify the log's behavior. `DecorativeWriteClosers`
// are the vehicles which make this possible. They wrap the internal writer
// with additional capabilities such as using different buffering strategies.
func (v *versionOneLogFile) Use(writers ...DecorativeWriteCloser) {

	// Only apply the middleware is the log is `CLOSED`. The log remains
	// in this state until `Open` is called.
	if v.state == UNOPENED {

		// Iterate over all the `DecorativeWriteClosers` replacing the internal
		// writer with the new one.
		for _, writer := range writers {
			v.writeCloser = writer(v.writeCloser)
		}
	}
}

// ###### *Append*

func (v *versionOneLogFile) Write(data []byte) (n int, err error) {

	// If the log is not `OPEN`, return error
	if v.state != OPEN {
		return 0, ErrLogClosed
	}

	// Update log checksum
	v.hash.Write(data)

	// Write data into file
	n, err = v.writeCloser.Write(data)
	if err != nil {
		return
	}
	v.size += int64(n)

	// Copy `timestamp` and `index` from log record into index buffer
	copy(v.indexBuffer[:16], data[8:])

	// Set lastWriteTime
	lastWriteTime, err := xbinary.LittleEndian.Int64(data, 0)
	if err != nil {
		return
	}
	v.lastWriteTime = lastWriteTime

	// Write file offset data into index buffer
	_, err = xbinary.LittleEndian.PutInt64(v.indexBuffer, 16, v.size)
	if err != nil {
		return
	}

	// Write ttl into index buffer
	_, err = xbinary.LittleEndian.PutInt64(v.indexBuffer, 24, v.ttl)
	if err != nil {
		return
	}

	// Write index record into log index
	v.index.Write(v.indexBuffer)

	return
}

// ###### *Cursor*

func (v *versionOneLogFile) Cursor() LogCursor {
	return nil
}

// ###### *Snapshot*

// Snapshot returns a Snapshot instance representing the current state of the log.
func (v *versionOneLogFile) Snapshot() (Snapshot, error) {
	return BasicSnapshot{v.lastWriteTime, v.size, v.hash.Sum64()}, nil
}

// ###### *Metadata*

// Metadata returns information about the log file.
func (v *versionOneLogFile) Metadata() (Metadata, error) {

	meta := Metadata{
		Size:             v.size,
		LastModifiedTime: v.lastWriteTime,
		FileName:         v.fd.Name(),
		IndexFileName:    v.fd.Name() + ".idx",
	}
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
	v.state = CLOSED

	// Finally, the record `io.WriteCloser` is closed. The `DecorativeWriteCloser`
	// is expected to flush all the remaining data. If there is an error during
	// close, the error bubbles up.
	return v.writeCloser.Close()
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
