package wallaby

import (
	"bufio"
	"os"
	"sync"
	"sync/atomic"

	"github.com/eliquious/xbinary"
)

// IndexFactory creates an index. The reason for this is solely for future growth... perhaps a bit premature. The main reason is for future versions of log files or different backing stores.
type IndexFactory interface {
	GetOrCreateIndex(flags uint32) (*LogIndex, error)
}

// LogIndex maintains a list of all the records in the log file. IndexRecords
type LogIndex interface {
	Size() (uint64, error)
	Header() (FileHeader, error)
	Append(record IndexRecord) (n int, err error)
	Slice(offset int64, limit int64) (IndexSlice, error)
	Flush() error
	Close() error
}

// FileHeader describes which version the file was written with. Flags
// represents boolean flags.
type FileHeader interface {
	Version() uint8
	Flags() uint32
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
}

// BasicIndexRecord implements the bare IndexRecord interface.
type BasicIndexRecord struct {
	nanos  int64
	index  uint64
	offset int64
}

// Time returns when the record was written to the data file.
func (i BasicIndexRecord) Time() int64 {
	return i.nanos
}

// Index is a record's numerical id.
func (i BasicIndexRecord) Index() uint64 {
	return i.index
}

// Offset is the distance the data record is from the start of the data file.
func (i BasicIndexRecord) Offset() int64 {
	return i.offset
}

// BasicFileHeader is the simplest implementation of the FileHeader interface.
type BasicFileHeader struct {
	version uint8
	flags   uint32
}

// Version returns the file version.
func (i BasicFileHeader) Version() uint8 {
	return i.version
}

// Flags returns the boolean flags for the file.
func (i BasicFileHeader) Flags() uint32 {
	return i.flags
}

// VersionOneIndexRecord implements the bare IndexRecord interface.
type VersionOneIndexRecord struct {
	buffer *[]byte
	offset int
}

// Time returns when the record was written to the data file.
func (i VersionOneIndexRecord) Time() int64 {
	nanos, err := xbinary.LittleEndian.Int64(*i.buffer, 0+i.offset)
	if err != nil {
		nanos = 0
	}

	return nanos
}

// Index is a record's numerical id.
func (i VersionOneIndexRecord) Index() uint64 {
	index, err := xbinary.LittleEndian.Uint64(*i.buffer, 8+i.offset)
	if err != nil {
		index = 0
	}

	return index
}

// Offset is the distance the data record is from the start of the data file.
func (i VersionOneIndexRecord) Offset() int64 {
	offset, err := xbinary.LittleEndian.Int64(*i.buffer, 16+i.offset)
	if err != nil {
		offset = 0
	}

	return offset
}

// VersionOneIndexFactory creates index files of v1.
type VersionOneIndexFactory struct {
	Filename string
}

// GetOrCreateIndex either creates a new file or reads from an existing index
// file.
//
// VersionOneLogHeader starts with a 3-byte string, "IDX", followed by an 8-bit
// version. After the version, a uint32 represents the boolean flags.
// The records start immediately following the bit flags.
func (f VersionOneIndexFactory) GetOrCreateIndex(flags uint32) (LogIndex, error) {

	// try to open index file, return error on fail
	file, err := os.OpenFile(f.Filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	// get file stat, close file and return on error
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// get header, close file and return on error
	buf := make([]byte, VersionOneIndexHeaderSize)
	var header BasicFileHeader
	var size uint64

	// if file already has header
	if stat.Size() >= VersionOneIndexHeaderSize {

		// read file header, close and return upon error
		n, err := file.ReadAt(buf, 0)
		if err != nil {
			file.Close()
			return nil, err

			// failed to read entire header
		} else if n != VersionOneIndexHeaderSize {
			file.Close()
			return nil, ErrReadIndexHeader
		}

		// read flags
		flags, err := xbinary.LittleEndian.Uint32(buf, 4)
		if err != nil {
			file.Close()
			return nil, err
		}

		// create header
		header = BasicFileHeader{flags: flags, version: buf[3]}

		size = (uint64(stat.Size()) - uint64(VersionOneIndexHeaderSize)) / uint64(VersionOneIndexRecordSize)
	} else {

		// write magic string
		xbinary.LittleEndian.PutString(buf, 0, "IDX")

		// write version
		buf[3] = byte(VersionOne)

		// write boolean flags
		xbinary.LittleEndian.PutUint32(buf, 4, flags)

		// write index header to file
		_, err := file.Write(buf)
		if err != nil {
			file.Close()
			return nil, err
		}

		// flush data to disk
		err = file.Sync()
		if err != nil {
			return nil, ErrWriteIndexHeader
		}

		// create index header
		header = BasicFileHeader{VersionOne, flags}

	}

	var lock sync.Mutex
	writer := bufio.NewWriterSize(file, 64*1024)
	idx := VersionOneIndexFile{
		fd:     file,
		writer: writer,
		header: header,
		mutex:  lock,
		extbuf: xbinary.LittleEndian,
		size:   &size,
		buffer: make([]byte, VersionOneIndexRecordSize)}
	return idx, nil
}

// VersionOneIndexFile implements the IndexFile interface and is created by VersionOneIndexFactory.
type VersionOneIndexFile struct {
	fd     *os.File
	writer *bufio.Writer
	header BasicFileHeader
	mutex  sync.Mutex
	extbuf xbinary.ExtendedBuffer
	size   *uint64
	buffer []byte
}

// Flush writes any buffered data onto permanant storage.
func (i VersionOneIndexFile) Flush() error {
	i.writer.Flush()

	// sync changes to disk
	err := i.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Close flushed the index with permanant storage and closes the index.
func (i VersionOneIndexFile) Close() error {
	err := i.Flush()
	if err != nil {
		return err
	}

	return i.fd.Close()
}

// Append adds an index record to the end of the index file. V1 index records have a time, an index and an offset in the data file.
func (i VersionOneIndexFile) Append(record IndexRecord) (n int, err error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// create buffer and byte count
	var written int

	// write time
	n, _ = i.extbuf.PutInt64(i.buffer, 0, record.Time())
	written += n

	// write index
	n, _ = i.extbuf.PutUint64(i.buffer, 8, record.Index())
	written += n

	// write byte offset in record file
	n, _ = i.extbuf.PutInt64(i.buffer, 16, record.Offset())
	written += n

	// check bytes written
	if written < VersionOneIndexRecordSize {
		return written, ErrWriteIndexHeader
	}

	// write index buffer to file
	n, err = i.writer.Write(i.buffer)
	if err != nil {
		return n, err
	}

	// increment index size
	i.incrementSize()

	// return num bytes and nil error
	return written, nil
}

// IncrementSize bumps the index size by one.
func (i *VersionOneIndexFile) incrementSize() {
	// *i.size++
	atomic.AddUint64(i.size, 1)
}

// Size is the number of elements in the index. Which should coorespond with the number of records in the data file.
func (i VersionOneIndexFile) Size() (uint64, error) {
	return atomic.LoadUint64(i.size), nil
}

// Header returns the file header which describes the index file.
func (i VersionOneIndexFile) Header() (FileHeader, error) {
	return i.header, nil
}

// Slice returns multiple index records starting at the given offset.
func (i VersionOneIndexFile) Slice(offset int64, limit int64) (IndexSlice, error) {
	// flush buffer on read
	i.Flush()

	// get size
	var size = atomic.LoadUint64(i.size)

	// offset too  or less than 0
	if offset > int64(size) || offset < 0 {
		return nil, ErrSliceOutOfBounds

		// invalid limit
	} else if limit < 1 {
		return nil, ErrSliceOutOfBounds
	}

	var buf []byte
	// requested slice too large
	if limit > MaximumIndexSlice {
		buf = make([]byte, VersionOneIndexRecordSize*MaximumIndexSlice)

		// request exceeds index size
	} else if uint64(offset+limit) > size {
		buf = make([]byte, VersionOneIndexRecordSize*(uint64(offset+limit)-(size)))

		// full request can be satisfied
	} else {
		buf = make([]byte, VersionOneIndexRecordSize*limit)
	}

	// read records into buffer
	read, err := i.fd.ReadAt(buf, int64(offset*VersionOneIndexRecordSize)+VersionOneIndexHeaderSize)
	if err != nil {
		return nil, ErrReadIndex
	}

	return VersionOneIndexSlice{buf, read / VersionOneIndexRecordSize}, nil
}

// VersionOneIndexSlice provides for retrieval of buffered index records
type VersionOneIndexSlice struct {
	buffer []byte
	size   int
}

// Size returns the number of records in this slice.
func (s VersionOneIndexSlice) Size() int {
	return s.size
}

// Get returns a particular index record. ErrSliceOutOfBounds is returned is the record index given is < 0 or greater than the size of the slice.
func (s VersionOneIndexSlice) Get(index int) (IndexRecord, error) {
	if index > s.size || index < 0 {
		return nil, ErrSliceOutOfBounds
	}

	return VersionOneIndexRecord{&s.buffer, index * VersionOneIndexRecordSize}, nil
}

func (s VersionOneIndexSlice) MarshalBinary() (data []byte, err error) {
	return s.buffer, nil
}
