package wallaby

import (
	"errors"
	"os"
	"sync"

	"github.com/eliquious/xbinary"
)

var (
	// ErrReadIndexHeader occurs when the index header cannot be read
	ErrReadIndexHeader = errors.New("failed to read index header")

	// ErrWriteIndexHeader occurs when the index header cannot be written
	ErrWriteIndexHeader = errors.New("failed to write index header")
)

const (
	// FlagsDefault is the default boolean flags for an index file
	FlagsDefault = 0

	// VersionOne is an integer denoting the first version
	VersionOne                = 1
	VersionOneIndexHeaderSize = 8
	VersionOneIndexRecordSize = 24
)

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

// BasicIndexHeader is the simplest implementation of the IndexHeader interface.
type BasicIndexHeader struct {
	version uint8
	flags   uint32
}

// Version returns the file version.
func (i BasicIndexHeader) Version() uint8 {
	return i.version
}

// Flags returns the boolean flags for the index file.
func (i BasicIndexHeader) Flags() uint32 {
	return i.flags
}

// VersionOneIndexFactory creates index files of v1.
type VersionOneIndexFactory struct {
	Filename string
}

// GetOrCreateIndex either creates a new file or reads from an existing index file.
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
	var header BasicIndexHeader
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
		header = BasicIndexHeader{flags: flags, version: buf[3]}

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

		// create index header
		header = BasicIndexHeader{VersionOne, flags}

	}

	var lock sync.Mutex
	idx := VersionOneIndexFile{file, header, lock, xbinary.LittleEndian, &size}
	return idx, nil
}

// VersionOneIndexFile implements the IndexFile interface and is created by VersionOneIndexFactory.
type VersionOneIndexFile struct {
	fd     *os.File
	header BasicIndexHeader
	mutex  sync.Mutex
	extbuf xbinary.ExtendedBuffer
	size   *uint64
}

func (i VersionOneIndexFile) Close() error {
	return i.fd.Close()
}

// Append adds an index record to the end of the index file. V1 index records have a time, an index and an offset in the data file.
func (i VersionOneIndexFile) Append(record IndexRecord) (n int, err error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	// create buffer and byte count
	var written int
	buf := make([]byte, VersionOneIndexRecordSize)

	// write time
	n, _ = i.extbuf.PutInt64(buf, 0, record.Time())
	written += n

	// write index
	n, _ = i.extbuf.PutUint64(buf, 8, record.Index())
	written += n

	// write byte offset in record file
	n, _ = i.extbuf.PutInt64(buf, 16, record.Offset())
	written += n

	// check bytes written
	if written < VersionOneIndexRecordSize {
		return written, ErrWriteIndexHeader
	}

	// write index buffer to file
	n, err = i.fd.Write(buf)
	if err != nil {
		return n, err
	}

	// sync changes to disk
	err = i.fd.Sync()
	if err != nil {
		return 0, err
	}

	// increment index size
	i.incrementSize()

	// return num bytes and nil error
	return written, nil
}

func (i *VersionOneIndexFile) incrementSize() {
	*i.size++
}

func (i *VersionOneIndexFile) getSize() uint64 {
	return *i.size
}

// Size is the number of elements in the index. Which should coorespond with the number of records in the data file.
func (i VersionOneIndexFile) Size() (uint64, error) {
	return i.getSize(), nil
}

// Header returns the file header which describes the index file.
func (i VersionOneIndexFile) Header() (IndexHeader, error) {
	return i.header, nil
}

// Slice returns multiple index records starting at the given offset.
func (i VersionOneIndexFile) Slice(offset int64, limit int64) ([]IndexRecord, error) {
	return nil, nil
}
