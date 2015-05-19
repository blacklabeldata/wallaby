package v1

import (
    "io"
    "os"

    "github.com/swiftkick-io/m3"
    "github.com/swiftkick-io/wallaby/common"
    "github.com/swiftkick-io/xbinary"
)

// NewIndexRecordEncoder writes an `IndexRecord` into a byte array.
func NewIndexRecordEncoder(writer io.Writer) common.IndexRecordEncoder {
    buffer := make([]byte, 24)
    return func(record common.IndexRecord) (int, error) {
        xbinary.LittleEndian.PutInt64(buffer, 0, record.Time())
        xbinary.LittleEndian.PutUint64(buffer, 8, record.Index())
        xbinary.LittleEndian.PutInt64(buffer, 16, record.Offset())
        return writer.Write(buffer)
    }
}

// NewIndexRecordDecoder creates an `IndexRecord` decoder which decodes byte
// arrays and returns the IndexRecord. An `ErrInvalidRecordSize` is returned if
// the record cannot be read.
func NewIndexRecordDecoder(reader io.Reader) common.IndexRecordDecoder {

    buffer := make([]byte, 24)
    return func() (common.IndexRecord, error) {
        n, err := reader.Read(buffer)
        if err != nil {
            return nil, err
        } else if n < 24 {
            return nil, common.ErrReadIndexRecord
        }
        return RawIndexRecord{buffer, 0}, nil
    }
}

// RawIndexRecord implements the bare IndexRecord interface.
type RawIndexRecord struct {
    buffer []byte
    offset int
}

// Time returns when the record was written to the data file.
func (i RawIndexRecord) Time() int64 {
    nanos, err := xbinary.LittleEndian.Int64(i.buffer, 0+i.offset)
    if err != nil {
        nanos = 0
    }

    return nanos
}

// Index is a record's numerical id.
func (i RawIndexRecord) Index() uint64 {
    index, err := xbinary.LittleEndian.Uint64(i.buffer, 8+i.offset)
    if err != nil {
        index = 0
    }

    return index
}

// Offset is the distance the data record is from the start of the data file.
func (i RawIndexRecord) Offset() int64 {
    offset, err := xbinary.LittleEndian.Int64(i.buffer, 16+i.offset)
    if err != nil {
        offset = 0
    }

    return offset
}

// IsExpired is a helper function for the index record which returns `true` if
// the given time is beyond the expiration time. The expiration time is
// calculated as written time + TTL.
func (i RawIndexRecord) IsExpired(now, ttl int64) bool {
    if ttl <= 0 {
        return false
    }
    return now > i.Time()+ttl
}

// VersionOneLogHeader starts with a 3-byte string, "IDX", followed by an 8-bit
// version. After the version, a uint32 represents the boolean flags.
// The records start immediately following the bit flags.
func VersionOneIndexFactory(file *os.File, version uint8, flags uint32, expiration int64) (common.LogIndex, error) {

    // get file stat, close file and return on error
    stat, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, err
    }

    // get header, on error close file and return
    var header common.FileHeader
    var size uint64

    // if file already has header
    if stat.Size() >= IndexHeaderSize {

        // read file header
        header, err = common.ReadFileHeader(file)
        if err != nil {
            file.Close()
            return nil, err
        }

        // determine where the index should start from
        if size >= IndexHeaderSize+IndexRecordSize {

            // seek to the last record and try to read the index
            file.Seek(stat.Size()-IndexRecordSize, 0)

            // if the record cannot be read start back at 0
            reader := NewIndexRecordDecoder(file)
            record, err := reader()
            if err != nil {
                size = 0
            } else {
                size = record.Index()
            }

            // seek to end of file
            file.Seek(stat.Size(), 0)

        } else {
            // if the file diesn't even contain a full record
            // truncate the file to put it back into a good state
            size = 0
            file.Truncate(IndexHeaderSize)

            // seek to end of file header
            file.Seek(IndexHeaderSize, 0)

        }
        // size = (uint64(stat.Size()) - uint64(IndexHeaderSize)) / uint64(IndexRecordSize)

    } else {

        // create index header
        header = common.NewFileHeader(VersionOne, flags, expiration)

        // write file header
        _, err := common.WriteFileHeader(common.IndexFileSignature, header, file)
        if err != nil {
            file.Close()
            return nil, common.ErrWriteIndexHeader
        }

        // flush data to disk
        err = file.Sync()
        if err != nil {
            return nil, common.ErrWriteIndexHeader
        }
    }

    // create writer with buffered middleware
    writer := m3.NewFileWriter(file, m3.NoSyncOnWrite)
    writer.Use(m3.NewBufferedWriter(IndexRecordSize * 8192))

    // writer := bufio.NewWriterSize(file, 64*1024)
    idx := VersionOneIndexFile{
        writer: writer,
        header: header,
        size:   size}
    return &idx, nil
}

// VersionOneIndexFile implements the IndexFile interface and is created by VersionOneIndexFactory.
type VersionOneIndexFile struct {
    writer m3.Writer
    header common.FileHeader
    size   uint64
}

// Close flushed the index with permanant storage and closes the index.
func (i *VersionOneIndexFile) Close() error {
    return i.writer.Close()
}

// Append adds an index record to the end of the index file. V1 index records have a time, an index and an offset in the data file.
func (i *VersionOneIndexFile) Write(record []byte) (n int, err error) {

    // write index buffer to file
    n, err = i.writer.Write(record)
    if err != nil {
        return n, err
    }

    // increment index size
    i.incrementSize()

    // return num bytes and nil error
    return
}

// IncrementSize bumps the index size by one.
func (i *VersionOneIndexFile) incrementSize() {
    i.size++
}

// Size is the number of elements in the index. Which should coorespond with the number of records in the data file.
func (i *VersionOneIndexFile) Size() uint64 {
    return i.size
}

// Header returns the file header which describes the index file.
func (i VersionOneIndexFile) Header() common.FileHeader {
    return i.header
}
