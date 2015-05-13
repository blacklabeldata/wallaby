package wallaby

import (
<<<<<<< Updated upstream
=======
    "fmt"
>>>>>>> Stashed changes
    "os"

    "github.com/swiftkick-io/xbinary"
)

<<<<<<< Updated upstream
type versionOneLogCursor struct {
    index        LogIndex
    file         *os.File
    slice        *IndexSlice
    sliceOffset  int
    position     int64
    recordBuffer []byte
}

func (c *versionOneLogCursor) Seek(offset int64) (LogRecord, error) {
    slice, err := c.index.Slice(offset, MaximumIndexSlice)
    if err != nil {
        return nil, err
    }

    // save slice
    c.slice = &slice
    c.position = offset + 1
    c.sliceOffset = 1

    indexRecord, err := slice.Get(0)
    if err != nil {
        return nil, err
    }

    n, err := c.file.ReadAt(c.recordBuffer[:VersionOneLogRecordHeaderSize], indexRecord.Offset())
=======
// versionOneLogCursor implements the LogCursor interface.
type versionOneLogCursor struct {
    index        LogIndex
    file         *os.File
    slice        IndexSlice
    sliceOffset  int
    position     uint64
    recordBuffer []byte
}

// ### Seek

// Seek moves the cursor to a particular record in the log and returns that record. The current implementation is naive and performs several reads. This could be optimized.
func (c *versionOneLogCursor) Seek(offset uint64) (LogRecord, error) {
    err := c.allocateSlice(offset)
    if err != nil {
        return nil, err
    }
    return c.Next()
}

func (c *versionOneLogCursor) allocateSlice(offset uint64) error {

    // Create an index slice for the given offset. If there was a problem creating the index slice return the error.
    slice, err := c.index.Slice(offset, MaximumIndexSlice)
    if err != nil {
        return err
    }

    // Save slice in the cursor as well as the position and slice offset
    c.slice = slice
    c.sliceOffset = 0
    c.position = offset + 1
    return nil
}

func (c *versionOneLogCursor) Next() (LogRecord, error) {

    if c.sliceOffset > c.slice.Size() {
        err := c.allocateSlice(c.position)
        if err != nil {
            return nil, err
        }
    }

    // Read the index record. If the IndexRecord could not be read, return an error.
    indexRecord, err := c.slice.Get(c.sliceOffset)
    if err != nil {
        return nil, err
    }
    c.sliceOffset++
    c.position++

    // Read the record header into the record buffer.
    n, err := c.file.ReadAt(c.recordBuffer[:VersionOneLogRecordHeaderSize], indexRecord.Offset())

    // If the number of bytes read is not equal to the number of bytes in the record header, return error
    // If the file could not be read also return an error.
>>>>>>> Stashed changes
    if n != VersionOneLogRecordHeaderSize {
        return nil, ErrReadLogRecord
    } else if err != nil {
        return nil, ErrReadLogRecord
    }

<<<<<<< Updated upstream
    // read size
    size, err := xbinary.LittleEndian.Int32(c.recordBuffer, 0)
=======
    // Read record size
    size, err := xbinary.LittleEndian.Int32(c.recordBuffer, 0)
    fmt.Println("size: ", size)
>>>>>>> Stashed changes
    if err != nil {
        return nil, ErrReadLogRecord
    } else if int(size) > len(c.recordBuffer) {
        return nil, ErrInvalidRecordSize
    }

<<<<<<< Updated upstream
    // read record
=======
    // Read record data
>>>>>>> Stashed changes
    n, err = c.file.ReadAt(c.recordBuffer[VersionOneLogRecordHeaderSize:VersionOneLogRecordHeaderSize+size],
        indexRecord.Offset()+VersionOneLogRecordHeaderSize)
    if n != int(size) {
        return nil, ErrReadLogRecord
    }

    // success
    return UnmarshalBasicLogRecord(c.recordBuffer[:VersionOneLogRecordHeaderSize+size])
}
<<<<<<< Updated upstream
=======

func (c *versionOneLogCursor) Close() error {
    return c.file.Close()
}
>>>>>>> Stashed changes
