package wallaby

import (
    "os"

    "github.com/swiftkick-io/xbinary"
)

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
    if n != VersionOneLogRecordHeaderSize {
        return nil, ErrReadLogRecord
    } else if err != nil {
        return nil, ErrReadLogRecord
    }

    // read size
    size, err := xbinary.LittleEndian.Int32(c.recordBuffer, 0)
    if err != nil {
        return nil, ErrReadLogRecord
    } else if int(size) > len(c.recordBuffer) {
        return nil, ErrInvalidRecordSize
    }

    // read record
    n, err = c.file.ReadAt(c.recordBuffer[VersionOneLogRecordHeaderSize:VersionOneLogRecordHeaderSize+size],
        indexRecord.Offset()+VersionOneLogRecordHeaderSize)
    if n != int(size) {
        return nil, ErrReadLogRecord
    }

    // success
    return UnmarshalBasicLogRecord(c.recordBuffer[:VersionOneLogRecordHeaderSize+size])
}
