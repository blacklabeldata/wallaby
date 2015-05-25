package v1

import (
    "hash"
    "io"
    "os"
    "time"

    "github.com/OneOfOne/xxhash"
    "github.com/swiftkick-io/m3"
    "github.com/swiftkick-io/wallaby/common"
    "github.com/swiftkick-io/xbinary"
)

// DefaultConfig can be used for sensible default log configuration.
var DefaultConfig common.Config = common.Config{
    FileMode:      0600,
    MaxRecordSize: common.DefaultMaxRecordSize,
    Flags:         common.DefaultRecordFlags,
    Version:       VersionOne,
    Truncate:      false,
    TimeToLive:    0,
    Strategy:      m3.NoSyncOnWrite,
}

// NewLogRecordEncoder creates a new LogRecordFactory which validates records are smaller than the given maxSize.
func NewLogRecordEncoder(maxSize int, writer io.Writer) (common.LogRecordEncoder, error) {

    // v1 log records can't be larger than 4gb
    if maxSize > MaxRecordSize {
        return nil, common.ErrRecordFactorySize
    }

    // create buffer
    buffer := make([]byte, maxSize+LogRecordHeaderSize)
    return func(index uint64, flags uint32, timestamp int64, data []byte) (int, error) {

        // validate record size
        if len(data) > maxSize {
            return 0, common.ErrRecordTooLarge
        }

        // write uint32 size
        xbinary.LittleEndian.PutUint32(buffer, 0, uint32(len(data)))

        // write uint32 flags
        xbinary.LittleEndian.PutUint32(buffer, 4, flags)

        // write int64 timestamp
        xbinary.LittleEndian.PutInt64(buffer, 8, timestamp)

        copy(buffer[LogRecordHeaderSize:len(data)+LogRecordHeaderSize], data[:])

        return writer.Write(buffer[:len(data)+LogRecordHeaderSize])
    }, nil
}

func NewLogRecordDecoder(maxSize int, reader io.Reader) common.LogRecordDecoder {

    buffer := make([]byte, maxSize+LogRecordHeaderSize)
    return func() (common.LogRecord, error) {
        _, err := reader.Read(buffer[:16])
        if err != nil {
            return nil, common.ErrReadLogRecord
        }

        size, err := xbinary.LittleEndian.Uint32(buffer, 0)
        if err != nil {
            return nil, common.ErrReadLogRecord
        } else if size > uint32(maxSize) {
            return nil, common.ErrInvalidRecordSize
        }

        _, err = reader.Read(buffer[LogRecordHeaderSize : size+LogRecordHeaderSize])
        if err != nil {
            return nil, common.ErrReadLogRecord
        }

        return &RawLogRecord{buffer[:size+LogRecordHeaderSize]}, nil
    }
}

// RawLogRecord implements the bare LogRecord interface.
type RawLogRecord struct {
    buffer []byte
}

// Size returns the length of the record payload
func (i *RawLogRecord) Size() uint32 {
    size, err := xbinary.LittleEndian.Uint32(i.buffer, 0)
    if err != nil {
        size = 0
    }

    return size
}

// Flags returns the boolean flags for the record
func (i *RawLogRecord) Flags() uint32 {
    flags, err := xbinary.LittleEndian.Uint32(i.buffer, 4)
    if err != nil {
        flags = 0
    }

    return flags
}

// Time is the record nanoseconds from epoch
func (i *RawLogRecord) Time() int64 {
    nanos, err := xbinary.LittleEndian.Int64(i.buffer, 8)
    if err != nil {
        nanos = 0
    }

    return nanos
}

// Data is the record payload
func (i *RawLogRecord) Data() []byte {
    return i.buffer[LogRecordHeaderSize:]
}

// IsExpired is a helper function for the index record which returns `true` if the given time is beyond the expiration time. The expiration time is calculated as written time + TTL.
func (i *RawLogRecord) IsExpired(now, ttl int64) bool {
    if ttl <= 0 {
        return false
    }
    return now > i.Time()+ttl
}

// ### **Creates a v1 log file**
// This function is used when creating a v1 log file.

// ###### *Implementation*
func Create(file *os.File, filename string, config common.Config) (common.WriteAheadLog, error) {

    // try to open index file, return error on fail
    idxFile, err := os.OpenFile(filename+".idx", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
    if err != nil {
        return nil, err
    }

    // hash
    hash := xxhash.New64()
    io.Copy(hash, idxFile)
    idxFile.Seek(0, 0)

    index, err := VersionOneIndexFactory(idxFile, config.Version, config.Flags, config.TimeToLive)
    if err != nil {
        file.Close()
        return nil, err
    }

    // Stat the file to get the size. If unsuccessful, close the file and return the error.
    stat, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, err
    }

    // create log writer
    writer, err := m3.NewMemMapWriter(file, 512*1024, stat.Size())
    if err != nil {
        return nil, err
    }

    logRecordEncoder, err := NewLogRecordEncoder(64*1024, writer)
    if err != nil {
        return nil, err
    }

    return &wal{
        filename:           filename,
        logWriter:          writer,
        index:              index,
        logRecordEncoder:   logRecordEncoder,
        indexRecordEncoder: NewIndexRecordEncoder(index),
        hashWriter:         NewIndexRecordEncoder(hash),
        hash:               hash,
        lastWriteTime:      0,
        flags:              config.Flags,
        logSize:            stat.Size(),
    }, nil
}

type wal struct {
    filename           string
    logWriter          io.WriteCloser
    index              common.LogIndex
    logRecordEncoder   common.LogRecordEncoder
    indexRecordEncoder common.IndexRecordEncoder
    hashWriter         common.IndexRecordEncoder
    hash               hash.Hash64
    lastWriteTime      int64
    flags              uint32
    logSize            int64
}

func (w *wal) Write(data []byte) (int, error) {

    // record attrs
    now := time.Now().UnixNano()
    size := w.index.Size()

    // write log record
    n, err := w.logRecordEncoder(size, w.flags, now, data)
    if err != nil {
        return n, err
    }

    // write index record
    indexRecord := common.NewIndexRecord(now, w.logSize, size)
    _, err = w.indexRecordEncoder(indexRecord)
    if err != nil {
        return n, err
    }

    // add log size
    w.logSize += int64(n)
    w.lastWriteTime = now

    // Update log checksum
    w.hashWriter(indexRecord)

    // return
    return n, nil
}

func (w *wal) Close() error {

    // close log writer
    err := w.logWriter.Close()
    if err != nil {
        w.index.Close()
        return err
    }

    // close index
    return w.index.Close()
}

func (w *wal) Recover() error {
    return nil
}

func (w *wal) Cursor() (common.LogCursor, error) {
    return nil, nil
}

func (w *wal) Snapshot() (common.Snapshot, error) {
    return common.NewSnapshot(w.lastWriteTime, w.logSize, w.hash.Sum64()), nil
}

func (w *wal) Metadata() (common.Metadata, error) {
    meta := common.Metadata{
        Size:             w.logSize,
        LastModifiedTime: w.lastWriteTime,
        FileName:         w.filename,
        IndexFileName:    w.filename + ".idx",
    }
    return meta, nil
}
