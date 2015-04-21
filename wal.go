package wallaby

import (
    "io"
    "sync"
    "time"
)
import "os"
import "errors"
import "github.com/eliquious/xbinary"

var (
    // ErrReadIndexHeader occurs when the index header cannot be read
    ErrReadIndexHeader = errors.New("failed to read index header")

    // ErrWriteIndexHeader occurs when the index header cannot be written
    ErrWriteIndexHeader = errors.New("failed to write index header")

    // ErrReadLogHeader occurs when the log header cannot be read
    ErrReadLogHeader = errors.New("failed to read log header")

    // ErrWriteLogHeader occurs when the log header cannot be written
    ErrWriteLogHeader = errors.New("failed to write log header")

    // ErrSliceOutOfBounds occurs when index.Slice is called and the offset is larger than the size of the index.
    ErrSliceOutOfBounds = errors.New("read offset out of bounds")

    // ErrReadIndex occurs when index.Slice fails to read the records
    ErrReadIndex = errors.New("failed to read index records")

    // ErrConfigRequired occurs when no log config is given when creating or opening a log file.
    ErrConfigRequired = errors.New("log config required")

    // ErrInvalidFileVersion occurs when the version in the file header is unrecognized.
    ErrInvalidFileVersion = errors.New("invalid file version")

    // ErrWriteLogRecord occurs when a record fails to be written to the log
    ErrWriteLogRecord = errors.New("failed to write record")
)

const (
    // FlagsDefault is the default boolean flags for an index file
    DefaultIndexFlags = 0

    // VersionOne is an integer denoting the first version
    VersionOne                = 1
    VersionOneIndexHeaderSize = 4
    VersionOneIndexRecordSize = 24

    // VersionOneLogHeaderSize is the header size of version 1 log files
    VersionOneLogHeaderSize = 8

    // MaximumIndexSlice is the maximum number of index records to be read at one time
    MaximumIndexSlice = 32000

    // HeaderOffset is the minimum number of bytes in the file before version headers begin.
    HeaderOffset = 4
)

// Snapshot captures a specific state of the log. It consists of the time the snapshot was taken, the number of items in the log, and a CRC64 of all the log entries.
type Snapshot interface {
    Time() time.Time
    Size() uint64
    Hash() uint64
}

// Metadata simply contains descriptive information aboutt the log
type Metadata struct {
    Size             int64
    LastModifiedTime int64
    FileName         string
    IndexFileName    string
}

// Config stores several log settings.
type Config struct {
    FileMode      os.FileMode
    MaxRecordSize int
    Flags         uint32
    Version       uint8
}

// DefaultConfig will be used if the given config is nil.
var DefaultConfig Config = Config{
    FileMode:      0600,
    MaxRecordSize: DefaultMaxRecordSize,
    Flags:         DefaultRecordFlags,
    Version:       VersionOne,
}

// Creates
func createVersionOne(file *os.File, filename string, config Config) (WriteAheadLog, error) {

    // create boolean flags
    var flags uint32

    // read file size
    stat, err := file.Stat()
    if err != nil {
        return nil, err
    }
    size := stat.Size()

    // create header buf
    buf := make([]byte, VersionOneIndexHeaderSize)

    // determine if it's a new file or not
    // By this point, the file is gauranteed to have at least 4 bytes.
    if size > HeaderOffset {
        // read file header, close and return upon error
        _, err := file.ReadAt(buf, HeaderOffset)
        if err != nil {
            file.Close()
            return nil, err
        }

        // read flags
        f, err := xbinary.LittleEndian.Uint32(buf, VersionOneIndexHeaderSize)
        if err != nil {
            file.Close()
            return nil, err
        }
        flags = f
    } else {

        // write boolean flags into header buffer
        xbinary.LittleEndian.PutUint32(buf, 0, config.Flags)

        // write version header to file
        _, err := file.Write(buf)
        if err != nil {
            file.Close()
            return nil, err
        }

        // flush data to disk
        err = file.Sync()
        if err != nil {
            return nil, ErrWriteLogHeader
        }
    }

    // create header
    header := BasicFileHeader{flags: flags, version: VersionOne}

    // create version one log file
    factory := VersionOneIndexFactory{filename + ".idx"}
    record_factory := VersionOneLogRecordFactory{config.MaxRecordSize}
    index, err := factory.GetOrCreateIndex(DefaultIndexFlags)
    if err != nil {
        file.Close()
        return nil, err
    }

    var lock sync.Mutex
    log := &VersionOneLogFile{lock, file, file, &header, index, record_factory, config.Flags, int64(size), CLOSED}

    return log, nil
}

func New(filename string, config Config) (WriteAheadLog, error) {

    // return error if config is nil
    if &config == nil {
        return nil, ErrConfigRequired
    }

    // try to open log file, return error on fail
    file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, config.FileMode)
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
    buf := make([]byte, 4)

    // create header and log
    // var header BasicFileHeader
    var log WriteAheadLog

    // if file already has header
    if stat.Size() >= HeaderOffset {

        // read file header, close and return upon error
        _, err := file.ReadAt(buf, 0)
        if err != nil {
            file.Close()
            return nil, err
        }

        // read version
        switch buf[3] {
        case VersionOne:

            return createVersionOne(file, filename, config)

        default:
            return nil, ErrInvalidFileVersion
        }
    } else {

        // write magic string
        xbinary.LittleEndian.PutString(buf, 0, "LOG")

        // write version
        buf[3] = byte(VersionOne)

        // write index header to file
        _, err := file.Write(buf)
        if err != nil {
            file.Close()
            return nil, err
        }

        // flush data to disk
        err = file.Sync()
        if err != nil {
            return nil, ErrWriteLogHeader
        }

        // create new log file
        switch config.Version {
        case VersionOne:

            return createVersionOne(file, filename, config)

        default:
            return nil, ErrInvalidFileVersion
        }
    }

    // return log file
    return log, nil
}

// VersionOneLogFile
type VersionOneLogFile struct {
    lock        sync.Mutex
    fd          *os.File
    writeCloser io.WriteCloser
    header      FileHeader
    index       LogIndex
    factory     VersionOneLogRecordFactory
    flags       uint32
    size        int64
    state       State
}

func (v *VersionOneLogFile) Open() error {
    v.state = OPEN
    return nil
}

func (v *VersionOneLogFile) Pipe(offset, limit uint64, writer io.Writer) error {
    // create cursor
    cur := v.Cursor()

    // iterate over requested record range
    for record, err := cur.Seek(offset); err != nil && record.Index() < limit+offset; record, err = cur.Next() {

        // marshal record into a buffer
        data, e := record.MarshalBinary()
        if e != nil {
            return e
        }

        // write data
        _, e = writer.Write(data)
        if e != nil {
            return e
        }
    }

    return cur.Close()
}

func (v *VersionOneLogFile) State() State {
    return v.state
}

func (v *VersionOneLogFile) Use(writers ...DecorativeWriteCloser) {
    if v.state == CLOSED {
        for _, writer := range writers {
            v.writeCloser = writer(v.writeCloser)
        }
    }
}

func (v *VersionOneLogFile) Append(data []byte) (LogRecord, error) {
    index, err := v.index.Size()
    if err != nil {
        return nil, ErrWriteLogRecord
    }

    // create log record
    record, err := v.factory.NewRecord(time.Now().UnixNano(), index, v.flags, data)
    if err != nil {
        return nil, ErrWriteLogRecord
    }

    // binary record
    buffer, err := record.MarshalBinary()
    if err != nil {
        return nil, err
    }

    // v.fd.Write()
    _, err = v.fd.Write(buffer)
    if err != nil {
        return nil, err
    }

    v.size += int64(len(buffer))
    v.index.Append(BasicIndexRecord{record.Time(), record.Index(), int64(v.size)})

    // return newly created record
    return record, nil
}

func (v *VersionOneLogFile) Cursor() Cursor {
    return nil
}

func (v *VersionOneLogFile) Snapshot() (Snapshot, error) {
    return nil, nil
}

func (v *VersionOneLogFile) Metadata() (Metadata, error) {
    meta := Metadata{}
    return meta, nil
}

func (v *VersionOneLogFile) Recover() error {
    return nil
}

// Close closes the underlying file.
func (v *VersionOneLogFile) Close() error {
    // Set state as CLOSED
    v.state = CLOSED

    // sync any last changes to disk
    err := v.fd.Sync()
    if err != nil {
        return err
    }

    // close file handle
    return v.fd.Close()
}
