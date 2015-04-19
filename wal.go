package wallaby

import "time"
import "os"
import "errors"

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
    VersionOneIndexHeaderSize = 8
    VersionOneIndexRecordSize = 24

    // VersionOneLogHeaderSize is the header size of version 1 log files
    VersionOneLogHeaderSize = 8

    // MaximumIndexSlice is the maximum number of index records to be read at one time
    MaximumIndexSlice = 32000
)

// Snapshot captures a specific state of the log. It consists of the time the snapshot was taken, the number of items in the log, and a CRC64 of all the log entries.
type Snapshot interface {
	Time() time.Time
	Size() uint64
	Hash() uint64
}

//
type Metadata struct {
	MaxIndex         int64
	IndexSize        int64
	StorageSize      int64
	LastModifiedTime int64
	CRC64            uint64
}

// Config stores several log settings.
type Config struct {
    FileMode      os.FileMode
    MaxRecordSize int
    Flags         uint32
    Version       uint8
}

// DefaultConfig will be used if the given config is nil.
const DefaultConfig := Config{
    FileMode: 0600,
    MaxRecordSize: DefaultMaxRecordSize,
    Flags: DefaultRecordFlags,
    Version: VersionOne
}

func Open(filename string, config Config) (WriteAheadLog, error) {

    // return error if config is nil
    if config == nil {
        return nil, ErrConfigRequired
    }

    // try to open log file, return error on fail
    file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
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
    buf := make([]byte, VersionOneLogHeaderSize)

    // create header and log
    var header BasicFileHeader
    var log WriteAheadLog

    // if file already has header
    if stat.Size() >= VersionOneLogHeaderSize {

        // read file header, close and return upon error
        n, err := file.ReadAt(buf, 0)
        if err != nil {
            file.Close()
            return nil, err

            // failed to read entire header
        } else if n != VersionOneLogHeaderSize {
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

        // read version
        switch header.Version() {
        case VersionOne:

            // create version one log file
            log = &VersionOneLogFile{}
        default:
            return nil, ErrInvalidFileVersion
        }
    } else {

        // create new log file
        switch config.Version {
        case VersionOne:

            // write magic string
            xbinary.LittleEndian.PutString(buf, 0, "LOG")

            // write version
            buf[3] = byte(VersionOne)

            // write boolean flags
            xbinary.LittleEndian.PutUint32(buf, 4, config.Flags)

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

            // create index header
            header = BasicFileHeader{VersionOne, flags}

            log = &VersionOneLogFile{file, header}
        default:
            return nil, ErrInvalidFileVersion
        }
    }

    // return log file
    return log, nil
}


// VersionOneLogFile
type VersionOneLogFile struct {
    fd *os.File
    header *FileHeader
    index *LogIndex
    factory VersionOneLogRecordFactory
    flags uint32
}


func (v *VersionOneLogFile) Append(data []byte) (LogRecord, error) {
    index, err := index.Size()
    if err != nil {
        return nil, ErrWriteLogRecord
    }

    // create log record
    record, err := v.factory.NewRecord(time.Now().UnixNano(), index, v.flags, data)
    if err != nil {
        return nil, ErrWriteLogRecord
    }

    // v.fd.Write()
}

func (v *VersionOneLogFile) Cursor() Cursor {
    
}

func (v *VersionOneLogFile) Snapshot() (Snapshot, error) {
    
}

func (v *VersionOneLogFile) Metadata() (Metadata, error) {
    
}

func (v *VersionOneLogFile) Recover() error {
    
}

// Close closes the underlying file.
func (v *VersionOneLogFile) Close() error {

    // sync any last changes to disk
    err := v.fd.Sync()
    if err != nil {
        return err
    }

    // close file handle
    v.fd.Close()
}
