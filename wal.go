// # wallaby - Write Ahead Log
//
// This file contains all of the errors, constants and entry points for wallaby.
//
package wallaby

import (
    "bytes"
    "errors"
    "os"
    "sync"
    "time"

    xxhash "github.com/OneOfOne/xxhash/native"
    "github.com/eliquious/xbinary"
)

// ## **Possible Log Errors**

var (
    // - `ErrReadIndexHeader` occurs when the index header cannot be read
    ErrReadIndexHeader = errors.New("failed to read index header")

    // - `ErrWriteIndexHeader` occurs when the index header cannot be written
    ErrWriteIndexHeader = errors.New("failed to write index header")

    // - `ErrReadLogHeader` occurs when the log header cannot be read
    ErrReadLogHeader = errors.New("failed to read log header")

    // - `ErrWriteLogHeader` occurs when the log header cannot be written
    ErrWriteLogHeader = errors.New("failed to write log header")

    // - `ErrSliceOutOfBounds` occurs when index.Slice is called and the offset
    // is larger than the size of the index.
    ErrSliceOutOfBounds = errors.New("read offset out of bounds")

    // - `ErrReadIndex` occurs when index.Slice fails to read the records
    ErrReadIndex = errors.New("failed to read index records")

    // - `ErrConfigRequired` occurs when no log config is given when creating
    // or opening a log file.
    ErrConfigRequired = errors.New("log config required")

    // - `ErrInvalidFileVersion` occurs when the version in the file header
    // is unrecognized.
    ErrInvalidFileVersion = errors.New("invalid file version")

    // - `ErrInvalidFileSignature` occurs when the signature in the file header
    // is unrecognized.
    ErrInvalidFileSignature = errors.New("invalid file signature")

    // - `ErrWriteLogRecord` occurs when a record fails to be written to the log
    ErrWriteLogRecord = errors.New("failed to write record")

    // - `ErrLogAlreadyOpen` occurs when an open log tries to be opened again
    ErrLogAlreadyOpen = errors.New("log already open")

    // - `ErrLogClosed` occurs when `Append` is called after the log has been
    // closed.
    ErrLogClosed = errors.New("log has been closed")

    // `ErrRecordTooLarge` occurs when writing a record which exceed the max
    // record size for the log.
    ErrRecordTooLarge = errors.New("record is too large")

    // ErrExceedsBufferSize occurs when the BufferedWriter is not large enough
    // to contain all the data being written to it.
    ErrExceedsBufferSize = errors.New("buffer too large")

    // ErrInvalidRecordSize
    ErrInvalidRecordSize = errors.New("invalid record size")

    // ErrInvalidSnapshot occurs when a Snapshot cannot be decoded.
    ErrInvalidSnapshot = errors.New("invalid snapshot")

    // ## **Log Variables**

    // Log signature bytes - `LOG`
    LogFileSignature = []byte("LOG")

    // Index signature bytes - `IDX`
    IndexFileSignature = []byte("IDX")
)

// ## **Log Constants**

const (
    // - `DefaultIndexFlags` is the default boolean flags for an index file.
    DefaultIndexFlags = 0

    // - `DefaultRecordFlags` represents the default boolean flags for each log record.
    DefaultRecordFlags uint32 = 0

    // - `DefaultMaxRecordSize` is the default maximum size of a log record.
    DefaultMaxRecordSize = 0xffff

    // - `LogHeaderSize` is the size of the file header.
    LogHeaderSize = 8

    // - `VersionOne` is an integer denoting the first version
    VersionOne = 1

    // - `VersionOneIndexHeaderSize` is the size of the index file header.
    VersionOneIndexHeaderSize = 8

    // - `VersionOneIndexRecordSize` is the size of the index records.
    VersionOneIndexRecordSize = 32

    // - `VersionOneLogHeaderSize` is the header size of version 1 log files
    VersionOneLogHeaderSize = 8

    // - `VersionOneLogRecordHeaderSize` is the size of the log record headers.
    VersionOneLogRecordHeaderSize = 24

    // - `MaximumIndexSlice` is the maximum number of index records to be read at
    // one time
    MaximumIndexSlice = 32000
)

// ## **Metadata**
// Metadata simply contains descriptive information about the log
type Metadata struct {
    Size             int64
    LastModifiedTime int64
    FileName         string
    IndexFileName    string
}

// ## **Config**
// Config stores several log settings. This is used to describe how the log
// should be opened.
type Config struct {
    FileMode      os.FileMode
    MaxRecordSize int
    Flags         uint32
    Version       uint8
    Truncate      bool
}

// > `DefaultConfig` can be used for sensible default log configuration.
var DefaultConfig Config = Config{
    FileMode:      0600,
    MaxRecordSize: DefaultMaxRecordSize,
    Flags:         DefaultRecordFlags,
    Version:       VersionOne,
    Truncate:      false,
}

// ## **Create a log file**
// Create returns a `WriteAheadLog` implementation if no errors occur. If the
// given filename already exists, the log file will try to be opened. If the
// file format can be verified, the existing log will be returned. If the
// file does not exist, a new log will be created with the given config.
//
// If the file already exists and the file version is different than the given
// `config.Version`, the file will remain the version in which it was created.
// In other words the file will not be updated to the newer version.

// ###### Implementation
func Create(filename string, config Config) (WriteAheadLog, error) {

    // Determine if the given config is valid. If the given config is `nil`,
    // a `ErrConfigRequired` error will be returned.
    if &config == nil {
        return nil, ErrConfigRequired
    }

    // Open the file name, creating the file if it does not already exist. The
    // file is opened with the `APPEND` flag, which means all writes are
    // appended to the file. Additional file modes can be given with the config.
    file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, config.FileMode)

    // If there was an error opening the file, the open error is returned.
    if err != nil {
        return nil, err
    }

    // Truncate the log file if requested in the given config
    if config.Truncate {
        err = file.Truncate(0)
        if err != nil {
            file.Close()
            return nil, err
        }
    }

    // Get the file stat. The file size is gotten from this call. This helps
    // to determine if the file already has a header or not.
    stat, err := file.Stat()

    // Return an error if the `os.FileStat` could not be retrieved. The file
    // is closed before returning.
    if err != nil {
        file.Close()
        return nil, err
    }

    // If the file size suggests the header exists, open an existing file.
    // Otherwise create a new file based on the given config.
    if stat.Size() >= LogHeaderSize {
        return openExisting(file, filename, config)
    }
    return createNew(file, filename, config)
}

// ## **Utility functions**
// These functions assist in opening both new and existing log files.

// ### **Creates a v1 log file**
// This function is used when creating a v1 log file.

// ###### *Implementation*
func createVersionOne(file *os.File, filename string, config Config) (WriteAheadLog, error) {

    // Create the file header structure from the log flags and version.
    header := BasicFileHeader{flags: config.Flags, version: config.Version}

    // Create the index file. If the file cannot be created, close the file
    // and return the error.
    index, err := VersionOneIndexFactory(filename+".idx", config.Version, config.Flags)
    if err != nil {
        file.Close()
        return nil, err
    }

    // Create the record factory. All records will be created using this
    // factory.
    recordFactory := BasicLogRecordFactory(config.MaxRecordSize)

    // Stat the file to get the size. If unsuccessful, close the file and return
    // the error.
    stat, err := file.Stat()
    if err != nil {
        file.Close()
        return nil, err
    }

    // Create a lock for the log and wrap the file in an atomic writer. The
    // atomic writer syncs to disk after each write.
    var lock sync.Mutex
    atomicWriter := NewAtomicWriter(file)

    // Finally, create the log file and return.
    return &versionOneLogFile{lock, file, atomicWriter, &header, index,
        recordFactory, config.Flags, stat.Size(), CLOSED, xxhash.New64(), time.Now().UnixNano()}, nil
}

// ### **Creates a new log file**
// A new log file is created with a file header consisting of a `LOG` signature
// followed by an 8-bit version. The file header is then synced to disk and
// a new log is created.

// ###### Implentation
func createNew(file *os.File, filename string, config Config) (WriteAheadLog, error) {

    // Create a buffer for the 4-byte file header.
    // The first 3 bytes contain the signature, `LOG`, followed by an 8-bit
    // version.
    buf := make([]byte, 8)

    // Write the `LOG` file signature to the first 3 bytes of the file.
    xbinary.LittleEndian.PutString(buf, 0, string(LogFileSignature))

    // Set file version to the given `config.Version`.
    buf[3] = byte(config.Version)

    // Write the config flags into the buffer
    xbinary.LittleEndian.PutUint32(buf, 4, config.Flags)

    // Write the file header buffer to the file.
    _, err := file.Write(buf)

    // If the header could not be written, close the file and return a
    // `ErrWriteLogHeader` error along with a `nil` log.
    if err != nil {
        file.Close()
        return nil, ErrWriteLogHeader
    }

    // If writing the file header succeeded, sync the file header to disk.
    err = file.Sync()

    // If the sync command failed, return a `ErrWriteLogHeader` error and a
    // `nil` log.
    if err != nil {
        return nil, ErrWriteLogHeader
    }

    // Returns the proper log parser based on the given `config.Version`.
    return selectVersion(file, filename, config)
}

// ### **Opens an existing log file**
// Opens an existing file and returns a log based on the file header. If the
// file contains a version which is not understood, the error
// `ErrInvalidFileVersion` is returned along with a `nil` log.
//
// If the file header cannot be read, an error is also returned.

// ###### Implementation
func openExisting(file *os.File, filename string, config Config) (WriteAheadLog, error) {
    // Create a buffer for the 8-byte file header.
    // The first 3 bytes are the signature `LOG` followed by an 8-bit version
    // and the boolean flags. Then read the file header into the buffer.
    buf := make([]byte, 8)
    _, err := file.ReadAt(buf, 0)

    // If there was an error reading the file header, close the file and return
    // a nil log and the read error.
    if err != nil {
        file.Close()
        return nil, err
    }

    // If the header was read sucessfully, verify the file signature matches
    // the expected "LOG" signature. If the first 3 bytes do not match `LOG`,
    // return a `nil` log and a `ErrInvalidFileSignature`.
    if !bytes.Equal(buf[0:3], LogFileSignature) {
        return nil, ErrInvalidFileSignature
    }

    // Read the boolean flags from the file header and overwrite the config
    // flags with the ones from the file.
    flags, err := xbinary.LittleEndian.Uint32(buf, 4)
    if err != nil {
        return nil, err
    }
    config.Flags = flags

    // The config version is updated to reflect the actual version of the file.
    // Then return the proper log parser based on the file version.
    config.Version = uint8(buf[3])
    return selectVersion(file, filename, config)
}

// ### **Select log version**
// `selectVersion` is only here to make the code a bit `DRY`er. It simple
// returns the proper log file based on the given version.

// ###### Implementation
// Open the log file based on the current version of the file.
// If the version is unrecognized, a `nil` log is returned as well as an
// `ErrInvalidFileVersion` error.
func selectVersion(file *os.File, filename string, config Config) (WriteAheadLog, error) {
    switch config.Version {
    case VersionOne:
        return createVersionOne(file, filename, config)
    default:
        return nil, ErrInvalidFileVersion
    }
}
