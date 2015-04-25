// # wallaby - Write Ahead Log

// Imports go here

package wallaby

import (
    "bytes"
    "errors"
    "io"
    "os"
    "sync"
    "time"

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

    // ## **Log Variables**
    // File signature bytes
    LogFileSignature = []byte("LOG")
)

// ## **Log Constants**

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

// ## **Open log file**
// Open returns a `WriteAheadLog` implementation if no errors occur. If the
// given filename already exists, the log file will try to be opened. If the
// file format can be verified, the existing log will be returned. If the
// file does not exist, a new log will be created with the given config.
//
// If the file already exists and the file version is different than the given
// `config.Version`, the file will remain the version in which is was created.
// In other words the file will not be updated to the newer version.

// ###### Implementation
func Open(filename string, config Config) (WriteAheadLog, error) {

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
    if stat.Size() >= HeaderOffset {
        return openExisting(file, filename, config)
    } else {
        return createNew(file, filename, config)
    }
}

// ## **Version One Log File**
//
// `versionOneLogFile` is obviously v1 of the WAL. It is the first class which
// implements the `WriteAheadLog` interface. However, it is not a public
// type. Wallaby strives to maintain a clean API and hence uses interfaces to
// abstract specific implementations.
type versionOneLogFile struct {
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

// `Open` simply switches the state to `OPEN`. If the log is already open, the
// error `ErrLogAlreadyOpen` is returned.
func (v *versionOneLogFile) Open() error {
    if v.state == OPEN {
        return ErrLogAlreadyOpen
    }

    v.state = OPEN
    return nil
}

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

// `State` allows the log state to be queried by returning the current state of
// the log.
func (v *versionOneLogFile) State() State {
    return v.state
}

// `Use` allows middleware to modify the log's behavior. `DecorativeWriteClosers`
// are the vehicle while makes this possible. They wrap the internal writer
// with additional capabilities such as using different buffering strategies.
func (v *versionOneLogFile) Use(writers ...DecorativeWriteCloser) {

    // Only apply the middleware is the log is `CLOSED`. The log remains
    // in this state until `Open` is called.
    if v.state == CLOSED {

        // Iterate over all the `DecorativeWriteClosers` replacing the internal
        // writer with the new one.
        for _, writer := range writers {
            v.writeCloser = writer(v.writeCloser)
        }
    }
}

func (v *versionOneLogFile) Append(data []byte) (LogRecord, error) {
    index := v.index.Size()

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
    v.index.Append(BasicIndexRecord{record.Time(), record.Index(), int64(v.size), 0})

    // return newly created record
    return record, nil
}

func (v *versionOneLogFile) Cursor() Cursor {
    return nil
}

func (v *versionOneLogFile) Snapshot() (Snapshot, error) {
    return nil, nil
}

func (v *versionOneLogFile) Metadata() (Metadata, error) {
    meta := Metadata{}
    return meta, nil
}

func (v *versionOneLogFile) Recover() error {
    return nil
}

// Close closes the underlying file.
func (v *versionOneLogFile) Close() error {
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

// ## **Utility functions**
//
// ### **Creates a version one log file**
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
    log := &versionOneLogFile{lock, file, file, &header, index, record_factory, config.Flags, int64(size), CLOSED}

    return log, nil
}

// ### **Creates a new log file**
// A new log file is created with a file header consisting of a `LOG` signature
// followed by an 8-bit version. The file header is then synced to disk and
// a new log is created.
// ###### Implentation
func createNew(file *os.File, filename string, config Config) (WriteAheadLog, error) {

    // Create a buffer for the 4-byte file header.
    // The first 3 bytes are the signature `LOG` followed by an 8-bit version.
    buf := make([]byte, 4)

    // Write the `LOG` file signature to the first 3 bytes of the file.
    xbinary.LittleEndian.PutString(buf, 0, "LOG")

    // Set file version to the given `config.Version`.
    buf[3] = byte(config.Version)

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

    // Create a buffer for the 4-byte file header.
    // The first 3 bytes are the signature `LOG` followed by an 8-bit version.
    buf := make([]byte, 4)

    // Read the file header into the buffer
    _, err := file.ReadAt(buf, 0)

    // If there was an error reading the file header, close the file and return
    // a nil log and the read error.
    if err != nil {
        file.Close()
        return nil, err
    }

    // If the header was read sucessfully, verify the file signature matches
    // the expected signature. If the first 3 bytes do not match `LOG`, return
    // a `nil` log and a `ErrInvalidFileSignature`.
    if !bytes.Equal(buf[0:3], LogFileSignature) {
        return nil, ErrInvalidFileSignature
    }

    // Returns the proper log parser based on the given version
    config.Version = uint8(buf[3])
    return selectVersion(file, filename, config)
}

// ### **Select log version**
// `selectVersion` is only here to make the code a bit `DRY`er. It simple
// returns the proper log file based on the given version.
// ###### Implementation
func selectVersion(file *os.File, filename string, config Config) (WriteAheadLog, error) {

    // Open the log file based on the current version of the file.
    // If the version is unrecognized, a `nil` log is returned as well as an
    // `ErrInvalidFileVersion` error.
    switch config.Version {
    case VersionOne:
        return createVersionOne(file, filename, config)
    default:
        return nil, ErrInvalidFileVersion
    }
}