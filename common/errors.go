package common

import "errors"

// ## **Possible Log Errors**

var (
    // ErrReadFileHeader occurs when the file header cannot be read
    ErrReadFileHeader = errors.New("failed to read file header")

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

    // - `ErrReadLogRecord` occurs when a record fails to be read from the log
    ErrReadLogRecord = errors.New("failed to read record")

    // ErrReadIndexRecord occurs when a record fails to be read from the index
    ErrReadIndexRecord = errors.New("failed to read index record")

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

    // ErrInvalidTTL occurs when the `config.TimeToLive` is less than 0
    ErrInvalidTTL = errors.New("invalid ttl; must be >= 0")

    // ErrInvalidLogStrategy occurs when the `config.Strategy` is nil
    ErrInvalidLogStrategy = errors.New("invalid write strategy")

    // ErrRecordFactorySize
    ErrRecordFactorySize = errors.New("invalid record factory; max record size exceeded")
)
