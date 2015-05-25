package common

// ## **Log Variables**
var (

    // LogFileSignature is the first 3 bytes of a log file - `LOG`
    LogFileSignature = []byte("LOG")

    // IndexFileSignature represents the first 3 bytes of an index file - `IDX`
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

    // - `MaximumIndexSlice` is the maximum number of index records to be read at
    // one time
    MaximumIndexSlice = 32000
)

// ## **Log State**

// State is used to maintain the current status of the log.
type State uint8

const UNOPENED State = 0

// `OPEN` signifies the log is currently open
const OPEN State = 1

// `CLOSED` represents a closed log file
const CLOSED State = 2
