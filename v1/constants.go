package v1

// ## **Log Constants**

const (

    // VersionOne is an integer denoting the first version
    VersionOne = 1

    // IndexHeaderSize is the size of the index file header.
    IndexHeaderSize = 16

    // IndexRecordSize is the size of the index records.
    IndexRecordSize = 24

    // LogHeaderSize is the header size of version 1 log files
    LogHeaderSize = 16

    // LogRecordHeaderSize is the size of the log record headers.
    LogRecordHeaderSize = 16

    // MaxRecordSize is the maximum size a record can be for version 1
    MaxRecordSize = 0xffffffff
)
