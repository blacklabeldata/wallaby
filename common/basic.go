package common

import (
    "bytes"
    "io"

    "github.com/swiftkick-io/xbinary"
)

// NewFileHeader creates a new FileHeader instance
func NewFileHeader(version uint8, flags uint32, expiration int64) FileHeader {
    return &BasicFileHeader{version, flags, expiration}
}

// ReadFileHeader creates a FileHeader from an io.Reader. Presumably this reader would be a file.
func ReadFileHeader(reader io.Reader) (FileHeader, error) {

    // read file header
    buffer := make([]byte, 16)
    if n, err := reader.Read(buffer); err != nil || n < len(buffer) {
        return nil, ErrReadIndexHeader
    }

    // grab version from header
    version := buffer[3]

    // read flags
    flags, err := xbinary.LittleEndian.Uint32(buffer, 4)
    if err != nil {
        return nil, err
    }

    // read ttl
    ttl, err := xbinary.LittleEndian.Int64(buffer, 8)
    if err != nil {
        return nil, err
    }

    // create file header
    return NewFileHeader(version, flags, ttl), nil
}

func WriteFileHeader(sig []byte, header FileHeader, writer io.Writer) (int, error) {

    // check for invalid length
    if len(sig) != 3 {
        return 0, ErrInvalidFileSignature

    }

    // check for valid signature
    if !bytes.Equal(sig, LogFileSignature) && !bytes.Equal(sig, IndexFileSignature) {
        return 0, ErrInvalidFileSignature
    }

    // make buffer and copy sig
    buffer := make([]byte, 16)
    copy(buffer, sig)

    // add version
    buffer[3] = header.Version()

    // add boolean flags
    xbinary.LittleEndian.PutUint32(buffer, 4, header.Flags())

    // add ttl
    xbinary.LittleEndian.PutInt64(buffer, 8, header.Expiration())

    // write header
    return writer.Write(buffer)
}

// BasicFileHeader is the simplest implementation of the FileHeader interface.
type BasicFileHeader struct {
    version    uint8
    flags      uint32
    expiration int64
}

// Version returns the file version.
func (i BasicFileHeader) Version() uint8 {
    return i.version
}

// Flags returns the boolean flags for the file.
func (i BasicFileHeader) Flags() uint32 {
    return i.flags
}

// Expiration returns the duration at which log records expire
func (i BasicFileHeader) Expiration() int64 {
    return i.expiration
}

// BasicLogRecord represents an element in the log. Each record has a timestamp, an index, boolean flags, a length and the data.
type BasicLogRecord struct {
    size  uint32
    nanos int64
    flags uint32
    data  []byte
}

// Time represents when the record was created.
func (r BasicLogRecord) Time() int64 {
    return r.nanos
}

// Flags returns boolean fields associated with the record.
func (r BasicLogRecord) Flags() uint32 {
    return r.flags
}

// Size returns the length of the record's data.
func (r BasicLogRecord) Size() uint32 {
    return r.size
}

// IsExpired determines if the record is expired.
func (r BasicLogRecord) IsExpired(now, ttl int64) bool {
    if ttl <= 0 {
        return false
    }
    return now > r.nanos+ttl
}

// Data returns the associated byte buffer.
func (r BasicLogRecord) Data() []byte {
    return r.data
}

// NewIndexRecord creates a new index record.
func NewIndexRecord(nanos, offset int64, index uint64) IndexRecord {
    return &BasicIndexRecord{nanos, index, offset}
}

// BasicIndexRecord implements the bare IndexRecord interface.
type BasicIndexRecord struct {
    nanos  int64
    index  uint64
    offset int64
}

// Time returns when the record was written to the data file.
func (i BasicIndexRecord) Time() int64 {
    return i.nanos
}

// Index is a record's numerical id.
func (i BasicIndexRecord) Index() uint64 {
    return i.index
}

// Offset is the distance the data record is from the start of the data file.
func (i BasicIndexRecord) Offset() int64 {
    return i.offset
}

// IsExpired is a helper function for the index record which returns `true` if
// the current time is beyond the expiration time. The expiration time is
// calculated as written time + TTL.
func (i BasicIndexRecord) IsExpired(now, ttl int64) bool {
    if ttl == 0 {
        return false
    }
    return now > i.nanos+ttl
}
