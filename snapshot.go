package wallaby

import (
    "encoding"
    "time"

    "github.com/eliquious/xbinary"
)

// ### **Snapshot**

// Snapshot captures a specific state of the log. It consists of the time the snapshot was taken, the number of items in the log, and a XXH64 hash of all the log entries.
type Snapshot interface {
    Time() time.Time
    Size() uint64
    Hash() uint64
    encoding.BinaryMarshaler
}

// #### **BasicSnapshot**

// BasicSnapshot represents the simplest snapshot which fulfills the Snapshot interface. The timestamp is stored as nanoseconds since epoch. Both size and hash are stored as 64-bit integers.
type BasicSnapshot struct {
    nanos int64
    size  uint64
    hash  uint64
}

// ###### *Time*

// Time converts the nanoseconds since epoch into a `time.Time` instance.
func (b BasicSnapshot) Time() time.Time {
    return time.Unix(0, b.nanos)
}

// ###### *Size*

// Size returns the number of records in the log at the time the snapshot was taken.
func (b BasicSnapshot) Size() uint64 {
    return b.size
}

// ###### *Hash*

// Hash returns the XXH64 hash of the log file.
func (b BasicSnapshot) Hash() uint64 {
    return b.hash
}

// ###### *MarshalSnapshot*

// MarshalBinary converts the snapshot into a byte array.
// The byte array is formatted like so:
//
// ```
// 8-byte int64  time
// 8-byte uint64 size
// 8-byte uint64 hash
//
// 0        8        16       24
// +--------+--------+--------+
// |  time  |  size  |  hash  |
// +--------+--------+--------+
// ```
func (b BasicSnapshot) MarshalBinary() ([]byte, error) {
    buffer := make([]byte, 24)
    xbinary.LittleEndian.PutInt64(buffer, 0, b.nanos)
    xbinary.LittleEndian.PutUint64(buffer, 8, b.size)
    xbinary.LittleEndian.PutUint64(buffer, 16, b.hash)
    return buffer, nil
}

// #### **UnmarshalShapshot**

// UnmarshalSnapshot is a utility function which converts a byte array into a snapshot. If the byte array is too small, a `ErrInvalidSnapshot` is returned.
func UnmarshalShapshot(data []byte) (Snapshot, error) {
    if len(data) != 24 {
        return nil, ErrInvalidSnapshot
    }

    var snapshot BasicSnapshot
    nanos, _ := xbinary.LittleEndian.Int64(data, 0)
    snapshot.nanos = nanos

    size, _ := xbinary.LittleEndian.Uint64(data, 8)
    snapshot.size = size

    hash, _ := xbinary.LittleEndian.Uint64(data, 16)
    snapshot.hash = hash

    return snapshot, nil
}
