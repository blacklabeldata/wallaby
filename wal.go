package wallaby

import "time"

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
