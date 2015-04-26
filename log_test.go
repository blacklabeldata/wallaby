package wallaby

import (
	"testing"
	"time"

	"github.com/eliquious/xbinary"
	"github.com/stretchr/testify/assert"
)

func TestBasicLogRecord(t *testing.T) {

	var index uint64
	buf := make([]byte, 64)
	var size uint32 = 64
	nanos := time.Now().UnixNano()
	flags := DefaultRecordFlags

	// create record
	record := BasicLogRecord{size, nanos, index, flags, buf}

	assert.Equal(t, index, record.Index(), "index should be 0")
	assert.Equal(t, size, record.Size(), "size should be 64")
	assert.Equal(t, nanos, record.Time())
	assert.Equal(t, flags, record.Flags())
	assert.Equal(t, buf, record.Data())
}

func TestBasicLogRecordFactory(t *testing.T) {

	var index uint64
	buf := make([]byte, 64)
	var size uint32 = 64
	nanos := time.Now().UnixNano()
	flags := DefaultRecordFlags

	// create record
	factory := BasicLogRecordFactory(DefaultMaxRecordSize)
	record, err := factory(nanos, index, flags, buf)
	assert.Nil(t, err)

	assert.Equal(t, index, record.Index(), "index should be 0")
	assert.Equal(t, size, record.Size(), "size should be 64")
	assert.Equal(t, nanos, record.Time())
	assert.Equal(t, flags, record.Flags())
	assert.Equal(t, buf, record.Data())
}

func TestBasicLogRecordFactoryMaxSize(t *testing.T) {

	var index uint64
	buf := make([]byte, 64)
	nanos := time.Now().UnixNano()
	flags := DefaultRecordFlags

	// create record
	factory := BasicLogRecordFactory(48)
	record, err := factory(nanos, index, flags, buf)
	assert.NotNil(t, err)
	assert.Nil(t, record)
}

func TestBasicLogRecordMarshal(t *testing.T) {

	var index uint64
	buf := make([]byte, 64)
	var size uint32 = 64
	nanos := time.Now().UnixNano()
	flags := DefaultRecordFlags

	// create record
	record := BasicLogRecord{size, nanos, index, flags, buf}
	bin, err := record.MarshalBinary()
	assert.Nil(t, err)

	// test index
	i, err := xbinary.LittleEndian.Uint64(bin, 16)
	assert.Nil(t, err)
	assert.Equal(t, record.Index(), i, "index should match ", index)

	// test size
	s, err := xbinary.LittleEndian.Uint32(bin, 0)
	assert.Nil(t, err)
	assert.Equal(t, record.Size(), s, "size should be 64")

	// test time
	n, err := xbinary.LittleEndian.Int64(bin, 8)
	assert.Nil(t, err)
	assert.Equal(t, record.Time(), n)

	// test flags
	f, err := xbinary.LittleEndian.Uint32(bin, 4)
	assert.Nil(t, err)
	assert.Equal(t, record.Flags(), f)
}

func TestBasicLogRecordUnmarshal(t *testing.T) {

	var index uint64
	buf := make([]byte, 64)
	var size uint32 = 64
	nanos := time.Now().UnixNano()
	flags := DefaultRecordFlags

	// create record
	record := BasicLogRecord{size, nanos, index, flags, buf}
	bin, err := record.MarshalBinary()
	assert.Nil(t, err)

	r2, err := UnmarshalBasicLogRecord(bin)
	assert.Nil(t, err)

	// test size
	assert.Equal(t, record.Size(), r2.Size(), "size should be 64")

	// test index
	assert.Equal(t, record.Index(), r2.Index(), "indexes should match")

	// test time
	assert.Equal(t, record.Time(), r2.Time())

	// test flags
	assert.Equal(t, record.Flags(), r2.Flags())
}

func TestBasicLogRecordUnmarshalFail(t *testing.T) {
	var buf []byte

	r2, err := UnmarshalBasicLogRecord(buf)
	assert.NotNil(t, err)
	assert.Equal(t, ErrInvalidRecordSize, err)

	buf = make([]byte, 64)
	xbinary.LittleEndian.PutUint32(buf, 0, 63)
	r2, err = UnmarshalBasicLogRecord(buf)
	assert.NotNil(t, err)
	assert.Nil(t, r2)
	assert.Equal(t, ErrInvalidRecordSize, err)
}

func TestOpenLog(t *testing.T) {

	log, err := Create("./tests/open.log", DefaultConfig)
	assert.Nil(t, err)
	assert.NotNil(t, log)

	state := log.State()
	assert.Equal(t, state, CLOSED)

	err = log.Open()
	assert.Nil(t, err)

	state = log.State()
	assert.Equal(t, state, OPEN)

	err = log.Open()
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrLogAlreadyOpen)

}
