package wallaby

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createTestDir(t *testing.T) string {
	dir := "./tests"
	err := os.MkdirAll(dir, os.ModeDir|0700)
	assert.Nil(t, err, "Test dir could not be created")
	return dir
}

func TestIndexRecord(t *testing.T) {
	now := time.Now()

	unix := now.UnixNano()
	index, offset := uint64(0), int64(24)
	ir := BasicIndexRecord{unix, index, offset, 0}

	// time
	assert.Equal(t, unix, ir.Time())

	// index
	assert.Equal(t, index, ir.Index())

	// offset
	assert.Equal(t, offset, ir.Offset())
}

func TestIndexHeader(t *testing.T) {

	var version uint8 = 128
	var flags uint32 = 0x377
	ih := BasicFileHeader{version, flags}

	// version
	assert.Equal(t, version, ih.Version())

	// flags
	assert.Equal(t, flags, ih.Flags())
}

func TestVersionOneCreateIndex(t *testing.T) {
	dir := createTestDir(t)

	// open index file
	indexfile := filepath.Join(dir, "test001.idx")

	// delete prior test file
	err := os.Remove(indexfile)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err)
	}

	// create index factory
	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)

	assert.NotNil(t, index, "Index file could not be created")
	assert.Nil(t, err, "CreateIndex produced an error")

	// stat file header size
	info, err := os.Stat(indexfile)
	assert.Nil(t, err, "os.Stat call resulted in error")
	assert.Equal(t, 8, info.Size(), "Invalid header size")

	// test header
	header := index.Header()
	assert.Equal(t, 1, int(header.Version()))
	assert.Equal(t, uint32(DefaultIndexFlags), header.Flags())

	// test Size
	size := index.Size()
	assert.Equal(t, 0, int(size))
}

func TestVersionOneCreateIndexExisting(t *testing.T) {
	dir := createTestDir(t)

	// open index file
	indexfile := filepath.Join(dir, "test002.idx")

	// delete prior test file
	err := os.Remove(indexfile)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err)
	}

	// create index factory
	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)
	index.Close()

	// test header
	header := index.Header()
	assert.Equal(t, 1, int(header.Version()))
	assert.Equal(t, uint32(DefaultIndexFlags), header.Flags())

	// test Size
	size := index.Size()
	assert.Equal(t, 0, int(size))
}

func TestVersionOneAppend(t *testing.T) {
	dir := createTestDir(t)

	// open index file
	indexfile := filepath.Join(dir, "test003.idx")

	// delete prior test file
	err := os.Remove(indexfile)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err)
	}

	// create index factory
	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)

	// test index file
	assert.NotNil(t, index, "Index file could not be created")
	assert.Nil(t, err, "CreateIndex produced an error")

	unix := time.Now().UnixNano()
	i, offset := uint64(0), int64(24)
	record := BasicIndexRecord{unix, i, offset, 0}
	n, err := index.Append(record)
	assert.Equal(t, VersionOneIndexRecordSize, n, "Invalid index record size")

	size := index.Size()
	assert.Equal(t, 1, int(size))

	for i := 2; i < 10; i++ {
		unix = time.Now().UnixNano()
		offset = int64(24)
		record := BasicIndexRecord{unix, uint64(i + 1), offset, 0}
		n, err := index.Append(record)
		assert.Equal(t, VersionOneIndexRecordSize, int(n), "Invalid index record size")
		assert.Nil(t, err)

		size := index.Size()
		assert.Equal(t, i, int(size))
	}
}

func TestVersionOneSlice(t *testing.T) {
	dir := createTestDir(t)

	// open index file
	indexfile := filepath.Join(dir, "test004.idx")

	// delete prior test file
	err := os.Remove(indexfile)
	if err != nil && !os.IsNotExist(err) {
		t.Error(err)
	}

	// create index factory
	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)

	// offset out of range
	slice, err := index.Slice(1, 1)
	assert.Nil(t, slice, "Slice should be nil for invalid offset")
	assert.NotNil(t, err, "Expected ErrSliceOufOfBounds")

	// offset out of range
	slice, err = index.Slice(0, 1)
	assert.Nil(t, slice, "Slice should be nil for invalid offset")
	assert.NotNil(t, err, "Expected ErrSliceOufOfBounds")

	// limit out of range
	slice, err = index.Slice(1, 0)
	assert.Nil(t, slice, "Slice should be nil for invalid limit")
	assert.NotNil(t, err, "Expected ErrSliceOufOfBounds")

	// append records
	for i := 0; i < 100; i++ {
		unix := time.Now().UnixNano()
		offset := int64(24*i + 8)
		record := BasicIndexRecord{unix, uint64(i), offset, 0}
		index.Append(record)
	}
	index.Flush()

	// read Slice
	slice, err = index.Slice(0, 5)
	assert.Equal(t, 5, slice.Size(), "Slice should contain 5 index records")

	var unix int64
	for i := 0; i < slice.Size(); i++ {
		record, err := slice.Get(i)
		assert.Nil(t, err, "Get should not produce an error")

		assert.Equal(t, int64(24*i+8), record.Offset(), "Record offset should equal 24")
		assert.Equal(t, uint64(i), record.Index(), "Invalid record index")
		assert.True(t, record.Time() > unix, "Each record's time should be greater than the last")
		unix = record.Time()

	}

	// close file
	index.Close()
}

func BenchmarkIndexAdd(b *testing.B) {

	dir := "./tests"
	os.MkdirAll(dir, os.ModeDir|0700)

	// open index file
	indexfile := filepath.Join(dir, "bench001.idx")

	// delete prior test file
	err := os.Remove(indexfile)
	if err != nil && !os.IsNotExist(err) {
		b.Error(err)
	}

	// create index factory
	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)

	// var unix, offset int64
	var record BasicIndexRecord
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.nanos = time.Now().UnixNano()
		record.index = uint64(i)
		index.Append(record)
	}

	// flush to disk and close file
	index.Close()

	// stat file header size
	// info, err := os.Stat(indexfile)
	// b.Logf("Filesize: %d", info.Size())

	// number of bytes per iteration
	b.SetBytes(VersionOneIndexRecordSize)
}

// func BenchmarkIndexSlice(b *testing.B) {
// 	dir := "./tests"
// 	os.MkdirAll(dir, os.ModeDir|0700)

// 	// open index file
// 	indexfile := filepath.Join(dir, "bench002.idx")

// 	// delete prior test file
// 	err := os.Remove(indexfile)
// 	if err != nil && !os.IsNotExist(err) {
// 		b.Error(err)
// 	}

// 	// create index factory
// 	index, err := VersionOneIndexFactory(indexfile, VersionOne, DefaultIndexFlags)

// 	// append records
// 	for i := 0; i < b.N; i++ {
// 		unix := time.Now().UnixNano()
// 		offset := int64(24*i + 8)
// 		record := BasicIndexRecord{unix, uint64(i), offset, 0}
// 		index.Append(record)
// 	}
// 	index.Flush()

// 	b.ResetTimer()

// 	// read slice
// 	var read int
// 	for read < b.N {
// 		index.Slice(int64(read), int64(MaximumIndexSlice))
// 		read += MaximumIndexSlice
// 		// read += slice.Size()
// 	}

// 	// for i := 0; i < slice.Size(); i++ {
// 	// 	slice.Get(i)
// 	// }

// 	// close file
// 	index.Close()

// 	// number of bytes per iteration
// 	b.SetBytes(VersionOneIndexRecordSize)
// }
