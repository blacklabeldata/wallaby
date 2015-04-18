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
	ir := BasicIndexRecord{unix, index, offset}

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
	ih := BasicIndexHeader{version, flags}

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
	factory := VersionOneIndexFactory{indexfile}
	index, err := factory.GetOrCreateIndex(FlagsDefault)

	assert.NotNil(t, index, "Index file could not be created")
	assert.Nil(t, err, "CreateIndex produced an error")

	// stat file header size
	info, err := os.Stat(indexfile)
	assert.Nil(t, err, "os.Stat call resulted in error")
	if info.Size() != 8 {
		t.Errorf("Invalid header size")
	}

	// test header
	header, err := index.Header()
	assert.Equal(t, 1, header.Version())
	assert.Equal(t, uint32(FlagsDefault), header.Flags())

	// test Size
	size, err := index.Size()
	assert.Equal(t, 0, size)
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
	factory := VersionOneIndexFactory{indexfile}
	index, err := factory.GetOrCreateIndex(FlagsDefault)
	index.Close()

	// re-open file
	index, err = factory.GetOrCreateIndex(FlagsDefault)

	// test header
	header, err := index.Header()
	assert.Equal(t, 1, header.Version())
	assert.Equal(t, uint32(FlagsDefault), header.Flags())

	// test Size
	size, err := index.Size()
	assert.Equal(t, 0, size)
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
	factory := VersionOneIndexFactory{indexfile}
	index, err := factory.GetOrCreateIndex(FlagsDefault)

	// test index file
	assert.NotNil(t, index, "Index file could not be created")
	assert.Nil(t, err, "CreateIndex produced an error")

	unix := time.Now().UnixNano()
	i, offset := uint64(0), int64(24)
	record := BasicIndexRecord{unix, i, offset}
	n, err := index.Append(record)
	assert.Equal(t, VersionOneIndexRecordSize, n, "Invalid index record size")

	size, _ := index.Size()
	assert.Equal(t, 1, size)

}
