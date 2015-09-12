package m3

import (
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestNewFileReader(t *testing.T) {

    filename := "testfile.bin"
    fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
    assert.Nil(t, err)

    // delete test file
    defer os.Remove(filename)

    buffer := make([]byte, 1024)
    n, err := fd.Write(buffer)
    assert.Equal(t, n, len(buffer))
    assert.Nil(t, err)

    // seek to beginning of file
    fd.Seek(0, 0)

    // create reader
    reader := NewFileReader(fd)

    // test read
    n, err = reader.Read(buffer)
    assert.Equal(t, n, len(buffer))
    assert.Nil(t, err)

    // test close
    err = reader.Close()
    assert.Nil(t, err)
}

func TestNewFileWriter(t *testing.T) {
    filename := "testfile.bin"
    fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
    assert.Nil(t, err)

    // delete test file
    defer os.Remove(filename)

    buffer := make([]byte, 1024)
    writer := NewFileWriter(fd, NoSyncOnWrite)

    // test write
    n, err := writer.Write(buffer)
    assert.Equal(t, n, len(buffer))
    assert.Nil(t, err)

    // get file stat
    stat, err := fd.Stat()
    assert.Nil(t, err)
    assert.Equal(t, stat.Size(), int64(len(buffer)))

    // test close
    err = writer.Close()
    assert.Nil(t, err)
}

func TestNewFileWriterSync(t *testing.T) {

    filename := "testfile.bin"
    fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
    assert.Nil(t, err)

    // delete test file
    defer os.Remove(filename)

    buffer := make([]byte, 1024)
    writer := NewFileWriter(fd, SyncOnWrite)

    // test write
    n, err := writer.Write(buffer)
    assert.Equal(t, n, len(buffer))
    assert.Nil(t, err)

    // get file stat
    stat, err := fd.Stat()
    assert.Nil(t, err)
    assert.Equal(t, stat.Size(), int64(len(buffer)))

    // test close
    err = writer.Close()
    assert.Nil(t, err)
}

func TestNewFileWriterSyncFail(t *testing.T) {

    filename := "testfile.bin"
    fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
    assert.Nil(t, err)

    // delete test file
    defer os.Remove(filename)

    buffer := make([]byte, 1024)
    writer := NewFileWriter(fd, SyncOnWrite)
    writer.Close()

    // test write
    n, err := writer.Write(buffer)
    assert.Equal(t, n, 0)
    assert.NotNil(t, err)

    // test close
    err = writer.Close()
    assert.NotNil(t, err)
}
