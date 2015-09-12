package m3

import (
    "crypto/rand"
    "hash/crc64"
    "io"
    "testing"

    "github.com/stretchr/testify/assert"
)

type CRC64WriterMiddleware struct {
    CRC64  uint64
    Closed bool
    table  *crc64.Table
    parent io.WriteCloser
}

func (c *CRC64WriterMiddleware) Write(data []byte) (int, error) {
    c.CRC64 = crc64.Update(c.CRC64, c.table, data)
    return c.parent.Write(data)
}

func (c *CRC64WriterMiddleware) Close() error {
    c.Closed = true
    return c.parent.Close()
}

type CountingWriteCloser struct {
    Count  int
    Closed bool
}

func (c *CountingWriteCloser) Write(data []byte) (int, error) {
    c.Count += len(data)
    return len(data), nil
}

func (c *CountingWriteCloser) Close() error {
    c.Closed = true
    return nil
}

func NewRandReader(table *crc64.Table) *RandReadCloser {
    return &RandReadCloser{false, 0, 0, table}
}

type RandReadCloser struct {
    Closed bool
    CRC64  uint64
    Count  int
    table  *crc64.Table
}

func (r *RandReadCloser) Read(data []byte) (n int, err error) {
    r.Count += len(data)
    n, err = rand.Read(data)

    r.CRC64 = crc64.Update(r.CRC64, r.table, data)
    return
}

func (r *RandReadCloser) Close() error {
    r.Closed = true
    return nil
}

func NewStatsReader(table *crc64.Table, r io.ReadCloser) *StatsReadCloser {
    return &StatsReadCloser{false, 0, 0, table, r}
}

type StatsReadCloser struct {
    Closed bool
    CRC64  uint64
    Count  int
    table  *crc64.Table
    parent io.ReadCloser
}

func (r *StatsReadCloser) Read(data []byte) (n int, err error) {
    r.Count += len(data)
    n, err = r.parent.Read(data)
    r.CRC64 = crc64.Update(r.CRC64, r.table, data)
    return
}

func (r *StatsReadCloser) Close() error {
    r.Closed = true
    return r.parent.Close()
}

func NewStatsWriter(table *crc64.Table, r io.WriteCloser) *StatsWriteCloser {
    return &StatsWriteCloser{false, 0, 0, table, r}
}

type StatsWriteCloser struct {
    Closed bool
    CRC64  uint64
    Count  int
    table  *crc64.Table
    parent io.WriteCloser
}

func (r *StatsWriteCloser) Write(data []byte) (n int, err error) {
    r.Count += len(data)
    r.CRC64 = crc64.Update(r.CRC64, r.table, data)
    return r.parent.Write(data)
}

func (r *StatsWriteCloser) Close() error {
    r.Closed = true
    return r.parent.Close()
}

func TestNewWriter(t *testing.T) {

    // create test data
    buffer := make([]byte, 1024)
    n, err := rand.Read(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))

    // test utils
    countingWriter := &CountingWriteCloser{}

    // create writer
    writer := NewWriter(countingWriter)

    // test write
    n, err = writer.Write(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))
    assert.Equal(t, n, countingWriter.Count)

    // test close
    err = writer.Close()
    assert.Nil(t, err)
    assert.True(t, countingWriter.Closed)
}

func TestWriterUse(t *testing.T) {

    // create test data
    buffer := make([]byte, 1024)
    n, err := rand.Read(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))

    // test utils
    crcTable := crc64.MakeTable(crc64.ISO)
    countingWriter := &CountingWriteCloser{}
    crcWriter := CRC64WriterMiddleware{0, false, crcTable, countingWriter}
    csum := crc64.Checksum(buffer, crcTable)

    // create writer
    writer := NewWriter(countingWriter)

    // add middleware
    writer.Use(func(w io.WriteCloser) io.WriteCloser {
        return &crcWriter
    })

    // test write
    n, err = writer.Write(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))
    assert.Equal(t, n, countingWriter.Count)

    // ensure crc matches
    assert.Equal(t, crcWriter.CRC64, csum)

    // test close
    err = writer.Close()
    assert.Nil(t, err)
    assert.True(t, crcWriter.Closed)
    assert.True(t, countingWriter.Closed)
}

func TestNewReader(t *testing.T) {

    // create test data
    buffer := make([]byte, 1024)

    // test utils
    table := crc64.MakeTable(crc64.ISO)
    randReader := NewRandReader(table)

    // create reader
    reader := NewReader(randReader)

    // test read
    n, err := reader.Read(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))
    assert.Equal(t, n, randReader.Count)

    // calc chesksum
    csum := crc64.Checksum(buffer, table)
    assert.Equal(t, randReader.CRC64, csum)

    // test close
    err = reader.Close()
    assert.Nil(t, err)
    assert.True(t, randReader.Closed)
}

func TestNewReaderUse(t *testing.T) {

    // create test data
    buffer := make([]byte, 1024)

    // test utils
    table := crc64.MakeTable(crc64.ISO)
    randReader := NewRandReader(table)
    var statsReader *StatsReadCloser

    // create reader
    reader := NewReader(randReader)
    reader.Use(func(r io.ReadCloser) io.ReadCloser {
        statsReader = NewStatsReader(table, r)
        return statsReader
    })

    // test read
    n, err := reader.Read(buffer)
    assert.Nil(t, err)
    assert.Equal(t, n, len(buffer))
    assert.Equal(t, n, randReader.Count)
    assert.Equal(t, n, statsReader.Count)

    // calc chesksum
    csum := crc64.Checksum(buffer, table)
    assert.Equal(t, randReader.CRC64, csum)
    assert.Equal(t, statsReader.CRC64, csum)

    // test close
    err = reader.Close()
    assert.Nil(t, err)
    assert.True(t, randReader.Closed)
    assert.True(t, statsReader.Closed)
}
