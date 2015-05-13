package wallaby

import (
<<<<<<< Updated upstream
    "testing"

    "github.com/stretchr/testify/assert"
=======
    "fmt"
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/swiftkick-io/xbinary"
>>>>>>> Stashed changes
)

func TestCursor(t *testing.T) {

    // create log file
    log, err := Create("./tests/cursor.log", DefaultTestConfig)
    assert.Nil(t, err)
    assert.NotNil(t, log)

    // open log
    err = log.Open()
    assert.Nil(t, err)

    // create buffer
    buffer := make([]byte, 8)

    var i int
<<<<<<< Updated upstream
    for i < 5000 {
=======
    for i < 5 {
>>>>>>> Stashed changes
        xbinary.LittleEndian.PutUint64(buffer, 0, uint64(i))

        // append record
        n, err := log.Write(buffer)
        assert.Nil(t, err)
        assert.Equal(t, n, 32)

        i++
    }

<<<<<<< Updated upstream
    cursor := log.Cursor()
    assert.NotNil(cursor)

    i = 0
    for record, err := cursor.Seek(0); err != nil; record, err = cursor.Next() {
        buf := record.Data()
=======
    cursor, err := log.Cursor()
    assert.Nil(t, err)
    assert.NotNil(t, cursor)

    i = 0
    var record LogRecord
    for record, err = cursor.Seek(0); err == nil && record.Index() <= 5; record, err = cursor.Next() {
        fmt.Println(i)
        assert.Nil(t, err)
        assert.NotNil(t, record)

        buf := record.Data()
        fmt.Println(i, " len: ", len(buf))
>>>>>>> Stashed changes
        j, err := xbinary.LittleEndian.Uint64(buf, 0)
        assert.Equal(t, j, uint64(i))
        assert.Equal(t, j, record.Index())
        assert.Nil(t, err)
<<<<<<< Updated upstream
    }
=======
        i++
    }
    fmt.Println(err)
    assert.Equal(t, i, 5)
}

func createTestLog(t *testing.T, filename string) {

    // create log file
    log, err := Create(filename, DefaultTestConfig)
    assert.Nil(t, err)
    assert.NotNil(t, log)

    // open log
    err = log.Open()
    assert.Nil(t, err)

    // create buffer
    buffer := make([]byte, 8)

    var i int
    for i < 5 {
        xbinary.LittleEndian.PutUint64(buffer, 0, uint64(i))

        // append record
        n, err := log.Write(buffer)
        assert.Nil(t, err)
        assert.Equal(t, n, 32)

        i++
    }
    log.Close()
}

func TestCursorAllocateSlice(t *testing.T) {
    filename := "./tests/cursor.log"
    createTestLog(t, filename)

    // open log file
    logfile, _ := os.Open(filename)

    // create index file
    index, _ := VersionOneIndexFactory(filename+".idx", 1, 0)

    // create cursor
    cursor := &versionOneLogCursor{index, logfile, nil, 0, 0, make([]byte, DefaultTestConfig.MaxRecordSize+24)}
    cursor.allocateSlice(0)

    assert.NotNil(t, cursor.slice)
    fmt.Println(cursor.slice.Size())
>>>>>>> Stashed changes
}
