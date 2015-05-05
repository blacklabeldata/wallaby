package wallaby

import (
    "testing"

    "github.com/stretchr/testify/assert"
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
    for i < 5000 {
        xbinary.LittleEndian.PutUint64(buffer, 0, uint64(i))

        // append record
        n, err := log.Write(buffer)
        assert.Nil(t, err)
        assert.Equal(t, n, 32)

        i++
    }

    cursor := log.Cursor()
    assert.NotNil(cursor)

    i = 0
    for record, err := cursor.Seek(0); err != nil; record, err = cursor.Next() {
        buf := record.Data()
        j, err := xbinary.LittleEndian.Uint64(buf, 0)
        assert.Equal(t, j, uint64(i))
        assert.Equal(t, j, record.Index())
        assert.Nil(t, err)
    }
}
