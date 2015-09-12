package m3

import (
    "errors"
    "os"
    "syscall"

    mmap "github.com/edsrzf/mmap-go"
)

var ErrUnsafeOperation = errors.New("unsafe operation")

func NewMemMapAppender(file *os.File, blockSize int, offset int64) (Writer, error) {
    stat, err := file.Stat()
    if err != nil {
        return nil, err
    }

    //
    if offset+int64(blockSize) > stat.Size() {
        file.Truncate(offset + int64(blockSize))
    }

    m := &mmapWriter{file, nil, offset, 0, blockSize, true}
    err = m.createMap()
    if err != nil {
        return nil, err
    }

    return &writer{m}, nil
}

type mmapWriter struct {
    file       *os.File
    m          mmap.MMap
    fileOffset int64
    position   int
    blockSize  int
    safe       bool
}

func (m *mmapWriter) createMap() error {
    m.file.Truncate(m.fileOffset + int64(m.blockSize))
    m2, err := mmap.MapRegion(m.file, m.blockSize, syscall.PROT_WRITE|syscall.MAP_PRIVATE, mmap.RDWR, m.fileOffset)
    if err != nil {
        return err
    }
    m.m = m2
    m.fileOffset += int64(m.blockSize)

    return nil
}

func (m *mmapWriter) advance() error {
    // unmap mmap
    err := m.m.Unmap()
    if err != nil {
        m.safe = false
        return err
    }

    // create new mmap buffer
    err = m.createMap()
    if err != nil {
        m.safe = false
        return err
    }

    // reset mmap position
    m.position = 0

    return nil
}

func (m *mmapWriter) remaining() int {
    return m.blockSize - m.position
}

func (m *mmapWriter) Write(data []byte) (n int, err error) {

    // check mmap safety
    if !m.safe {
        return 0, ErrUnsafeOperation
    }

    // check remaining mmap buffer
    if m.remaining() == 0 {
        err = m.advance()
        if err != nil {
            m.safe = false
            return 0, err
        }
    }

    // iterate until all data has been written
    var written int
    for written < len(data) {

        // if the remaining data fits in the current mmap buffer
        if len(data)-written < m.remaining() {

            // copy data into mmap buffer
            copy(m.m[m.position:], data[written:])

            // update mmap position
            m.position += len(data)
            written = len(data)

        } else {

            // copy partial data into mmap block
            copy(m.m[m.position:], data[written:written+m.remaining()])
            written += m.remaining()

            // advance mmap buffer
            err = m.advance()
            if err != nil {
                return written, err
            }
        }
    }

    return written, nil
}

func (m *mmapWriter) Close() error {
    if m.safe {
        m.safe = false

        // unmap mmap
        err := m.m.Unmap()
        if err != nil {
            return err
        }
    }

    // removes bytes from file which were never written to
    m.file.Truncate(m.fileOffset - int64(m.remaining()))
    return m.file.Close()
}
