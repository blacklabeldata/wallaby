// ## **Writers - Log Middleware**

// Writers contains a few different log middlemares which intercept log writes
// for different reasons such as buffering the writes.
//
// By default all writes into the log are atomic and are flushed to disk
// immediately. Having middleware allows the write strategy to be tweaked.
package wallaby

import (
    "bufio"
    "io"
    "os"
)

// <br/>
// ### *Middleware Interface*

// `DecorativeWriteCloser` allows for middleware around the internal file writer.
type DecorativeWriteCloser func(io.WriteCloser) io.WriteCloser

// ### *Built-in Middleware*

// - `DefaultBufferedWriter` is pre-configured middleware with a buffer size of 64KB
var DefaultBufferedWriter DecorativeWriteCloser = NewBufferedWriter(65536)

// ---
// ### **NewBufferedWriter**
// `NewBufferedWriter` creates a buffered writer with the given buffer
// size. A `DecorativeWriteCloser` is returned which wraps the log's
// internal `io.WriteCloser` in a `bufio.Writer`.
func NewBufferedWriter(size int) DecorativeWriteCloser {
    return func(writer io.WriteCloser) io.WriteCloser {
        return bufferedWriteCloser{bufio.NewWriterSize(writer, size), writer}
    }
}

// > The buffered middleware is implemented as a `bufferedWriteCloser`
// which requires the newly created `bufio.Writer` as well as the
// log's internal `io.WriteCloser`.
type bufferedWriteCloser struct {
    writer *bufio.Writer
    parent io.WriteCloser
}

// #### Write

// `Write` writes the data into the buffer.
func (b bufferedWriteCloser) Write(data []byte) (int, error) {
    return b.writer.Write(data)
}

// #### Close

// `Close` flushes the buffer and then closes the parent `io.WriteCloser.`
func (b bufferedWriteCloser) Close() error {
    err := b.writer.Flush()
    if err != nil {
        return err
    }
    return b.parent.Close()
}

// ---
// ### **NewAtomicWriter**
// `NewAtomicWriter` is built-in middleware which wrapps all file writes. Each
// write is flushed to disk and a close also closes the underlying file.
// > The atomic middleware is implemented as an `atomicWriteCloser`.
func NewAtomicWriter(file *os.File) io.WriteCloser {
    return atomicWriteCloser{file}
}

type atomicWriteCloser struct {
    file *os.File
}

// #### Write

// `Write` writes the data into the file and syncs the file to disk. If the
// write causes an error it is bubbled up. If the write succeeds, the data is
// then synced.
func (a atomicWriteCloser) Write(data []byte) (n int, err error) {
    n, err = a.file.Write(data)
    if err != nil {
        return 0, err
    }

    err = a.file.Sync()
    return
}

// #### Close

// `Close` syncs the file to disk and then closes the file.
func (a atomicWriteCloser) Close() error {
    err := a.file.Sync()
    if err != nil {
        return err
    }
    return a.file.Close()
}
