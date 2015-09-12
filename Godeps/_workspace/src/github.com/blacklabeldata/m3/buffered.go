package m3

import (
    "bufio"
    "io"
)

// NewBufferedWriter creates a buffered writer with the given buffer
// size. A `WriterMiddleware` is returned which wraps the given
// `io.WriteCloser` in a `bufio.Writer`. On Close the buffer is flushed and the Close method is forwarded.
func NewBufferedWriter(size int) WriterMiddleware {
    return func(writeCloser io.WriteCloser) io.WriteCloser {
        return &bufferedWriter{bufio.NewWriterSize(writeCloser, size), writeCloser}
    }
}

type bufferedWriter struct {
    writer *bufio.Writer
    closer io.Closer
}

func (c *bufferedWriter) Write(data []byte) (int, error) {
    return c.writer.Write(data)
}

func (c *bufferedWriter) Close() error {
    c.writer.Flush()
    return c.closer.Close()
}

// NewBufferedReader creates a ReaderMiddleware based on bufio.Reader.
func NewBufferedReader(size int) ReaderMiddleware {
    return func(readCloser io.ReadCloser) io.ReadCloser {
        return &ReadCombiner{bufio.NewReaderSize(readCloser, size), readCloser}
    }
}
