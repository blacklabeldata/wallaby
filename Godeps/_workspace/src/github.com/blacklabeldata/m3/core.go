package m3

import "io"

// ReaderMiddleware wraps an `io.ReadCloser` with additional functionality and returns a new `io.ReadCloser`. These functions are meant to be stack on top of one another for more complex data patterns and abstractions when reading data.
type ReaderMiddleware func(io.ReadCloser) io.ReadCloser

// Reader implements `io.ReadCloser` and adds another method for stacking middleware.
type Reader interface {
    io.ReadCloser

    // Use provides middleware for the internal `io.ReadCloser`
    Use(...ReaderMiddleware)
}

// WriterMiddleware wraps an `io.WriteCloser` with additional functionality and returns a new `io.WriteCloser`. These functions are meant to stact on top of one another for more complex data patterns and abstractions when writing data.
type WriterMiddleware func(io.WriteCloser) io.WriteCloser

// Writer implements `io.WriteCloser` and adds another method for stacking middleware.
type Writer interface {
    io.WriteCloser

    // Use provides middleware for the internal `io.WriteCloser`
    Use(...WriterMiddleware)
}

// NewReader creates a Reader from an existing `io.ReadCloser`.
func NewReader(r io.ReadCloser) Reader {
    return &reader{r}
}

type reader struct {
    rc io.ReadCloser
}

func (r *reader) Read(data []byte) (n int, err error) {
    return r.rc.Read(data)
}

func (r *reader) Close() (err error) {
    return r.rc.Close()
}
func (r *reader) Use(readers ...ReaderMiddleware) {

    // Iterate over all the `ReaderMiddleware` replacing the internal
    // reader with the new one.
    for _, reader := range readers {
        r.rc = reader(r.rc)
    }
}

// NewWriter creates a Writer from an existing `io.WriteCloser`.
func NewWriter(w io.WriteCloser) Writer {
    return &writer{w}
}

type writer struct {
    wc io.WriteCloser
}

func (w *writer) Write(data []byte) (n int, err error) {
    return w.wc.Write(data)
}

func (w *writer) Close() (err error) {
    return w.wc.Close()
}

func (w *writer) Use(writers ...WriterMiddleware) {

    // Iterate over all the `WriterMiddleware` replacing the internal
    // reader with the new one.
    for _, writer := range writers {
        w.wc = writer(w.wc)
    }
}

type WriteCombiner struct {
    writer io.Writer
    closer io.Closer
}

func (c *WriteCombiner) Write(data []byte) (int, error) {
    return c.writer.Write(data)
}

func (c *WriteCombiner) Close() error {
    return c.closer.Close()
}

type ReadCombiner struct {
    reader io.Reader
    closer io.Closer
}

func (c *ReadCombiner) Read(data []byte) (int, error) {
    return c.reader.Read(data)
}

func (c *ReadCombiner) Close() error {
    return c.closer.Close()
}
