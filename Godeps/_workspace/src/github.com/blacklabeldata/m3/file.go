package m3

import (
    "io"
    "os"
)

// WriteStrategy modifies how writes are made to a file. This function type takes an `os.File` and returns a new `io.WriteCloser`
type WriteStrategy func(file *os.File) io.WriteCloser

// NewFileWriter creates a new Writer from an `os.File` handle and a WriteStrategy. When Close() is called on the Writer the file is also closed assuming all middleware forward the Close request.
func NewFileWriter(file *os.File, strategy WriteStrategy) Writer {
    return &writer{strategy(file)}
}

// NewFileReader creates a new Reader from an `os.File` handle. When Close() is called on the Writer the file is also closed assuming all middleware forward the Close request.
func NewFileReader(file *os.File) Reader {
    return &reader{file}
}

// NoSyncOnWrite is a non-op write strategy. It does not modify the writes to the file.
func NoSyncOnWrite(file *os.File) io.WriteCloser {
    return file
}

// SyncOnWrite syncs the file to permanent storage after every write. While this can be used to increase durability, performance suffers.
func SyncOnWrite(file *os.File) io.WriteCloser {
    return &syncOnWrite{file}
}

// syncOnWrite implements the `io.WriteCloser` interface and flushes file changes to disk on every write.
type syncOnWrite struct {
    file *os.File
}

// `Write` writes the data into the file and syncs the file to disk. If the
// write causes an error it is bubbled up. If the write succeeds, the data is
// then synced.
func (a syncOnWrite) Write(data []byte) (n int, err error) {
    n, err = a.file.Write(data)
    if err != nil {
        return 0, err
    }

    err = a.file.Sync()
    return
}

// `Close` syncs the file to disk and then closes the file.
func (a syncOnWrite) Close() error {
    err := a.file.Sync()
    if err != nil {
        return err
    }
    return a.file.Close()
}
