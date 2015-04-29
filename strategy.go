package wallaby

import (
    "io"
    "os"
)

type AtomicStrategy func(file *os.File) io.WriteCloser

func SyncOnWrite(file *os.File) io.WriteCloser {
    return &syncOnWrite{file}
}

type syncOnWrite struct {
    file *os.File
}

// #### Write

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

// #### Close

// `Close` syncs the file to disk and then closes the file.
func (a syncOnWrite) Close() error {
    err := a.file.Sync()
    if err != nil {
        return err
    }
    return a.file.Close()
}
