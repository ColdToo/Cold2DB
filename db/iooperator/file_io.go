package iooperator

import "os"

// FileIoOperator represents using standard file I/O.
type FileIoOperator struct {
	fd *os.File
}

func NewFileIoOperator(fName string, fsize int64) (IoOperator, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIoOperator{fd: file}, nil
}

func (fio *FileIoOperator) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIoOperator) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIoOperator) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIoOperator) Close() error {
	return fio.fd.Close()
}

func (fio *FileIoOperator) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
