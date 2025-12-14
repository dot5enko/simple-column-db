package io

import (
	"errors"
	"os"
)

type FileReader struct {
	path   string
	file   *os.File
	opened bool
}

func NewFileReader(path string) (*FileReader, error) {

	_, err := os.Stat(path)

	freader := &FileReader{
		path: path,
	}

	return freader, err
}

func (f *FileReader) OpenForReadOnly(v bool) (topErr error) {

	var perm os.FileMode = 0644

	if v == true {
		f.file, topErr = os.OpenFile(f.path, os.O_RDONLY, perm)
	} else {
		f.file, topErr = os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	}

	if topErr == nil {
		f.opened = true
	}

	return topErr

}

func (f *FileReader) Close() error {
	if f.opened == false {
		return nil
	}

	return f.file.Close()
}

func (f *FileReader) ReadAt(out []byte, off, length int) (err error) {
	if f.opened == false {
		err = errors.New("file not opened")
		return err
	}

	var readBytes int
	readBytes, err = f.file.ReadAt(out, int64(off))

	if readBytes != length {
		err = errors.New("read bytes mismatch")
		return err
	}

	return nil
}

func (f *FileReader) WriteAt(in []byte, off, length int) (err error) {
	if f.opened == false {
		err = errors.New("file not opened")
		return err
	}

	var writtenBytes int
	writtenBytes, err = f.file.WriteAt(in, int64(off))
	if writtenBytes != len(in) {
		err = errors.New("written bytes mismatch")
		return err
	}

	return nil
}
