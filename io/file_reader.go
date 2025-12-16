package io

import (
	"errors"
	"os"
)

type FileReader struct {
	path   string
	file   *os.File
	opened bool

	exists bool
}

func NewFileReader(path string) *FileReader {

	_, err := os.Stat(path)

	freader := &FileReader{
		path:   path,
		exists: err == nil,
	}

	return freader
}

func (f *FileReader) Open(readOnly bool) (topErr error) {

	var perm os.FileMode = 0644

	if readOnly == true {
		f.file, topErr = os.OpenFile(f.path, os.O_RDONLY, perm)
	} else {
		f.file, topErr = os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, perm)
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

// fill zeroes to the file at offset with given size
func (f *FileReader) FillZeroes(offset, size int) (err error) {
	if f.opened == false {
		err = errors.New("file not opened")
		return err
	}

	var writtenBytes int
	zeroes := make([]byte, size)

	writtenBytes, err = f.file.WriteAt(zeroes, int64(offset))
	if writtenBytes != len(zeroes) {
		err = errors.New("written bytes mismatch")
		return err
	}

	return nil
}
