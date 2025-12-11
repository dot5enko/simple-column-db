package bits

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"unsafe"

	"github.com/google/uuid"
)

var (
	ErrEOF          = errors.New("end of file")
	ErrReadMismatch = errors.New("read size mismatch")
)

const MaxBinReaderBufferSize = 256

type BitsReader struct {
	readBuffer [MaxBinReaderBufferSize]byte

	buf   io.Reader
	order binary.ByteOrder
}

func NewReader(buf io.Reader, order binary.ByteOrder) *BitsReader {
	return &BitsReader{buf: buf, order: order}
}

func (r *BitsReader) readNextBytesIntoReadBuffer(size int) error {
	readBytes, err := r.buf.Read(r.readBuffer[:size])

	if err != nil {
		return err
	}

	if readBytes != size {
		return ErrReadMismatch
	}

	return nil
}

func (r *BitsReader) ReadU8() (uint8, error) {
	err := r.readNextBytesIntoReadBuffer(1)

	if err != nil {
		return 0, err
	}

	return r.readBuffer[0], err
}

func (r *BitsReader) ReadI8() (int8, error) {
	u, err := r.ReadU8()
	return int8(u), err
}

func (r *BitsReader) ReadU16() (uint16, error) {

	err := r.readNextBytesIntoReadBuffer(2)

	if err != nil {
		return 0, err
	}

	v := r.order.Uint16(r.readBuffer[:2])
	return v, err
}

func (r *BitsReader) MustReadU16() uint16 {
	u, er := r.ReadU16()
	if er != nil {
		panic(er)
	}
	return u
}

func (r *BitsReader) MustReadU8() uint8 {
	u, er := r.ReadU8()
	if er != nil {
		panic(er)
	}
	return u
}

func (r *BitsReader) ReadI16() (int16, error) {
	v, err := r.ReadU16()
	return int16(v), err
}

func (r *BitsReader) ReadUUID() (result uuid.UUID, err error) {
	err = r.ReadBytes(16, result[:])
	return result, err
}

func (r *BitsReader) ReadU32() (uint32, error) {
	readErr := r.readNextBytesIntoReadBuffer(4)
	if readErr != nil {
		return 0, readErr
	}
	v := r.order.Uint32(r.readBuffer[:4])
	return v, nil
}

func (r *BitsReader) ReadI32() (int32, error) {
	v, err := r.ReadU32()
	return int32(v), err
}

func (r *BitsReader) ReadU64() (uint64, error) {

	readErr := r.readNextBytesIntoReadBuffer(8)
	if readErr != nil {
		return 0, readErr
	}

	v := r.order.Uint64(r.readBuffer[:8])
	return v, nil
}

func (r *BitsReader) MustReadU64() uint64 {
	u, er := r.ReadU64()
	if er != nil {
		panic(er)
	}
	return u
}

func (r *BitsReader) ReadI64() (int64, error) {
	v, err := r.ReadU64()
	return int64(v), err
}

func (r *BitsReader) MustReadI64() int64 {
	i, er := r.ReadI64()
	if er != nil {
		panic(er)
	}
	return i

}

func (r *BitsReader) ReadF32() (float32, error) {
	u, err := r.ReadU32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(u), nil
}

func (r *BitsReader) ReadF64() (float64, error) {
	u, err := r.ReadU64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(u), nil
}

func (r *BitsReader) MustReadF64() float64 {
	f, er := r.ReadF64()
	if er != nil {
		panic(er)
	}
	return f
}

func (r *BitsReader) ReadBytes(n int, out []byte) error {

	readBytes, err := r.buf.Read(out[:n])

	if readBytes != n {
		return ErrReadMismatch
	}

	return err
}

func MapBytesToInt[T any](data []byte, count int) *T {
	if len(data) < count*8 {
		panic("not enough data")
	}

	// Convert slice header to *[count]uint64
	return (*T)(unsafe.Pointer(&data[0]))
}
