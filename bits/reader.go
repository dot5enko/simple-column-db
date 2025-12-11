package bits

import (
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	"github.com/google/uuid"
)

var ErrEOF = errors.New("end of file")

type Reader struct {
	buf   []byte
	pos   int
	order binary.ByteOrder
}

func NewBinReader(buf []byte, order binary.ByteOrder) *Reader {
	return &Reader{buf: buf, order: order}
}

func (r *Reader) remaining(n int) bool {
	return r.pos+n <= len(r.buf)
}

func (r *Reader) ReadU8() (uint8, error) {
	if !r.remaining(1) {
		return 0, ErrEOF
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

func (r *Reader) ReadI8() (int8, error) {
	u, err := r.ReadU8()
	return int8(u), err
}

func (r *Reader) ReadU16() (uint16, error) {
	if !r.remaining(2) {
		return 0, ErrEOF
	}
	v := r.order.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadI16() (int16, error) {
	v, err := r.ReadU16()
	return int16(v), err
}

func (r *Reader) ReadUUID() (result uuid.UUID, err error) {
	err = r.ReadBytes(16, result[:])
	return result, err
}

func (r *Reader) ReadU32() (uint32, error) {
	if !r.remaining(4) {
		return 0, ErrEOF
	}
	v := r.order.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadI32() (int32, error) {
	v, err := r.ReadU32()
	return int32(v), err
}

func (r *Reader) ReadU64() (uint64, error) {
	if !r.remaining(8) {
		return 0, ErrEOF
	}
	v := r.order.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *Reader) MustReadU64() uint64 {
	u, er := r.ReadU64()
	if er != nil {
		panic(er)
	}
	return u
}

func (r *Reader) ReadI64() (int64, error) {
	v, err := r.ReadU64()
	return int64(v), err
}

func (r *Reader) MustReadI64() int64 {
	i, er := r.ReadI64()
	if er != nil {
		panic(er)
	}
	return i

}

func (r *Reader) ReadF32() (float32, error) {
	u, err := r.ReadU32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(u), nil
}

func (r *Reader) ReadF64() (float64, error) {
	u, err := r.ReadU64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(u), nil
}

func (r *Reader) MustReadF64() float64 {
	f, er := r.ReadF64()
	if er != nil {
		panic(er)
	}
	return f
}

func (r *Reader) ReadBytes(n int, out []byte) error {
	if !r.remaining(n) {
		return ErrEOF
	}
	copy(out, r.buf[r.pos:r.pos+n])
	r.pos += n
	return nil
}

func MapBytesToInt[T any](data []byte, count int) *T {
	if len(data) < count*8 {
		panic("not enough data")
	}

	// Convert slice header to *[count]uint64
	return (*T)(unsafe.Pointer(&data[0]))
}
