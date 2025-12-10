package bits

import (
	"errors"
	"unsafe"
)

// BitOrder specifies bit direction
type BitOrder int

const (
	MSBFirst BitOrder = iota // Most Significant Bit first (default network order)
	LSBFirst                 // Least Significant Bit first
)

type BitReader struct {
	data     []byte
	pos      int // bit position in the data slice
	bitOrder BitOrder
}

// NewBitReader creates a new BitReader with specified bit order
func NewBitReader(data []byte, order BitOrder) *BitReader {
	return &BitReader{
		data:     data,
		pos:      0,
		bitOrder: order,
	}
}

// ReadBits reads n bits and returns them as uint64
func (r *BitReader) ReadBits(n int) (uint64, error) {
	if n <= 0 || n > 64 {
		return 0, errors.New("can only read 1 to 64 bits at a time")
	}

	var result uint64
	for i := 0; i < n; i++ {
		byteIndex := r.pos / 8
		if byteIndex >= len(r.data) {
			return 0, errors.New("out of bounds")
		}

		bitIndex := r.pos % 8
		var bit uint8
		if r.bitOrder == MSBFirst {
			bit = (r.data[byteIndex] >> (7 - bitIndex)) & 1
		} else {
			bit = (r.data[byteIndex] >> bitIndex) & 1
		}

		result = (result << 1) | uint64(bit)
		r.pos++
	}

	return result, nil
}

// ReadUint8 reads 8 bits as uint8
func (r *BitReader) ReadUint8() (uint8, error) {
	val, err := r.ReadBits(8)
	return uint8(val), err
}

// ReadUint16 reads 16 bits as uint16
func (r *BitReader) ReadUint16() (uint16, error) {
	val, err := r.ReadBits(16)
	return uint16(val), err
}

func MapBytesToInt[T any](data []byte, count int) *T {
	if len(data) < count*8 {
		panic("not enough data")
	}

	// Convert slice header to *[count]uint64
	return (*T)(unsafe.Pointer(&data[0]))
}
