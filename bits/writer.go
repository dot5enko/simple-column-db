package bits

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type BitWriter struct {
	pos   int
	data  []byte
	size  int
	order binary.ByteOrder

	growingEnabled bool
}

func NewEncodeBuffer(buf []byte, order binary.ByteOrder) BitWriter {

	result := BitWriter{}

	result.data = buf
	result.pos = 0
	result.size = len(buf)
	result.order = order

	return result
}

func (this *BitWriter) EnableGrowing() {
	this.growingEnabled = true
}

func (this *BitWriter) Reset() {
	this.pos = 0
}

func (this BitWriter) Position() int {
	return this.pos
}

func (this *BitWriter) ReadByte() (n byte, err error) {

	n = this.data[this.pos]
	this.pos++

	return
}

func (this *BitWriter) grow(atLeast int) {

	newSize := this.size * 2
	if atLeast > newSize {
		newSize += atLeast
	}

	newBuf := make([]byte, newSize)

	copy(newBuf, this.data[:this.pos])
	this.data = newBuf
	this.size = newSize
}
func (this *BitWriter) tryGrow(n int) {
	if (this.pos + n) > this.size {
		if this.growingEnabled {
			this.grow(n)
		} else {
			panic(fmt.Sprintf("bit writer growing is disabled on pos : %d, try grow %d, from size : %d", this.pos, n, this.size))
		}
	}
}

func (this *BitWriter) Write(p []byte) (n int, err error) {

	oldl := len(p)
	this.tryGrow(oldl)

	n = copy(this.data[this.pos:], p)

	if oldl != n {
		return 0, errors.New("not enough space")
	}

	this.pos += n

	return
}

func (this *BitWriter) EmptyBytes(i int) {
	this.tryGrow(i)
	this.pos += i
}

func (this *BitWriter) Bytes() []byte {
	return this.data[:this.pos]
}

func (this *BitWriter) PutInt32(v int32) {
	this.tryGrow(4)
	this.order.PutUint32(this.data[this.pos:], uint32(v))
	this.pos += 4
}

func (this *BitWriter) PutUint64(v uint64) {
	this.tryGrow(8)
	this.order.PutUint64(this.data[this.pos:], v)
	this.pos += 8
}
func (this *BitWriter) PutInt64(v int64) {
	this.tryGrow(8)
	this.order.PutUint64(this.data[this.pos:], uint64(v))
	this.pos += 8
}

func (this *BitWriter) PutFloat32(v float32) {
	this.tryGrow(4)
	this.order.PutUint32(this.data[this.pos:], math.Float32bits(v))
	this.pos += 4
}

func (this *BitWriter) PutUint16(v uint16) {
	this.tryGrow(2)
	this.order.PutUint16(this.data[this.pos:], v)
	this.pos += 2
}

func (this *BitWriter) WriteByte(u uint8) {
	this.tryGrow(1)
	this.data[this.pos] = u
	this.pos++
}

func (this *BitWriter) PutFloat64(f float64) {
	this.tryGrow(8)
	this.order.PutUint64(this.data[this.pos:], math.Float64bits(f))
	this.pos += 8
}
