package cache

import (
	"testing"

	"github.com/dot5enko/simple-column-db/schema"
)

func touch(buf []byte) {
	for i := 0; i < len(buf); i += 64 {
		buf[i]++
	}
}

func BenchmarkSliceArena(b *testing.B) {
	p := NewFixedSizeBufferPool(128, schema.SlabHeaderFixedSize)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, idx := p.Get()
			touch(buf)
			p.Return(idx)
		}
	})
}

func BenchmarkSlice(b *testing.B) {
	p := NewBufferPool(128, schema.SlabHeaderFixedSize)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, idx := p.Get()
			touch(buf)
			p.Return(idx)
		}
	})
}

func BenchmarkSliceNoArena(b *testing.B) {
	p := NewBufferPoolNoArena(128, schema.SlabHeaderFixedSize)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, idx := p.Get()
			touch(buf)
			p.Return(idx)
		}
	})
}

func BenchmarkSliceFixed(b *testing.B) {
	p := NewFixedBufferPool()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, idx := p.Get()
			touch(buf)
			p.Return(idx)
		}
	})
}

// test impls

type BufferID uint16

type BufferPool struct {
	arena   []byte
	buffers [][]byte
	free    chan BufferID
	bufSize int
}

func NewBufferPoolNoArena(n int, bufSize int) *BufferPool {

	buffers := make([][]byte, n)
	for i := 0; i < n; i++ {
		buffers[i] = make([]byte, bufSize)
	}

	free := make(chan BufferID, n)
	for i := 0; i < n; i++ {
		free <- BufferID(i)
	}

	return &BufferPool{
		buffers: buffers,
		free:    free,
		bufSize: bufSize,
	}
}

func NewBufferPool(n int, bufSize int) *BufferPool {
	arena := make([]byte, n*bufSize)

	buffers := make([][]byte, n)
	for i := 0; i < n; i++ {
		start := i * bufSize
		end := start + bufSize
		buffers[i] = arena[start:end:end] // full slice expression
	}

	free := make(chan BufferID, n)
	for i := 0; i < n; i++ {
		free <- BufferID(i)
	}

	return &BufferPool{
		arena:   arena,
		buffers: buffers,
		free:    free,
		bufSize: bufSize,
	}
}

func (p *BufferPool) Get() ([]byte, BufferID) {
	id := <-p.free
	return p.buffers[id], id
}

func (p *BufferPool) Return(id BufferID) {
	p.free <- id
}

const fixedBufferPoolSize = 128
const fixedPoolSizeElement = schema.SlabHeaderFixedSize

type FixedBufferPool struct {
	buffers [fixedBufferPoolSize][fixedPoolSizeElement]byte
	free    chan BufferID
}

func NewFixedBufferPool() *FixedBufferPool {
	p := &FixedBufferPool{
		free: make(chan BufferID, fixedBufferPoolSize),
	}
	for i := 0; i < fixedBufferPoolSize; i++ {
		p.free <- BufferID(i)
	}
	return p
}

func (p *FixedBufferPool) Get() ([]byte, BufferID) {
	id := <-p.free // blocks if none available
	return p.buffers[id][:], id
}

func (p *FixedBufferPool) Return(id BufferID) {
	p.free <- id
}
