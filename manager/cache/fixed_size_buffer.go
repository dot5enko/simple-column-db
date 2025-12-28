package cache

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	Size int

	Reads   atomic.Int32
	Returns atomic.Int32

	WaitTime atomic.Int64
}
type FixedSizeBufferPool struct {
	buffers [][]byte
	free    chan uint16

	arena   []byte
	bufSize int

	stats Stats
}

func NewFixedSizeBufferPool(n int, bufSize int) *FixedSizeBufferPool {
	arena := make([]byte, n*bufSize)

	buffers := make([][]byte, n)
	for i := 0; i < n; i++ {
		start := i * bufSize
		end := start + bufSize
		buffers[i] = arena[start:end:end]
	}

	free := make(chan uint16, n)
	for i := 0; i < n; i++ {
		free <- uint16(i)
	}

	return &FixedSizeBufferPool{
		arena:   arena,
		buffers: buffers,
		free:    free,
		bufSize: bufSize,
		stats: Stats{
			Size: n,
		},
	}
}

// stats
func (p *FixedSizeBufferPool) GetStats() *Stats { return &p.stats }

func (p *FixedSizeBufferPool) Get() ([]byte, uint16) {

	time0 := time.Now()
	id := <-p.free

	waitTime := time.Since(time0)

	p.stats.Reads.Add(1)
	p.stats.WaitTime.Add(int64(waitTime))

	return p.buffers[id], id
}

func (p *FixedSizeBufferPool) Return(id uint16) {

	p.stats.Returns.Add(1)

	p.free <- id
}
