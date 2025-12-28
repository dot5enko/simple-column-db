package cache

import "time"

type TypedRingBuffer[T any] struct {
	buffers []T
	free    chan uint16

	stats Stats
}

func NewTypedRingBuffer[T any](n int) *TypedRingBuffer[T] {

	buffers := make([]T, n)

	free := make(chan uint16, n)
	for i := 0; i < n; i++ {
		free <- uint16(i)
	}

	return &TypedRingBuffer[T]{
		buffers: buffers,
		free:    free,
		stats: Stats{
			Size: n,
		},
	}
}

func (p *TypedRingBuffer[T]) Get() (*T, uint16) {

	time0 := time.Now()

	id := <-p.free

	waitTime := time.Since(time0)

	p.stats.Reads.Add(1)
	p.stats.WaitTime.Add(int64(waitTime))

	return &p.buffers[id], id
}

func (p *TypedRingBuffer[T]) Return(id uint16) {

	p.stats.Returns.Add(1)
	p.free <- id
}

func (p *TypedRingBuffer[T]) GetStats() *Stats { return &p.stats }
