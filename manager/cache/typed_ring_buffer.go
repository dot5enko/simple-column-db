package cache

type TypedRingBuffer[T any] struct {
	buffers []T
	free    chan uint16
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
	}
}

func (p *TypedRingBuffer[T]) Get() (*T, uint16) {
	id := <-p.free
	return &p.buffers[id], id
}

func (p *TypedRingBuffer[T]) Return(id uint16) {
	p.free <- id
}
