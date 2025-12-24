package cache

type FixedSizeBufferPool struct {
	buffers [][]byte
	free    chan uint16

	arena   []byte
	bufSize int
}

func NewFixedSizeBufferPool(n int, bufSize int) *FixedSizeBufferPool {
	arena := make([]byte, n*bufSize)

	buffers := make([][]byte, n)
	for i := 0; i < n; i++ {
		start := i * bufSize
		end := start + bufSize
		buffers[i] = arena[start:end:end] // full slice expression
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
	}
}

func (p *FixedSizeBufferPool) Get() ([]byte, uint16) {
	id := <-p.free
	return p.buffers[id], id
}

func (p *FixedSizeBufferPool) Return(id uint16) {
	p.free <- id
}
