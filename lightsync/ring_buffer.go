package lightsync

import (
	"log/slog"
	"sync/atomic"
)

type RingQueueEntry[T any] struct {
	Value T
	Set   int32
}
type RingQueue[T any] struct {
	buf  []RingQueueEntry[T]
	head uint64
	tail uint64
	mask uint64
}

func NewRingQueue[T any](size int) *RingQueue[T] {
	if size&(size-1) != 0 {
		panic("size must be power of two") // why? because we use mask to get index in buffer
	}
	return &RingQueue[T]{
		buf:  make([]RingQueueEntry[T], size),
		mask: uint64(size - 1),
	}
}

func (q *RingQueue[T]) Push(item T) bool {

	pushIters := 0
	defer func() {
		if pushIters > 1 {
			slog.Info("push iter count", "count", pushIters)
		}
	}()

	size := uint64(len(q.buf))

	for {
		tail := atomic.LoadUint64(&q.tail)
		head := atomic.LoadUint64(&q.head)
		if tail-head >= size {
			return false // full
		}
		if atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {

			idx := tail & q.mask
			q.buf[idx].Value = item
			atomic.StoreInt32(&q.buf[idx].Set, 1)
			return true
		}
	}
}

func (q *RingQueue[T]) Pop() (T, bool) {
	head := atomic.LoadUint64(&q.head)
	if !atomic.CompareAndSwapInt32(&q.buf[head&q.mask].Set, 1, 0) {
		var zero T
		return zero, false // not ready yet
	}
	item := q.buf[head&q.mask].Value
	atomic.AddUint64(&q.head, 1)
	return item, true
}
