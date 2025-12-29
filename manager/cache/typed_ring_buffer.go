package cache

import (
	"sync/atomic"
	"time"
)

type TypedRingBuffer[T any] struct {
	stats Stats

	head atomic.Pointer[node[T]]
}

type node[T any] struct {
	next *node[T]
	prev *node[T]

	val *T
}

func (rb *TypedRingBuffer[T]) pushNode(n *node[T]) {
	for {
		cur := rb.head.Load()
		n.prev = cur
		if rb.head.CompareAndSwap(cur, n) {
			if cur != nil {
				cur.next = n
			}
			return
		}
	}
}

func (rb *TypedRingBuffer[T]) popNode() *node[T] {
	for {
		cur := rb.head.Load()

		if cur == nil {
			panic("empty TypedRingBuffer value on the head")
		}

		if rb.head.CompareAndSwap(cur, cur.prev) {
			return cur
		}
	}
}

func NewTypedRingBuffer[T any](n int) *TypedRingBuffer[T] {

	buffers := make([]T, n)

	rb := &TypedRingBuffer[T]{
		stats: Stats{
			Size: n,
		},
	}

	for i := 0; i < n; i++ {
		rb.pushNode(&node[T]{val: &buffers[i]})
	}

	return rb
}

func (p *TypedRingBuffer[T]) Get() *T {

	time0 := time.Now()

	nodeVal := p.popNode()
	waitTime := time.Since(time0)

	p.stats.Reads.Add(1)
	p.stats.WaitTime.Add(int64(waitTime))

	return nodeVal.val
}

func (p *TypedRingBuffer[T]) Return(val *T) {

	p.stats.Returns.Add(1)

	p.pushNode(&node[T]{val: val})

}

func (p *TypedRingBuffer[T]) GetStats() *Stats { return &p.stats }
