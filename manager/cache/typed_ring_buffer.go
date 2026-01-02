package cache

import (
	"sync/atomic"
	"time"

	"github.com/fatih/color"
)

type TypedRingBuffer[T any] struct {
	stats Stats

	head atomic.Pointer[node[T]]

	generator func(*T) *T
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

	counter := 0

	for {
		cur := rb.head.Load()

		if cur == nil {
			panic("empty TypedRingBuffer value on the head")
		}

		counter += 1

		if rb.head.CompareAndSwap(cur, cur.prev) {
			return cur
		}

		if counter > 128 {
			color.Red("too many cycles of waiting")

			if rb.generator != nil {
				newNode := &node[T]{val: rb.generator(nil)}
				rb.pushNode(newNode)
				return newNode
			}

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

func (p *TypedRingBuffer[T]) WithInitializer(cb func(item *T) *T) {
	p.generator = cb
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
