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

	size           atomic.Int32
	totalAllocated atomic.Int32

	name string
}

type node[T any] struct {
	next *node[T]
	prev *node[T]

	val *T
}

func NewTypedRingBuffer[T any](n int) *TypedRingBuffer[T] {

	rb := &TypedRingBuffer[T]{
		stats: Stats{
			Size: n,
		},
	}

	rb.preallocateChunk(n)

	return rb
}

func (p *TypedRingBuffer[T]) preallocateChunk(n int) *TypedRingBuffer[T] {

	// buffers := make([]T, n)

	// for i := 0; i < n; i++ {
	// 	p.pushNode(&node[T]{val: &buffers[i]})
	// }

	values := make([]T, n)
	nodes := make([]node[T], n)

	for i := 0; i < n; i++ {
		nodes[i].val = &values[i]
		p.pushNode(&nodes[i])
	}

	return p
}

func (rb *TypedRingBuffer[T]) pushNode(n *node[T]) {
	for {
		cur := rb.head.Load()
		n.prev = cur
		if rb.head.CompareAndSwap(cur, n) {

			rb.size.Add(1)

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
			counter += 1

			if counter > 128 {

				if rb.generator != nil {
					newNode := &node[T]{val: rb.generator(nil)}
					rb.pushNode(newNode)

					allocatedTotal := rb.totalAllocated.Add(1)
					color.Magenta("ring buffer empty %s. +node. <size=%d> <total_allocated=%d>", rb.name, rb.size.Load(), allocatedTotal)
					return newNode

				} else {
					panic("no generator function provided, can't generate new element")
				}

			}

		} else {
			if rb.head.CompareAndSwap(cur, cur.prev) {
				rb.size.Add(-1)
				return cur
			}
		}

	}
}

func (p *TypedRingBuffer[T]) WithInitializer(cb func(item *T) *T) *TypedRingBuffer[T] {
	p.generator = cb
	return p
}

func (p *TypedRingBuffer[T]) WithName(n string) *TypedRingBuffer[T] { p.name = n; return p }

func (p *TypedRingBuffer[T]) Get() *T {

	time0 := time.Now()

	nodeVal := p.popNode()
	waitTime := time.Since(time0)

	p.stats.Reads.Add(1)
	p.stats.WaitTime.Add(int64(waitTime))

	// color.Blue(" -reading %s: <size=%d>", p.name, p.size.Load())

	return nodeVal.val
}

func (p *TypedRingBuffer[T]) Return(val *T) {

	p.stats.Returns.Add(1)

	// color.Blue(" +returning %s: <size=%d>", p.name, p.size.Load())

	p.pushNode(&node[T]{val: val})

}

func (p *TypedRingBuffer[T]) GetStats() *Stats { return &p.stats }
