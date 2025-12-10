package block

import "sync"

type RuntimeBlockData[T any] struct {
	Header DiskHeader

	lock sync.RWMutex

	Data  []T
	Cap   int
	Items int
}

func (b *RuntimeBlockData[T]) Write(data []T, elements int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	copy(b.Data[b.Items:], data[:elements])
	b.Items += elements
}

func (b *RuntimeBlockData[T]) ExportData(out []T) int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return copy(out, b.Data[:b.Items])
}

func NewRuntimeBlockData[T any](cap int) *RuntimeBlockData[T] {
	return &RuntimeBlockData[T]{
		Cap:  cap,
		Data: make([]T, 0, cap),
	}
}
