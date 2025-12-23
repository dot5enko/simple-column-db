package executor

import (
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type ChunkExecutorThreadCache struct {
	absBlockMaps       [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	blocks             [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo
	indicesResultCache [schema.BlockRowsSize]uint16
}

type WrappedCacheItem struct {
	cache  *ChunkExecutorThreadCache
	locker atomic.Bool
	uid    uuid.UUID
}
type ExecutorCacheManager struct {
	threadCaches []*WrappedCacheItem
}

func (m *ExecutorCacheManager) Get() (*ChunkExecutorThreadCache, uuid.UUID) {
	for i := range m.threadCaches {
		if !m.threadCaches[i].locker.CompareAndSwap(false, true) {
			continue
		} else {

			tcacheEntry := m.threadCaches[i]

			return tcacheEntry.cache, tcacheEntry.uid
		}
	}

	return nil, uuid.Nil
}

// prefill
func (m *ExecutorCacheManager) Prefill(count int) {
	for i := 0; i < count; i++ {
		newCache := &ChunkExecutorThreadCache{}

		id, _ := uuid.NewV7()

		m.threadCaches = append(m.threadCaches, &WrappedCacheItem{newCache, atomic.Bool{}, id})
	}
}

func (m *ExecutorCacheManager) Release(uid uuid.UUID) {

	for i := range m.threadCaches {
		if m.threadCaches[i].uid == uid {
			m.threadCaches[i].locker.Store(false)
			break
		}
	}

}

func NewExecutorCacheManager() *ExecutorCacheManager {
	return &ExecutorCacheManager{
		threadCaches: []*WrappedCacheItem{},
	}
}
