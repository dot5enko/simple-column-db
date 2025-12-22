package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type CacheEntry struct {
	Item  SlabCacheItem
	InUse atomic.Bool
}

type SlabCacheManager struct {
	storage       map[uuid.UUID]*CacheEntry
	storageLocker sync.RWMutex
}

func NewSlabCacheManager() *SlabCacheManager {
	return &SlabCacheManager{
		storage: make(map[uuid.UUID]*CacheEntry),
	}
}

func (m *SlabCacheManager) Prefill(size int) {

	m.storageLocker.Lock()
	defer m.storageLocker.Unlock()

	for i := 0; i < size; i++ {
		entry := m.newEntry()
		m.storage[entry.Item.CacheEntryId] = entry
	}
}

var ErrNoFreeEntries = errors.New("no free entries")

func (m *SlabCacheManager) newEntry() *CacheEntry {

	tn := time.Now()

	uid, _ := uuid.NewV7()

	// preallocate memory
	cacheItem := SlabCacheItem{
		CacheEntryId: uid,
		RtStats:      &CacheStats{Created: tn, Reads: 0},
	}

	item := &CacheEntry{
		Item: cacheItem,
	}

	return item
}

func (m *SlabCacheManager) GetCacheEntry() (*SlabCacheItem, error) {

	m.storageLocker.RLock()
	defer m.storageLocker.RUnlock()

	for _, entry := range m.storage {
		if entry.InUse.CompareAndSwap(false, true) {

			// todo mark usage
			return &entry.Item, nil
		}
	}

	return nil, ErrNoFreeEntries

}
