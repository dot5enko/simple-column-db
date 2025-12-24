package cache

import (
	"errors"
	"log/slog"
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

	usedCount int32
	allocated int
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

	m.allocated += len(m.storage)

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

			entry.Item.RtStats.Reads++
			usage := atomic.AddInt32(&m.usedCount, 1)

			if usage == int32(m.allocated) {
				slog.Info("slab cache entry used", "total_used", usage, "allocated", m.allocated)
			}

			// todo mark usage
			return &entry.Item, nil
		}
	}

	return nil, ErrNoFreeEntries

}

func (m *SlabCacheManager) Release(uid uuid.UUID) {

	m.storageLocker.RLock()
	defer m.storageLocker.RUnlock()

	for _, v := range m.storage {
		wasInUse := v.InUse.CompareAndSwap(true, false)

		if wasInUse {
			usage := atomic.AddInt32(&m.usedCount, -1)
			slog.Info("slab cache entry released", "total_used", usage, "allocated", m.allocated)
		}

	}

}
