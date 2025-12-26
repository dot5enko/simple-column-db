package cache

import (
	"time"

	"github.com/dot5enko/simple-column-db/schema"
)

type SlabCacheItem struct {
	CacheEntryId uint16

	Header *schema.DiskSlabHeader

	//
	RtStats *CacheStats
}

// reset
func (item *SlabCacheItem) Reset() {
	item.Header = nil

	if item.RtStats != nil {
		item.RtStats.Reads = 0
		item.RtStats.Created = time.Now()
	} else {
		item.RtStats = &CacheStats{
			Created: time.Now(),
		}
	}
}

type SlabDataCacheItem struct {
	Data [schema.SlabDiskContentsUncompressed]byte
	// data is dirty flag ?

	RtStats *CacheStats
}

func (item *SlabDataCacheItem) Reset() {

	if item.RtStats != nil {
		item.RtStats.Reads = 0
		item.RtStats.Created = time.Now()
	} else {
		item.RtStats = &CacheStats{
			Created: time.Now(),
		}
	}
}
