package cache

import (
	"time"

	"github.com/dot5enko/simple-column-db/schema"
)

type SlabCacheItem struct {
	CacheEntryId uint16

	Header *schema.DiskSlabHeader

	DataLoaded bool
	Data       [schema.SlabDiskContentsUncompressed]byte

	//
	RtStats *CacheStats
}

// reset
func (item *SlabCacheItem) Reset() {
	item.Header = nil
	item.DataLoaded = false
	item.RtStats = &CacheStats{
		Created: time.Now(),
	}
}
