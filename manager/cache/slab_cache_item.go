package cache

import (
	"github.com/dot5enko/simple-column-db/schema"
)

type SlabCacheItem struct {
	CacheEntryId uint16

	Header  *schema.DiskSlabHeader
	Data    [schema.SlabDiskContentsUncompressed]byte
	RtStats *CacheStats
}
