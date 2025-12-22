package cache

import (
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type SlabCacheItem struct {
	CacheEntryId uuid.UUID

	Header  *schema.DiskSlabHeader
	Data    [schema.SlabDiskContentsUncompressed]byte
	RtStats *CacheStats
}
