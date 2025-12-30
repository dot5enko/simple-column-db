package cache

import "time"

type CacheStats struct {
	CacheEntryId uint16

	Reads   int64
	Created time.Time
}
