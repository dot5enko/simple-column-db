package cache

import "time"

type CacheStats struct {
	CacheEntryId uint16

	Reads   int
	Created time.Time
}
