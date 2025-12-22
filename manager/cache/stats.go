package cache

import "time"

type CacheStats struct {
	Reads   int
	Created time.Time
}
