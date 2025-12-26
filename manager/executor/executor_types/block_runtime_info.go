package executortypes

import (
	"github.com/dot5enko/simple-column-db/schema"
)

type BlockRuntimeFilterCache struct {
	MatchResult schema.BoundsFilterMatchResult
	Bounds      schema.BoundsFloat
}

type BlockRuntimeInfo struct {
	Val *schema.RuntimeBlockData

	BlockHeader *schema.DiskHeader

	// 16 filters max ?
	// if a query has more than that app panics
	HeaderFilterMatchResult [16]BlockRuntimeFilterCache
}
