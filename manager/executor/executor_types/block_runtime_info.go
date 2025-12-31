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

	// if a query has more than that app panics
	HeaderFilterMatchResult [MaxFiltersPerField]BlockRuntimeFilterCache
}
