package executor

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
	SlabHeader  *schema.DiskSlabHeader

	// 32 filters max ?
	HeaderFilterMatchResult [16]BlockRuntimeFilterCache
}
