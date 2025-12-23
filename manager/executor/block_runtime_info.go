package executor

import "github.com/dot5enko/simple-column-db/schema"

type BlockRuntimeInfo struct {
	Val *schema.RuntimeBlockData

	BlockHeader *schema.DiskHeader
	SlabHeader  *schema.DiskSlabHeader

	// 32 filters max ?
	HeaderFilterMatchResult [4]schema.BoundsFilterMatchResult
}
