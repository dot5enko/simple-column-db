package executortypes

import (
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type ChunkExecutorThreadCache struct {
	AbsBlockMaps       [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks             [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo
	IndicesResultCache [schema.BlockRowsSize]uint16
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.Val = nil
	}
}
