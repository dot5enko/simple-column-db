package executortypes

import (
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
)

const MaxFiltersPerField = 16

// todo realocate with arena to allow dynamic size of blocks and chunks?
type ChunkExecutorThreadCache struct {
	AbsBlockMaps [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks       [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo

	FilterCache [MaxFiltersPerField]query.RuntimeFilterCache

	ThreadIdx int
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.SetRuntimeValue(nil, c.ThreadIdx)
	}
}
