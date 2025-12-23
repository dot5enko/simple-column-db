package executor

import (
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type ChunkExecutorThreadCache struct {
	absBlockMaps       [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	blocks             [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo
	indicesResultCache [schema.BlockRowsSize]uint16
}
