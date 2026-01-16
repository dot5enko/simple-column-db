package executortypes

import (
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

const MaxFiltersPerField = rtconfig.QUERY_MAX_FILTERS_PER_FIELD

// todo realocate with arena to allow dynamic size of blocks and chunks?
type ChunkExecutorThreadCache struct {
	AbsBlockMaps [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks       [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo

	FilterCache [MaxFiltersPerField]query.RuntimeFilterCache

	SelectorBuffer [schema.BlockRowsSize]uint64

	// fcacheL     sync.RWMutex
	FilterApplyCache map[schema.FilterIdType]map[schema.BlockUniqueId]*bits.Bitfield

	ThreadIdx int
}

func (r *ChunkExecutorThreadCache) GetCachedFilter(f schema.FilterIdType, blockUid schema.BlockUniqueId) *bits.Bitfield {
	// r.fcacheL.RLock()
	val := r.FilterApplyCache[f]

	if val != nil {
		return val[blockUid]
	}

	// r.fcacheL.RUnlock()
	return nil
}

var total atomic.Int32

func (r *ChunkExecutorThreadCache) PutCached(f schema.FilterIdType, blockUid schema.BlockUniqueId, val *bits.Bitfield) {
	// r.fcacheL.Lock()

	totalvalues := total.Add(1)
	color.Red("total cached filters: %s", totalvalues)

	cV := r.FilterApplyCache[f]

	if cV == nil {
		r.FilterApplyCache[f] = map[schema.BlockUniqueId]*bits.Bitfield{}
	}

	r.FilterApplyCache[f][blockUid] = val
	// r.fcacheL.Unlock()
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.SetRuntimeValue(nil, c.ThreadIdx)
	}
}
