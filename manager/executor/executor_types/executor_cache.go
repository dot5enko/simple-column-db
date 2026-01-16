package executortypes

import (
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/schema"
)

const MaxFiltersPerField = rtconfig.QUERY_MAX_FILTERS_PER_FIELD

// todo realocate with arena to allow dynamic size of blocks and chunks?
type ChunkExecutorThreadCache struct {
	AbsBlockMaps [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks       [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo

	FilterCache [MaxFiltersPerField]query.RuntimeFilterCache

	SelectorBuffer [schema.BlockRowsSize]uint64

	// local thread cache, no locks needed
	FilterApplyCache map[FilterApplyKeyType]*BlockScanFilterResultCache
	BitsetCache      *cache.TypedRingBuffer[bits.Bitfield]

	ThreadIdx int
}

type FilterApplyKeyType [48 + 17]byte

type BlockScanFilterResultCache struct {
	Result *bits.Bitfield
	Reads  int
}

func (r *ChunkExecutorThreadCache) GetCachedFilter(f schema.FilterIdType, blockUid schema.BlockUniqueId) (*bits.Bitfield, int) {

	var fullId FilterApplyKeyType
	copy(fullId[:], f[:])
	copy(fullId[48:], blockUid[:])

	val := r.FilterApplyCache[fullId]

	// defer perf.AllocsDetection()()

	if val == nil {
		val = &BlockScanFilterResultCache{
			Reads: 0,
		}
		r.FilterApplyCache[fullId] = val
	}

	val.Reads += 1

	return val.Result, int(val.Reads)
}

var total atomic.Int32

func (r *ChunkExecutorThreadCache) PutCached(f schema.FilterIdType, blockUid schema.BlockUniqueId, val *bits.Bitfield) {

	// totalvalues := total.Add(1)
	// color.Red("total cached filters: %d", totalvalues)

	var fullId FilterApplyKeyType
	copy(fullId[:], f[:])
	copy(fullId[48:], blockUid[:])

	cV := r.FilterApplyCache[fullId]

	cV.Result = val
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.SetRuntimeValue(nil, c.ThreadIdx)
	}
}
