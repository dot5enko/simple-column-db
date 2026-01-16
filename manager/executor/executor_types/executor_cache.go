package executortypes

import (
	"fmt"
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/schema"
)

const MaxFiltersPerField = rtconfig.QUERY_MAX_FILTERS_PER_FIELD

// todo realocate with arena to allow dynamic size of blocks and chunks?
// local thread cache, no locks needed

const MaxBitsetsCached = rtconfig.ROWS_PER_BLOCK / 8

type ChunkExecutorThreadCache struct {
	AbsBlockMaps [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks       [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo

	FilterCache [MaxFiltersPerField]query.RuntimeFilterCache

	FilterApplyCacheMapping map[FilterApplyKeyType]uint16 // max filters * blocks cached = 32k entries
	BitsetsCache            [MaxBitsetsCached]BlockScanFilterResultCache
	CachedBitsets           uint16

	//
	ThreadIdx int
}

type FilterApplyKeyType [48 + 17]byte

type BlockScanFilterResultCache struct {
	Result      bits.Bitfield
	Reads       uint16
	initialized bool
}

func (r *ChunkExecutorThreadCache) GetCachedFilter(f schema.FilterIdType, blockUid schema.BlockUniqueId) (*bits.Bitfield, int) {

	var fullId FilterApplyKeyType
	copy(fullId[:], f[:])
	copy(fullId[48:], blockUid[:])

	mappingIdx, exists := r.FilterApplyCacheMapping[fullId]

	// defer perf.AllocsDetection()()

	if !exists {

		if r.CachedBitsets >= MaxBitsetsCached {
			panic("thread cache bitset cache overflow")
		}

		mappingIdx = r.CachedBitsets
		r.CachedBitsets += 1

		r.FilterApplyCacheMapping[fullId] = mappingIdx
	}
	val := &r.BitsetsCache[mappingIdx]
	val.Reads += 1

	if !val.initialized {
		return nil, int(val.Reads)
	}

	return &val.Result, int(val.Reads)
}

var total atomic.Int32

func (r *ChunkExecutorThreadCache) PutCached(f schema.FilterIdType, blockUid schema.BlockUniqueId, val bits.Bitfield) {

	// totalvalues := total.Add(1)
	// color.Red("total cached filters: %d", totalvalues)

	var fullId FilterApplyKeyType
	copy(fullId[:], f[:])
	copy(fullId[48:], blockUid[:])

	mappingIdx, ok := r.FilterApplyCacheMapping[fullId]

	if !ok {
		panic(fmt.Sprintf("trying to set cache for non existing mapping: %d => %s", mappingIdx, string(fullId[:20])))
	}

	rval := &r.BitsetsCache[mappingIdx]
	rval.initialized = true

	rval.Result = val
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.SetRuntimeValue(nil, c.ThreadIdx)
	}
}
