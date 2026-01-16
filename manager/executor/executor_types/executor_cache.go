package executortypes

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/schema"
)

const MaxFiltersPerField = rtconfig.QUERY_MAX_FILTERS_PER_FIELD

// todo realocate with arena to allow dynamic size of blocks and chunks?
// local thread cache, no locks needed

const MaxBitsetsCached = rtconfig.ROWS_PER_BLOCK / 16

type ChunkExecutorThreadCache struct {
	AbsBlockMaps [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged
	Blocks       [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo

	FilterCache      [MaxFiltersPerField]query.RuntimeFilterCache
	BitsetsMetaCache [MaxBitsetsCached]BlockScanFilterResultCache
	BitsetsCache     [MaxBitsetsCached]bits.Bitfield

	FilterApplyCacheMapping map[FilterApplyKeyType]uint16 // max filters * blocks cached = 32k entries

	CachedBitsets uint16
	FreedBitsets  uint16

	//
	ThreadIdx int
}

type FilterApplyKeyType [48 + 17]byte

type BlockScanFilterResultCache struct {
	Reads       uint16
	initialized bool
	lastUsageNs uint64
}

func (r *ChunkExecutorThreadCache) compactCache() error {

	tNow := time.Now().UnixNano()
	_ = tNow

	return nil
}

func (r *ChunkExecutorThreadCache) GetCachedFilter(
	f schema.FilterIdType,
	blockUid schema.BlockUniqueId,
	tNs uint64,
) (*bits.Bitfield, int) {

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
	val := &r.BitsetsMetaCache[mappingIdx]
	val.Reads += 1
	val.lastUsageNs = tNs

	if !val.initialized {
		return nil, int(val.Reads)
	}

	return &r.BitsetsCache[mappingIdx], int(val.Reads)
}

var total atomic.Int32

func (r *ChunkExecutorThreadCache) PutCached(
	f schema.FilterIdType,
	blockUid schema.BlockUniqueId,
	val bits.Bitfield,
	nowNano uint64,
) {

	// totalvalues := total.Add(1)
	// color.Red("total cached filters: %d", totalvalues)

	var fullId FilterApplyKeyType
	copy(fullId[:], f[:])
	copy(fullId[48:], blockUid[:])

	mappingIdx, ok := r.FilterApplyCacheMapping[fullId]

	if !ok {
		panic(fmt.Sprintf("trying to set cache for non existing mapping: %d => %s", mappingIdx, string(fullId[:20])))
	}

	rval := &r.BitsetsMetaCache[mappingIdx]
	rval.initialized = true

	r.BitsetsCache[mappingIdx] = val
}

func (c *ChunkExecutorThreadCache) Reset() {

	for i := range query.ExecutorChunkSizeBlocks {
		c.AbsBlockMaps[i].Reset()

		bRef := &c.Blocks[i]

		bRef.BlockHeader = nil
		bRef.SetRuntimeValue(nil, c.ThreadIdx)
	}
}
