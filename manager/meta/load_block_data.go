package meta

import (
	"fmt"
	"slices"

	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func UniqueCountSorted[T ops.NumericTypes](xs []T) int {
	if len(xs) == 0 {
		return 0
	}
	slices.Sort(xs)

	cnt := 1
	for i := 1; i < len(xs); i++ {
		if xs[i] != xs[i-1] {
			cnt++
		}
	}
	return cnt
}

func (m *SlabManager) LoadBlockToRuntimeBlockData(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
) (*schema.RuntimeBlockData, error) {

	cached := m.getBlockFromCache(slab.Uid, block)

	if cached != nil {
		return cached.runtime, nil
	} else {

		newCachedEntry := &BlockCacheItem{}

		// put into cache

		blockIdx := -1
		blockStartOffset := 0

		for idx, it := range slab.BlockHeaders {
			if it.Uid == block {
				newCachedEntry.header = it
				blockIdx = idx
				break
			}
		}

		if blockIdx < 0 {
			return nil, fmt.Errorf("block you are looking for (%s) not found in slab %s", block.String(), slab.Uid.String())
		} else {

			blockSize := newCachedEntry.header.DataType.BlockSize()

			slabData := m.getSlabDataFromCache(slab.Uid)
			if slabData == nil {
				_, loadSlabErr := m.LoadSlabDataContents(&schemaObject, slab.Uid)
				if loadSlabErr != nil {
					return nil, loadSlabErr
				}
				slabData = m.getSlabDataFromCache(slab.Uid)
				if slabData == nil {
					panic("cache should be loaded by now, probably out of memory?")
				}
			}

			blockStartOffset = blockIdx * blockSize
			blockRawData := slabData.Data[blockStartOffset:]

			// this function returns a reference to memory, not copy of it
			runtimeBlockData, runtimeDecodeErr := DecodeRawBlockData(blockRawData, &newCachedEntry.header)

			// runtime debug info
			runtimeBlockData.Slab = slab.Uid
			runtimeBlockData.BlockIndice = uint64(blockIdx)

			if runtimeDecodeErr != nil {
				return nil, fmt.Errorf("unable to decoded raw block data for slab %s. block %s: %s", slab.Uid.String(), block.String(), runtimeDecodeErr.Error())
			} else {
				m.rt.locker.Lock()
				defer m.rt.locker.Unlock()

				blockId := GetUniqueBlockId(slab.Uid, block)

				// slog.Info("cache entry put", "entry_id", slabData.RtStats.CacheEntryId)

				newCachedEntry.runtime = runtimeBlockData

				m.rt.cache[blockId] = newCachedEntry

				// &BlockCacheItem{
				// 	header:  &blockHeader,
				// 	runtime: runtimeBlockData,
				// 	rtStats: &cache.CacheStats{CacheEntryId: slabData.RtStats.CacheEntryId, Reads: 1},
				// }

				return runtimeBlockData, nil
			}

		}

	}

}
