package meta

import (
	"fmt"
	"sync"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

type BlockCacheItem struct {
	header  *schema.DiskHeader
	runtime *schema.RuntimeBlockData

	rtStats *cache.CacheStats
}

const HeadersCacheSize = 256 * schema.TotalHeaderSize

type SlabManager struct {
	storagePath string

	// runtime cache
	cache  map[[32]byte]BlockCacheItem
	locker sync.RWMutex

	slabHeaderCacheItem   map[uuid.UUID]*cache.SlabCacheItem
	slabHeaderCacheLocker sync.RWMutex

	// buffers
	headerReaderBufferRing *cache.FixedSizeBufferPool
	fullSlabBufferRing     *cache.FixedSizeBufferPool
	slabHeaderCache        *cache.TypedRingBuffer[schema.DiskSlabHeader]
	slabRuntimeCache       *cache.TypedRingBuffer[cache.SlabCacheItem]

	meta *MetaManager

	loadGroup singleflight.Group
}

// todo : remove const/literals, add config param
func NewSlabManager(storagePath string, meta *MetaManager) *SlabManager {
	sm := &SlabManager{
		storagePath:         storagePath,
		cache:               map[[32]byte]BlockCacheItem{},
		slabHeaderCacheItem: map[uuid.UUID]*cache.SlabCacheItem{},

		meta: meta,
	}

	// 1slab = ±10MB ram
	sm.fullSlabBufferRing = cache.NewFixedSizeBufferPool(16, schema.SlabDiskContentsUncompressed)
	sm.headerReaderBufferRing = cache.NewFixedSizeBufferPool(32, schema.SlabHeaderFixedSize)

	sm.slabRuntimeCache = cache.NewTypedRingBuffer[cache.SlabCacheItem](32)

	// slab reusing header
	// todo profile and optimize
	sm.slabHeaderCache = cache.NewTypedRingBuffer[schema.DiskSlabHeader](128)

	return sm
}

func (m *SlabManager) GetSlabFromCache(uid uuid.UUID) *cache.SlabCacheItem {
	return m.getSlabFromCache(uid)
}
func (m *SlabManager) getSlabFromCache(uid uuid.UUID) *cache.SlabCacheItem {

	m.slabHeaderCacheLocker.RLock()
	defer m.slabHeaderCacheLocker.RUnlock()

	if item, ok := m.slabHeaderCacheItem[uid]; ok {

		item.RtStats.Reads++
		return item
	}

	return nil
}

// IngestIntoBlock(field.slab, curBlock, field.Data[field.ingested:])

func GetUniqueBlockId(slab, block uuid.UUID) [32]byte {

	uid := [32]byte{}

	copy(uid[0:], slab[:])
	copy(uid[16:], block[:])

	return uid
}

func (m *SlabManager) getBlockFromCache(slab, block uuid.UUID) *BlockCacheItem {

	m.locker.RLock()
	defer m.locker.RUnlock()

	uid := GetUniqueBlockId(slab, block)

	if item, ok := m.cache[uid]; ok {

		// log.Printf(" --- reading block %s from cache : %d", block.String(), item.rtStats.Reads)

		item.rtStats.Reads++
		return &item
	}

	return nil
}

// load block from slab
func (m *SlabManager) LoadBlockToRuntimeBlockData(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
) (*schema.RuntimeBlockData, error) {

	cached := m.getBlockFromCache(slab.Uid, block)

	if cached != nil {
		return cached.runtime, nil
	} else {
		// put into cache

		var blockHeader schema.DiskHeader
		blockIdx := -1
		blockStartOffset := 0

		for idx, it := range slab.BlockHeaders {
			if it.Uid == block {
				blockHeader = it
				blockIdx = idx
				break
			}
		}

		if blockIdx < 0 {
			return nil, fmt.Errorf("block you are looking for (%s) not found in slab %s", block.String(), slab.Uid.String())
		} else {

			blockSize := blockHeader.DataType.BlockSize()
			blockStartOffset = blockIdx * blockSize

			slabCache := m.getSlabFromCache(slab.Uid)
			if slabCache == nil || !slabCache.DataLoaded {
				_, loadSlabErr := m.LoadSlabDataContents(&schemaObject, slab.Uid)
				if loadSlabErr != nil {
					return nil, loadSlabErr
				}
				slabCache = m.getSlabFromCache(slab.Uid)
				if slabCache == nil {
					panic("cache should be loaded by now, probably out of memory?")
				}
			}

			blockRawData := slabCache.Data[blockStartOffset:]

			// log.Printf(" --- loading %s block. blockHeader.StartOffset:%d", blockHeader.Uid.String(), blockHeader.StartOffset)

			runtimeBlockData, runtimeDecodeErr := DecodeRawBlockData(blockRawData, &blockHeader)

			if runtimeDecodeErr != nil {
				return nil, fmt.Errorf("unable to decoded raw block data for slab %s. block %s: %s", slab.Uid.String(), block.String(), runtimeDecodeErr.Error())
			} else {
				m.locker.Lock()
				defer m.locker.Unlock()

				blockId := GetUniqueBlockId(slab.Uid, block)

				m.cache[blockId] = BlockCacheItem{
					header:  &blockHeader,
					runtime: runtimeBlockData,
					rtStats: &cache.CacheStats{CacheEntryId: slabCache.CacheEntryId, Created: time.Now(), Reads: 1},
				}

				return runtimeBlockData, nil
			}

		}

	}

}

// return RuntimeBlockData
func DecodeRawBlockData(blockData []byte, bheader *schema.DiskHeader) (*schema.RuntimeBlockData, error) {

	var runtimeData *schema.RuntimeBlockData

	switch bheader.DataType {

	case schema.Float64FieldType:
		result := bits.MapBytesToArray[float64](blockData, schema.BlockRowsSize)
		runtimeData = schema.NewRuntimeBlockDataFromSlice(result, int(bheader.Items))

	case schema.Float32FieldType:
		result := bits.MapBytesToArray[float32](blockData, schema.BlockRowsSize)
		runtimeData = schema.NewRuntimeBlockDataFromSlice(result, int(bheader.Items))

	case schema.Uint64FieldType:

		result := bits.MapBytesToArray[uint64](blockData, schema.BlockRowsSize)
		runtimeData = schema.NewRuntimeBlockDataFromSlice(result, int(bheader.Items))

	case schema.Uint8FieldType:
		result := bits.MapBytesToArray[uint8](blockData, schema.BlockRowsSize)
		runtimeData = schema.NewRuntimeBlockDataFromSlice(result, int(bheader.Items))

	default:
		return nil, fmt.Errorf("unknown type while decoding raw block data: %s", bheader.DataType.String())
	}

	runtimeData.Header = bheader

	return runtimeData, nil

}
