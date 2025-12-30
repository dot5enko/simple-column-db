package meta

import (
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/manager/cache"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/perf"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

type BlockCacheItem struct {
	header  *schema.DiskHeader
	runtime *schema.RuntimeBlockData

	rtStats *cache.CacheStats
}

type SlabManagerRuntimeCache struct {
	cache  map[[32]byte]BlockCacheItem
	locker sync.RWMutex

	slabHeaderCacheItem   map[uuid.UUID]*cache.SlabCacheItem
	slabHeaderCacheLocker sync.RWMutex

	slabDataCache       map[uuid.UUID]*cache.SlabDataCacheItem
	slabDataCacheLocker sync.RWMutex

	loadGroup singleflight.Group

	globalSessionCounter uint64
}

type SlabManagerSession struct {
	perf_stats *perf.PerformanceMetrics
	cache      *executortypes.ChunkExecutorThreadCache
	idx        uint64
}

type SlabManager struct {
	storagePath string

	// runtime cache
	rt *SlabManagerRuntimeCache

	// buffers
	headerReaderBufferRing *cache.FixedSizeBufferPool
	fullSlabBufferRing     *cache.FixedSizeBufferPool
	slabHeaderCache        *cache.TypedRingBuffer[schema.DiskSlabHeader]
	slabRuntimeCache       *cache.TypedRingBuffer[cache.SlabDataCacheItem]

	meta *MetaManager

	session *SlabManagerSession
}

// copy with session
func (sm *SlabManager) NewSession(cache *executortypes.ChunkExecutorThreadCache) *SlabManager {
	newSm := &SlabManager{
		storagePath:            sm.storagePath,
		rt:                     sm.rt,
		headerReaderBufferRing: sm.headerReaderBufferRing,
		fullSlabBufferRing:     sm.fullSlabBufferRing,
		slabHeaderCache:        sm.slabHeaderCache,
		slabRuntimeCache:       sm.slabRuntimeCache,
		meta:                   sm.meta,
		session: &SlabManagerSession{
			perf_stats: &perf.PerformanceMetrics{
				IoTime: time.Duration(0),
			},
			cache: cache,
			idx:   atomic.AddUint64(&sm.rt.globalSessionCounter, 1),
		},
	}

	return newSm
}

func (sm *SlabManager) GetSession() *perf.PerformanceMetrics {

	if sm.session == nil {
		return nil
	}
	return sm.session.perf_stats
}

// buffers report
func printSingleBufferReport(name string, bufStats *cache.Stats) {
	slog.Info("buffer effectiveness report",
		"size", bufStats.Size,
		"total_reads", bufStats.Reads.Load(),
		"returns", bufStats.Returns.Load(),
		"wait_time_ns", time.Duration(bufStats.WaitTime.Load()),
		"buf_name", name,
	)
}
func (m *SlabManager) PrintBufferEffectivityReport() {

	headerSize := 32

	headerPart := strings.Repeat("-", headerSize)

	log.Printf("%s%s%s", headerPart, "buffer effectiveness", headerPart)

	printSingleBufferReport("headerReaderBufferRing", m.headerReaderBufferRing.GetStats())
	printSingleBufferReport("fullSlabBufferRing", m.fullSlabBufferRing.GetStats())
	printSingleBufferReport("slabHeaderCache", m.slabHeaderCache.GetStats())
	printSingleBufferReport("slabRuntimeCache", m.slabRuntimeCache.GetStats())

	log.Println(headerPart)

}

// todo : remove const/literals, add config param
func NewSlabManager(storagePath string, meta *MetaManager) *SlabManager {

	sm := &SlabManager{
		storagePath: storagePath,
		rt: &SlabManagerRuntimeCache{
			cache:               map[[32]byte]BlockCacheItem{},
			slabHeaderCacheItem: map[uuid.UUID]*cache.SlabCacheItem{},
			slabDataCache:       map[uuid.UUID]*cache.SlabDataCacheItem{},
		},
		meta: meta,
	}

	// 1slab = ±10MB ram
	sm.fullSlabBufferRing = cache.NewFixedSizeBufferPool(16, schema.SlabDiskContentsUncompressed)
	sm.headerReaderBufferRing = cache.NewFixedSizeBufferPool(32, schema.SlabHeaderFixedSize)

	sm.slabRuntimeCache = cache.NewTypedRingBuffer[cache.SlabDataCacheItem](32)

	// slab reusing header
	// todo profile and optimize
	sm.slabHeaderCache = cache.NewTypedRingBuffer[schema.DiskSlabHeader](128)

	return sm
}

func (m *SlabManager) GetSlabHeaderFromCache(uid uuid.UUID) *cache.SlabCacheItem {
	return m.getSlabHeaderFromCache(uid)
}
func (m *SlabManager) getSlabHeaderFromCache(uid uuid.UUID) *cache.SlabCacheItem {

	m.rt.slabHeaderCacheLocker.RLock()
	defer m.rt.slabHeaderCacheLocker.RUnlock()

	if item, ok := m.rt.slabHeaderCacheItem[uid]; ok {

		item.RtStats.Reads++
		return item
	}

	return nil
}

func (m *SlabManager) getSlabDataFromCache(uid uuid.UUID) *cache.SlabDataCacheItem {

	m.rt.slabDataCacheLocker.RLock()
	defer m.rt.slabDataCacheLocker.RUnlock()

	if item, ok := m.rt.slabDataCache[uid]; ok {
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

	m.rt.locker.RLock()
	defer m.rt.locker.RUnlock()

	uid := GetUniqueBlockId(slab, block)

	if item, ok := m.rt.cache[uid]; ok {

		atomic.AddInt64(&item.rtStats.Reads, 1)
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

			blockRawData := slabData.Data[blockStartOffset:]
			runtimeBlockData, runtimeDecodeErr := DecodeRawBlockData(blockRawData, &blockHeader)

			if runtimeDecodeErr != nil {
				return nil, fmt.Errorf("unable to decoded raw block data for slab %s. block %s: %s", slab.Uid.String(), block.String(), runtimeDecodeErr.Error())
			} else {
				m.rt.locker.Lock()
				defer m.rt.locker.Unlock()

				blockId := GetUniqueBlockId(slab.Uid, block)

				// slog.Info("cache entry put", "entry_id", slabData.RtStats.CacheEntryId)

				m.rt.cache[blockId] = BlockCacheItem{
					header:  &blockHeader,
					runtime: runtimeBlockData,
					rtStats: &cache.CacheStats{CacheEntryId: slabData.RtStats.CacheEntryId, Created: time.Now(), Reads: 1},
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
