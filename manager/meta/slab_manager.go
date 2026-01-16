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
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/perf"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

type BlockCacheItem struct {
	header schema.DiskHeader
	// holds a reference to cached memory, not a copy of it
	runtime *schema.RuntimeBlockData
	rtStats cache.CacheStats
}

type SlabManagerRuntimeCache struct {
	cache  map[[32]byte]*BlockCacheItem
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

type smBuffers struct {
	headerReaderBufferRing *cache.FixedSizeBufferPool
	fullSlabBufferRing     *cache.FixedSizeBufferPool
	SlabHeaderCache        *cache.TypedRingBuffer[schema.DiskSlabHeader]
	SlabRuntimeCache       *cache.TypedRingBuffer[cache.SlabDataCacheItem]

	// generic caches
	MegabyteCache      *cache.TypedRingBuffer[cache.MbCacheItem]
	ThreeMegabyteCache *cache.TypedRingBuffer[cache.ThreeMbCacheItem]
}

type SlabManager struct {
	storagePath string

	// runtime cache
	rt *SlabManagerRuntimeCache

	// buffers
	buffers *smBuffers

	meta    *MetaManager
	session *SlabManagerSession
}

func (sm *SlabManager) GetRuntimeCache() *smBuffers {
	return sm.buffers
}

// copy with session
func (sm *SlabManager) NewSession(cache *executortypes.ChunkExecutorThreadCache) *SlabManager {
	newSm := &SlabManager{
		storagePath: sm.storagePath,
		rt:          sm.rt,
		buffers:     sm.buffers,
		meta:        sm.meta,
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

func (sm *SlabManager) GetCache() *executortypes.ChunkExecutorThreadCache {
	return sm.session.cache
}

func (sm *SlabManager) GetSessionThreadIdx() int {
	return sm.session.cache.ThreadIdx
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

	buffers := m.buffers

	printSingleBufferReport("headerReaderBufferRing", buffers.headerReaderBufferRing.GetStats())
	printSingleBufferReport("fullSlabBufferRing", buffers.fullSlabBufferRing.GetStats())
	printSingleBufferReport("slabHeaderCache", buffers.SlabHeaderCache.GetStats())
	printSingleBufferReport("slabRuntimeCache", buffers.SlabRuntimeCache.GetStats())

	log.Println(headerPart)

}

// todo : remove const/literals, add config param
func NewSlabManager(storagePath string, meta *MetaManager) *SlabManager {

	sm := &SlabManager{
		storagePath: storagePath,
		rt: &SlabManagerRuntimeCache{
			cache:               map[[32]byte]*BlockCacheItem{},
			slabHeaderCacheItem: map[uuid.UUID]*cache.SlabCacheItem{},
			slabDataCache:       map[uuid.UUID]*cache.SlabDataCacheItem{},
		},
		meta: meta,
	}

	buffers := &smBuffers{}

	// 1slab = ±10MB ram
	buffers.fullSlabBufferRing = cache.NewFixedSizeBufferPool(rtconfig.CACHE_STANDBY_SLABS, schema.SlabDiskContentsUncompressed)
	buffers.headerReaderBufferRing = cache.NewFixedSizeBufferPool(rtconfig.CACHE_PRECACHED_SLAB_HEADERS, schema.SlabHeaderFixedSize)

	buffers.SlabRuntimeCache = cache.NewTypedRingBuffer[cache.SlabDataCacheItem](rtconfig.CACHE_PRECACHED_SLABS).
		WithInitializer(func(item *cache.SlabDataCacheItem) *cache.SlabDataCacheItem {
			return &cache.SlabDataCacheItem{RtStats: &cache.CacheStats{}}
		}).
		WithName("SlabRuntimeDataCache")

	// slab reusing header
	// todo profile and optimize
	buffers.SlabHeaderCache = cache.NewTypedRingBuffer[schema.DiskSlabHeader](128).WithName("SlabHeaderCache")

	// generic buffers
	buffers.MegabyteCache = cache.NewTypedRingBuffer[cache.MbCacheItem](32).WithInitializer(func(item *cache.MbCacheItem) *cache.MbCacheItem {
		return &cache.MbCacheItem{RtStats: &cache.CacheStats{}}
	}).WithName("MbCache")
	buffers.ThreeMegabyteCache = cache.NewTypedRingBuffer[cache.ThreeMbCacheItem](16).WithInitializer(func(item *cache.ThreeMbCacheItem) *cache.ThreeMbCacheItem {
		return &cache.ThreeMbCacheItem{RtStats: &cache.CacheStats{}}
	}).WithName("ThreeMbCache")

	sm.buffers = buffers

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
		return item
	}

	return nil
}

// load block from slab

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

		// // sort ?
		// sortStart := time.Now()
		// uniqueItems := UniqueCountSorted(result)
		// sortTook := time.Since(sortStart).Seconds()

		// fmt.Printf(" --- sort [%s] took %.2fms: unique : %d\n", bheader.DataType.String(), sortTook*1000, uniqueItems)

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
