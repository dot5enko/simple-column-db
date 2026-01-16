package executor

import (
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/manager/cache"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

var QueueSizeNow atomic.Int32

func ChunkSingleThreadProcessor(threadId int, slabManager *meta.SlabManager, tasksQueue <-chan *ChunkProcessingTask) {

	// per worker local cache
	threadCache := &executortypes.ChunkExecutorThreadCache{
		ThreadIdx:        threadId,
		FilterApplyCache: make(map[schema.FilterIdType]map[schema.BlockUniqueId]*executortypes.BlockScanFilterResultCache),
	}

	threadSlabManagerSession := slabManager.NewSession(threadCache)

	slabMergerContext := &executortypes.BlockMergerContext{
		Blocks:       threadCache.Blocks[:],
		AbsBlockMaps: threadCache.AbsBlockMaps[:],
	}

	for task := range tasksQueue {

		// color.Red(" chunk processed [%d] x %d blocks", task.ChunkIdx, query.ExecutorChunkSizeBlocks)

		curStatus := task.Status

		processed := curStatus.ChunksProcessed.Add(1)

		if processed == 1 {
			curStatus.QueueTime = time.Since(curStatus.StartTime)
		}

		start := time.Now()

		if curStatus.Err.Load() {

			if curStatus.ErrObject == nil {
				panic("err object not set, but err flag is true")
			} else {
				color.Red("skipped because of error: %s", curStatus.ErrObject.Error())
			}
			return
		}

		threadSlabManagerSession.GetSession().IoTime = 0

		taskRes := &executortypes.ChunkFilterProcessResult{}

		err := filterDataOnChunk(
			threadCache,
			threadSlabManagerSession,
			task.Plan,
			task.Bchunk,
			slabMergerContext,
			taskRes,
		)
		if err != nil {
			curStatus.Err.Store(true)
			curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
		} else {

			processingTook := time.Since(start).Seconds() * 1000.0

			if false {
				slog.Info("chunk processing done ", "chunk_id", task.ChunkIdx, "took_ms", processingTook, "io_time", taskRes.IoTime)
			}

			globalChunkResult := &curStatus.ChunkResult

			session := threadSlabManagerSession.GetSession()

			atomic.AddInt64(&globalChunkResult.TotalItems, taskRes.TotalItems)
			atomic.AddInt64(&globalChunkResult.WastedMerges, taskRes.WastedMerges)
			atomic.AddInt64(&globalChunkResult.SkippedBlocksDueToHeaderFiltering, taskRes.SkippedBlocksDueToHeaderFiltering)
			atomic.AddInt64(&globalChunkResult.ProcessedBlocks, taskRes.ProcessedBlocks)
			atomic.AddInt64(&globalChunkResult.FullSkips, taskRes.FullSkips)
			atomic.AddInt64(&globalChunkResult.IoTime, session.IoTime.Nanoseconds())
			atomic.AddInt64(&globalChunkResult.TookSelectors, taskRes.TookSelectors)

			selectorsProcessingErr := processSelectorsOnChunk(
				threadCache,
				threadSlabManagerSession,
				task.Plan,
				task.Bchunk,
				slabMergerContext,
				taskRes,
			)

			if selectorsProcessingErr != nil {
				curStatus.Err.Store(true)
				curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", selectorsProcessingErr.Error())
			}

			for _, item := range taskRes.SelectorBuffers[:len(task.Plan.SelectorsGroupedByFields)] {
				switch cacheHandler := item.BufferHandler.(type) {

				case *cache.MbCacheItem:
					threadSlabManagerSession.GetRuntimeCache().MegabyteCache.Return(cacheHandler)
				case *cache.ThreeMbCacheItem:
					threadSlabManagerSession.GetRuntimeCache().ThreeMegabyteCache.Return(cacheHandler)
				case *cache.SlabDataCacheItem:
					threadSlabManagerSession.GetRuntimeCache().SlabRuntimeCache.Return(cacheHandler)
				default:
					slog.Error("unknown buffer handler type", "type", fmt.Sprintf("%T", cacheHandler))
				}
			}

			if processed == int32(curStatus.ChunksTotal) {
				globalChunkResult.TotalQueryDuration = time.Since(task.Status.StartTime)
				curStatus.Waiter.Done()

				// cleanup chunks buffers

				// nowSize := QueueSizeNow.Add(-1)
				// log.Printf("queue size now: %d\n", nowSize)qi
			}
		}
	}

	// check thread effectivity

	slog.Info("thread cache info", "thread_id", threadId, "filters_cached", len(threadCache.FilterApplyCache))

	readStats := []int{}

	for filterId, it := range threadCache.FilterApplyCache {
		// slog.Info("thread filder info", "thread_id", threadId, "filter_id", string(filterId[:20]), "blocks", len(it))
		_ = filterId

		for _, jval := range it {
			readStats = append(readStats, jval.Reads)
		}
	}

	slices.Sort(readStats)

	// top k usages

	topK := 5
	itemsLength := len(readStats)

	if itemsLength < topK {
		topK = itemsLength
	}


	p50val := 

	for i := 0; i < topK; i += 1 {

		idx := itemsLength - topK + i
		curValue := readStats[idx]
		log.Printf(" -- max usage of block(%d out of %d): %d times", idx, itemsLength, curValue)
	}

}

// color.Green("<query=%d/chunk%d>  total_items=%d, memory_usage=%.3f", plan.Id, blockChunk.GlobalBlockOffset, chunkItemsFiltered, usageMb)
