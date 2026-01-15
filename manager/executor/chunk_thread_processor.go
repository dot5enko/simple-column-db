package executor

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/fatih/color"
)

var QueueSizeNow atomic.Int32

func ChunkSingleThreadProcessor(threadId int, slabManager *meta.SlabManager, tasksQueue <-chan *ChunkProcessingTask) {

	// per worker local cache
	threadCache := &executortypes.ChunkExecutorThreadCache{
		ThreadIdx: threadId,
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
				curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
			}

			if processed == int32(curStatus.ChunksTotal) {
				globalChunkResult.TotalQueryDuration = time.Since(task.Status.StartTime)
				curStatus.Waiter.Done()

				// nowSize := QueueSizeNow.Add(-1)
				// log.Printf("queue size now: %d\n", nowSize)qi
			}
		}
	}
}

// color.Green("<query=%d/chunk%d>  total_items=%d, memory_usage=%.3f", plan.Id, blockChunk.GlobalBlockOffset, chunkItemsFiltered, usageMb)
