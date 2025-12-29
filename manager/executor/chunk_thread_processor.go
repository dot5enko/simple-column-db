package executor

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/lightsync"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/fatih/color"
)

func ChunkSingleThreadProcessor(threadId int, slabManager *meta.SlabManager, tasksQueue *lightsync.RingQueue[ChunkProcessingTask]) {

	threadCache := &executortypes.ChunkExecutorThreadCache{}

	// runtime.LockOSThread()
	// defer runtime.UnlockOSThread()

	for {

		task, ok := tasksQueue.Pop()
		if !ok {
			// todo profile ?

			continue
		}

		curStatus := task.Status
		start := time.Now()

		if curStatus.Err.Load() {

			if curStatus.ErrObject == nil {
				panic("err object not set, but err flag is true")
			} else {
				color.Red("skipped because of error: %s", curStatus.ErrObject.Error())
			}
			continue
		}

		sManager := slabManager.NewSession()

		taskRes, err := ExecutePlanForChunk(threadCache, sManager, task.Plan, task.Bchunk)
		if err != nil {
			curStatus.Err.Store(true)
			curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
		} else {

			processed := curStatus.ChunksProcessed.Add(1)

			processingTook := time.Since(start).Seconds() * 1000.0

			if false {
				slog.Info("chunk processing done ", "chunk_id", task.ChunkIdx, "took_ms", fmt.Sprintf("%.2f", processingTook))
			}

			globalChunkResult := &curStatus.ChunkResult

			session := sManager.GetSession()

			atomic.AddInt64(&globalChunkResult.TotalItems, taskRes.TotalItems)
			atomic.AddInt64(&globalChunkResult.WastedMerges, taskRes.WastedMerges)
			atomic.AddInt64(&globalChunkResult.SkippedBlocksDueToHeaderFiltering, taskRes.SkippedBlocksDueToHeaderFiltering)
			atomic.AddInt64(&globalChunkResult.ProcessedBlocks, taskRes.ProcessedBlocks)
			atomic.AddInt64(&globalChunkResult.FullSkips, taskRes.FullSkips)
			atomic.AddInt64(&globalChunkResult.IoTime, session.IoTime.Nanoseconds())

			// copy bitset to global result bitset
			// perform selectors according to query

			// for idx := range query.ExecutorChunkSizeBlocks {

			// 	blockFilterMask := &threadCache.AbsBlockMaps[idx]

			// 	if blockFilterMask.Merges() == task.Plan.FilterSize {
			// 	}
			// }

			if processed == int32(curStatus.ChunksTotal) {
				curStatus.Waiter.Done()
			}

		}
	}
}
