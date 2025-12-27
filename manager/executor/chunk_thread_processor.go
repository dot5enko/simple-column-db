package executor

import (
	"fmt"
	"log/slog"
	"time"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/fatih/color"
)

func ChunkSingleThreadProcessor(threadId int, slabManager *meta.SlabManager, tasksQueue <-chan *ChunkProcessingTask) {

	threadCache := &executortypes.ChunkExecutorThreadCache{}

	slog.Info("worker started", "thread_id", threadId)
	defer slog.Info("worker stopped", "thread_id", threadId)

	for task := range tasksQueue {

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

		taskRes, err := ExecutePlanForChunk(threadCache, slabManager, task.Plan, task.Bchunk)
		if err != nil {
			curStatus.Err.Store(true)
			curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
		} else {

			processed := curStatus.ChunksProcessed.Add(1)

			processingTook := time.Since(start).Seconds() * 1000.0

			if false {
				slog.Info("chunk processing done ", "chunk_id", task.ChunkIdx, "took_ms", fmt.Sprintf("%.2f", processingTook))
			}

			func() {

				timeB := time.Now()

				curStatus.Lock.Lock()
				lockTook := time.Since(timeB)
				defer curStatus.Lock.Unlock()

				globalChunkResult := &curStatus.ChunkResult

				globalChunkResult.LockTook += lockTook
				globalChunkResult.TotalItems += taskRes.TotalItems
				globalChunkResult.WastedMerges += taskRes.WastedMerges
				globalChunkResult.SkippedBlocksDueToHeaderFiltering += taskRes.SkippedBlocksDueToHeaderFiltering
				globalChunkResult.ProcessedBlocks += taskRes.ProcessedBlocks
				globalChunkResult.FullSkips += taskRes.FullSkips
			}()

			if processed == int32(curStatus.ChunksTotal) {
				curStatus.Waiter.Done()
			}

		}
	}
}
