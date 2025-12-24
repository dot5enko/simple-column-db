package executor

import (
	"fmt"
	"log/slog"

	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/fatih/color"
)

func ChunkSingleThreadProcessor(threadId int, sm *meta.SlabManager, tasksQueue <-chan *ChunkProcessingTask) {

	threadCache := &ChunkExecutorThreadCache{}

	slog.Info("worker started", "thread_id", threadId)
	defer slog.Info("worker stopped", "thread_id", threadId)

	for task := range tasksQueue {

		curStatus := task.Status

		if curStatus.Err.Load() {

			if curStatus.ErrObject == nil {
				panic("err object not set, but err flag is true")
			} else {
				color.Red("skipped because of error: %s", curStatus.ErrObject.Error())
			}
			continue
		}

		taskRes, err := ExecutePlanForChunk(threadCache, sm, task.Plan, task.Bchunk)
		if err != nil {
			curStatus.Err.Store(true)
			curStatus.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
		} else {

			func() {
				curStatus.Lock.Lock()
				defer curStatus.Lock.Unlock()

				globalChunkResult := &curStatus.ChunkResult

				globalChunkResult.TotalItems += taskRes.TotalItems
				globalChunkResult.WastedMerges += taskRes.WastedMerges
				globalChunkResult.SkippedBlocksDueToHeaderFiltering += taskRes.SkippedBlocksDueToHeaderFiltering
			}()

			processed := curStatus.ChunksProcessed.Add(1)

			if processed == int32(curStatus.ChunksTotal) {
				curStatus.Waiter.Done()
			}

		}
	}
}
