package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dot5enko/simple-column-db/manager/executor"
	"github.com/fatih/color"
)

// todo handle context
func (sm *Manager) StartWorkers(routines int, ctx context.Context) *sync.WaitGroup {

	slog.Info("starting workers", "max_executors", routines)

	return StartWorkerThreads(routines, func(threadId int) {

		threadCache := &executor.ChunkExecutorThreadCache{}

		slog.Info("worker started", "thread_id", threadId)
		defer slog.Info("worker stopped", "thread_id", threadId)

		for task := range sm.chunksQueue {

			curStatus := task.Status

			if curStatus.Err.Load() {

				if curStatus.ErrObject == nil {
					panic("err object not set, but err flag is true")
				} else {
					color.Red("skipped because of error: %s", curStatus.ErrObject.Error())
				}
				continue
			}

			threadCache.Reset()

			taskRes, err := executor.ExecutePlanForChunk(threadCache, sm.Slabs, task.Plan, task.Bchunk)
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

					globalChunkResult.ProcessedChunks += 1
				}()

				processed := curStatus.ChunksProcessed.Add(1)

				if processed == int32(curStatus.ChunksTotal) {
					curStatus.Waiter.Done()
				}

			}
		}
	})
}
