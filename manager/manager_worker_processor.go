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

			if task.Status.Err.Load() {

				if task.Status.ErrObject == nil {
					panic("err object not set, but err flag is true")
				} else {
					color.Red("skipped because of error: %s", task.Status.ErrObject.Error())
				}
				continue
			}

			threadCache.Reset()

			taskRes, err := executor.ExecutePlanForChunk(threadCache, sm.Slabs, task.Plan, task.Bchunk)
			if err != nil {
				task.Status.Err.Store(true)
				task.Status.ErrObject = fmt.Errorf("error while executing plan chunk: %s", err.Error())
			} else {

				func() {
					task.Status.Lock.Lock()
					defer task.Status.Lock.Unlock()

					task.Status.ChunkResult.TotalItems += taskRes.TotalItems
					task.Status.ChunkResult.WastedMerges += taskRes.WastedMerges
					task.Status.ChunkResult.SkippedBlocksDueToHeaderFiltering += taskRes.SkippedBlocksDueToHeaderFiltering

				}()

				processed := task.Status.ChunksProcessed.Add(1)

				if processed == int32(task.Status.ChunksTotal) {
					task.Status.Waiter.Done()
				}

			}
		}
	})
}
