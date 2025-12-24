package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/manager/executor"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

func StartWorkerThreads(workerCount int, cb func(threadId int)) *sync.WaitGroup {

	swg := sync.WaitGroup{}
	swg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer func() {
				swg.Done()

				rec := recover()
				if rec != nil {
					slog.Error("executor panicked", "err", fmt.Sprintf("%v", rec))
				}
			}()

			cb(i)
		}()
	}

	return &swg
}

func (sm *Manager) Query(
	schemaName string,
	queryData query.Query,
	ctx context.Context,
) ([]map[string]any, error) {

	result := []map[string]any{}

	// var indicesResultCache [schema.BlockRowsSize]uint16

	schemaObject := sm.Meta.GetSchema(schemaName)
	if schemaObject == nil {
		return nil, fmt.Errorf("no such schema '%s'", schemaName)
	}

	plan, planErr := sm.Planner.Plan(schemaName, queryData, sm.Meta)

	if planErr != nil {
		return nil, fmt.Errorf("unable to construct query execution plan : %s", planErr.Error())
	}

	cummResult := executor.ChunkFilterProcessResult{}
	slog.Info("starting workers", "max_executors", sm.config.ExecutorsMaxConcurentThreads)

	type TaskStatus struct {
		ChunksTotal     int
		ChunksProcessed atomic.Int32

		Err atomic.Bool

		ChunkResult executor.ChunkFilterProcessResult

		Waiter *sync.WaitGroup
	}

	type ChunkProcessingTask struct {
		bchunk *query.BlockChunk
		slabs  *meta.SlabManager
		plan   *query.QueryPlan

		chunkIdx int

		status *TaskStatus
	}

	chunksQueue := make(chan *ChunkProcessingTask, 100)
	// responsesQueue := make(chan *executor.ChunkFilterProcessResult, 100)

	StartWorkerThreads(4, func(threadId int) {

		threadCache := &executor.ChunkExecutorThreadCache{}

		slog.Info("worker started", "thread_id", threadId)
		defer slog.Info("worker stopped", "thread_id", threadId)

		for task := range chunksQueue {

			if task.status.Err.Load() {
				continue
			}

			threadCache.Reset()

			taskRes, err := executor.ExecutePlanForChunk(threadCache, sm.Slabs, task.plan, task.bchunk)
			if err != nil {
				task.status.Err.Store(true)
			} else {

				processed := task.status.ChunksProcessed.Add(1)

				slog.Info("chunk DONE", "worker_id", threadId, "bchunk_idx", task.chunkIdx, "filtered_items", taskRes.TotalItems, "tasks_done", processed)

				if processed == int32(task.status.ChunksTotal) {
					task.status.Waiter.Done()
					slog.Info("finished processing all chunks")
				}

				// responsesQueue <- &taskRes
			}
		}
	})

	bChunksSize := len(plan.BlockChunks)

	queryWaitGroup := sync.WaitGroup{}
	queryWaitGroup.Add(1)

	for bChunkIdx := 0; bChunkIdx < bChunksSize; bChunkIdx++ {

		// cacheItem, uid := sm.exCacheManager.Get()

		// if cacheItem == nil {
		// 	return nil, fmt.Errorf("unable to acquire executor cache")
		// }

		// cacheItem.Reset()

		chunksQueue <- &ChunkProcessingTask{
			bchunk: &plan.BlockChunks[bChunkIdx],
			slabs:  sm.Slabs,
			plan:   &plan,

			chunkIdx: bChunkIdx,

			status: &TaskStatus{ChunksTotal: bChunksSize, Waiter: &queryWaitGroup},
		}

		slog.Info("submitted chunk to processors", "chunk_idx", bChunkIdx)

		// data, chunkErr := executor.ExecutePlanForChunk(cacheItem, sm.Slabs, &plan, &plan.BlockChunks[bChunkIdx])
		// if chunkErr != nil {
		// 	return nil, fmt.Errorf("error while executing plan chunk: %s", chunkErr.Error())
		// }

		// cummResult.TotalItems += data.TotalItems
		// cummResult.WastedMerges += data.WastedMerges

		// sm.exCacheManager.Release(uid)

	}

	slog.Info("waiting for tasks completion")

	queryWaitGroup.Wait()

	slog.Info("merge info", "wasted_merges", cummResult.WastedMerges, "skipped_blocks", cummResult.SkippedBlocksDueToHeaderFiltering, "total_filtered", cummResult.TotalItems)

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
