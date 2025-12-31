package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dot5enko/simple-column-db/manager/executor"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
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

					panic(rec)

					slog.Error("executor panicked", "err", fmt.Sprintf("%v", rec))
				}
			}()

			cb(i)
		}()
	}

	return &swg
}

type QueryResult struct {
	Data map[string][]any

	finalized bool
	metrics   executortypes.ChunkFilterProcessResult

	task *executor.TaskStatus

	Error error
}

func (q *QueryResult) GetMetrics() executortypes.ChunkFilterProcessResult {

	if !q.finalized {
		q.Wait()
	}

	return q.metrics
}

func (q *QueryResult) Wait() {

	taskStatus := q.task

	timeBefore := time.Now()
	taskStatus.Waiter.Wait()
	waitTookMs := time.Since(timeBefore)

	cummResult := taskStatus.ChunkResult

	cummResult.PureLock = waitTookMs.Nanoseconds()

	q.finalized = true
	q.metrics = cummResult
}

func (sm *Manager) Query(
	schemaName string,
	queryData query.Query,
	ctx context.Context,
) (*QueryResult, error) {

	before := time.Now()
	result := &QueryResult{}

	schemaObject := sm.Meta.GetSchema(schemaName)
	if schemaObject == nil {
		return nil, fmt.Errorf("no such schema '%s'", schemaName)
	}

	plan, planErr := sm.Planner.Plan(
		schemaName, queryData,
		sm.Meta,
		sm.Slabs,
		&sm.queryOptions,
	)

	planTime := time.Since(before)

	if planErr != nil {
		return nil, fmt.Errorf("unable to construct query execution plan : %s", planErr.Error())
	}

	bChunksSize := len(plan.BlockChunks)

	taskStatus := &executor.TaskStatus{
		StartTime:   time.Now(),
		ChunksTotal: bChunksSize,
	}
	taskStatus.ChunkResult.PlanTook = planTime.Nanoseconds()
	taskStatus.ChunkResult.TotalChunks = int64(bChunksSize)

	// taskStatus.Waiter.SetSleepStep(time.Microsecond * 500)
	taskStatus.Waiter.Add(1)

	for bChunkIdx := 0; bChunkIdx < bChunksSize; bChunkIdx++ {
		sm.chunksQueue <- &executor.ChunkProcessingTask{
			Bchunk: &plan.BlockChunks[bChunkIdx],
			Slabs:  sm.Slabs,
			Plan:   &plan,

			ChunkIdx: bChunkIdx,

			Status: taskStatus,
		}
	}

	result.task = taskStatus

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
