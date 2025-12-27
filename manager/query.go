package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dot5enko/simple-column-db/manager/executor"
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

	Metrics executor.ChunkFilterProcessResult

	Error error
}

func (sm *Manager) Query(
	schemaName string,
	queryData query.Query,
	ctx context.Context,
) (*QueryResult, error) {

	before := time.Now()
	result := &QueryResult{}

	// var indicesResultCache [schema.BlockRowsSize]uint16

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

	// discard non intersecting blocks from the plan

	if planErr != nil {
		return nil, fmt.Errorf("unable to construct query execution plan : %s", planErr.Error())
	}

	bChunksSize := len(plan.BlockChunks)

	taskStatus := &executor.TaskStatus{ChunksTotal: bChunksSize}
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
	timeBefore := time.Now()
	taskStatus.Waiter.Wait()
	waitTookMs := time.Since(timeBefore)

	queryTookMs := time.Since(before)

	cummResult := taskStatus.ChunkResult

	cummResult.PureLock = waitTookMs
	cummResult.TotalQueryDuration = time.Duration(queryTookMs)
	cummResult.PlanTook = planTime
	cummResult.TotalChunks = bChunksSize

	result.Metrics = cummResult

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
