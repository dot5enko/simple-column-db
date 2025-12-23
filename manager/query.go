package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dot5enko/simple-column-db/manager/executor"
	"github.com/dot5enko/simple-column-db/manager/query"
)

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

	for _, blockChunk := range plan.BlockChunks {

		cacheItem, uid := sm.exCacheManager.Get()

		if cacheItem == nil {
			return nil, fmt.Errorf("unable to acquire executor cache")
		}

		func() error {

			defer sm.exCacheManager.Release(uid)

			cacheItem := &executor.ChunkExecutorThreadCache{}

			data, chunkErr := executor.ExecutePlanForChunk(cacheItem, sm.Slabs, &plan, blockChunk)
			if chunkErr != nil {
				return fmt.Errorf("error while executing plan chunk: %s", chunkErr.Error())
			}

			// resultLock.Lock()
			// defer resultLock.Unlock()

			cummResult.TotalItems += data.TotalItems
			cummResult.WastedMerges += data.WastedMerges

			return nil
		}()
	}

	slog.Info("merge info", "wasted_merges", cummResult.WastedMerges, "skipped_blocks", cummResult.SkippedBlocksDueToHeaderFiltering, "total_filtered", cummResult.TotalItems)

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
