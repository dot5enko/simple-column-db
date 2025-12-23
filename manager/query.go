package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"

	"github.com/dot5enko/simple-column-db/manager/executor"
	"github.com/dot5enko/simple-column-db/manager/query"
	"golang.org/x/sync/errgroup"
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

	numberOfCpus := runtime.NumCPU()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numberOfCpus)

	for _, blockChunk := range plan.BlockChunks {

		g.Go(func() error {

			data, chunkErr := executePlanForChunk(sm, &plan, blockChunk)
			if chunkErr != nil {
				return fmt.Errorf("error while executing plan chunk: %s", chunkErr.Error())
			}

			cummResult.TotalItems += data.totalItems
			cummResult.WastedMerges += data.wastedMerges

			return nil
		})

		// paralelize with https://pkg.go.dev/golang.org/x/sync/errgroup

		// for _, blockSegment := range blockSegments {

		// }

		// slog.Info("processing chunk", "blocks", len(blockChunk.SlabsByFields))
	}

	slog.Info("merge info", "wasted_merges", cummResult.WastedMerges, "skipped_blocks", cummResult.SkippedBlocksDueToHeaderFiltering, "total_filtered", cummResult.TotalItems)

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
