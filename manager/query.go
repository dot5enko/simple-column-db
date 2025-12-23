package manager

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/dot5enko/simple-column-db/manager/query"
)

func (sm *Manager) Query(
	schemaName string,
	queryData query.Query,
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

	cummResult := ChunkFilterProcessResult{}

	for _, blockChunk := range plan.BlockChunks {

		data, chunkErr := executePlanChunk(sm, &plan, blockChunk)
		if chunkErr != nil {
			return nil, fmt.Errorf("error while executing plan chunk: %s", chunkErr.Error())
		}

		cummResult.totalItems += data.totalItems
		cummResult.wastedMerges += data.wastedMerges

		// paralelize with https://pkg.go.dev/golang.org/x/sync/errgroup

		// for _, blockSegment := range blockSegments {

		// }

		// slog.Info("processing chunk", "blocks", len(blockChunk.SlabsByFields))
	}

	slog.Info("merge info", "wasted_merges", cummResult.wastedMerges, "skipped_blocks", cummResult.skippedBlocksDueToHeaderFiltering, "total_filtered", cummResult.totalItems)

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
