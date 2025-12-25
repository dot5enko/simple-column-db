package executor

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

type ChunkFilterProcessResult struct {
	SkippedBlocksDueToHeaderFiltering int

	TotalItems   int
	WastedMerges int
}

func preloadChunks(slabs *meta.SlabManager, plan *query.QueryPlan, blockChunk *query.BlockChunk) error {

	schemaObject := plan.Schema

	preloadingStart := time.Now()
	for _, filtersGroup := range plan.FilterGroupedByFields {
		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]
		for _, segment := range blockSegments {
			_, err := slabs.LoadSlabToCache(schemaObject, segment.Slab)
			if err != nil {
				return fmt.Errorf("unable to load slab : %s", err.Error())
			}
		}
	}
	preloadingTook := time.Since(preloadingStart).Seconds() * 1000

	if preloadingTook > 10 {
		slog.Info("slow slabs preloading for chunk executor", "took", preloadingTook)
	}

	return nil
}

func ExecutePlanForChunk(cache *ChunkExecutorThreadCache, sm *meta.SlabManager, plan *query.QueryPlan, blockChunk *query.BlockChunk) (ChunkFilterProcessResult, error) {

	cache.Reset()

	// preload all slabs that are in the chunk
	// preloadErr := preloadChunks(sm, plan, blockChunk)
	// if preloadErr != nil {
	// 	return ChunkFilterProcessResult{}, fmt.Errorf("unable to preload chunks : %s", preloadErr.Error())
	// }

	// per field/slab processing
	//
	// could be parallelized
	// but synchronization is needed which could be less effective
	// than chunk process parallelization

	result := ChunkFilterProcessResult{}

	for _, filtersGroup := range plan.FilterGroupedByFields {

		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]

		filtersSize := len(filtersGroup.Conditions)

		slabMergerContext := BlockMergerContext{
			Schema:         plan.Schema,
			AbsOffsetStart: blockChunk.GlobalBlockOffset,

			// filters applied to single column
			FilterColumn: filtersGroup.Conditions,
			FilterSize:   filtersSize,

			Blocks:       cache.blocks[:],
			AbsBlockMaps: cache.absBlockMaps[:],

			CurrentBlockProcessingIdx: 0,
		}

		// preprocess segments into blocks
		blocksPreprocessErr := preprocessSegmentsIntoBlocksAndHeaderFilter(sm, &slabMergerContext, blockSegments)
		if blocksPreprocessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("unable to preprocess blocks from segments: %s", blocksPreprocessErr.Error())
		}

		singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(&slabMergerContext, cache.indicesResultCache[:])
		if chunkProcessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("chunk processing failed : %s", chunkProcessErr.Error())
		} else {
			result.SkippedBlocksDueToHeaderFiltering += singleColumnProcessResult.skippedBlocksDueToHeaderFiltering
		}
	}

	totalItems := 0
	wastedMerges := 0

	// filter merged blocks info
	for idx := range query.ExecutorChunkSizeBlocks {

		blockFilterMask := &cache.absBlockMaps[idx]

		if blockFilterMask.Merges() == plan.FilterSize {
			amount := blockFilterMask.ResultBitset.Count()
			totalItems += amount

		} else {
			wastedMerges += blockFilterMask.Merges()
		}
	}

	result.TotalItems = totalItems
	result.WastedMerges = wastedMerges

	// todo cleanup
	// absBlockMaps

	return result, nil
}
