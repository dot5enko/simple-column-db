package executor

import (
	"fmt"
	"log/slog"
	"time"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

type ChunkFilterProcessResult struct {
	SkippedBlocksDueToHeaderFiltering int64
	ProcessedBlocks                   int64
	FullSkips                         int64

	TotalItems   int64
	WastedMerges int64

	LockTook           int64
	PlanTook           int64
	PureLock           int64
	TotalQueryDuration int64

	IoTime int64

	TotalChunks int64
}

func preloadSlabHeaders(slabs *meta.SlabManager, plan *query.QueryPlan, blockChunk *query.BlockChunk) error {

	schemaObject := plan.Schema

	preloadingStart := time.Now()
	for _, filtersGroup := range plan.FilterGroupedByFields {
		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]
		for _, segment := range blockSegments {
			_, err := slabs.LoadSlabHeaderToCache(&schemaObject, segment.Slab)
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

func ExecutePlanForChunk(
	cache *executortypes.ChunkExecutorThreadCache,
	sm *meta.SlabManager,
	plan *query.QueryPlan,
	blockChunk *query.BlockChunk,
) (ChunkFilterProcessResult, error) {

	cache.Reset()

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

			// todo use circular buffer per thread?
			FilterColumnRuntimeCache: make([]query.RuntimeFilterCache, len(filtersGroup.Conditions)),

			FilterSize: filtersSize,

			Blocks:       cache.Blocks[:],
			AbsBlockMaps: cache.AbsBlockMaps[:],

			CurrentBlockProcessingIdx: 0,
		}

		// preprocess segments into blocks
		blocksPreprocessErr := preprocessSegmentsIntoBlocksAndHeaderFilter(sm, &slabMergerContext, blockSegments)
		if blocksPreprocessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("unable to preprocess blocks from segments: %s", blocksPreprocessErr.Error())
		}

		singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(&slabMergerContext, cache.IndicesResultCache[:])
		if chunkProcessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("chunk processing failed : %s", chunkProcessErr.Error())
		} else {
			result.SkippedBlocksDueToHeaderFiltering += int64(singleColumnProcessResult.skippedBlocksDueToHeaderFiltering)
			result.ProcessedBlocks += int64(singleColumnProcessResult.processedBlocks)
			result.FullSkips += int64(singleColumnProcessResult.fullSkips)

			// slog.Info("single column processing done", "skipped", singleColumnProcessResult.skippedBlocksDueToHeaderFiltering, "processed", singleColumnProcessResult.processedBlocks, "total_processed", result.ProcessedBlocks, "block_offset", blockChunk.GlobalBlockOffset)

		}
	}

	totalItems := 0
	wastedMerges := 0

	// filter merged blocks info
	for idx := range query.ExecutorChunkSizeBlocks {

		blockFilterMask := &cache.AbsBlockMaps[idx]

		if blockFilterMask.Merges() == plan.FilterSize {
			amount := blockFilterMask.ResultBitset.Count()
			totalItems += amount

		} else {
			wastedMerges += blockFilterMask.Merges()
		}
	}

	result.TotalItems = int64(totalItems)
	result.WastedMerges = int64(wastedMerges)

	return result, nil
}
