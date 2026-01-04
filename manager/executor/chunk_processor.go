package executor

import (
	"fmt"
	"log"
	"log/slog"
	"runtime"
	"time"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops/generated"
	"github.com/dot5enko/simple-column-db/schema"
)

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

func allocsDetection() func() {
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	it0 := mstats

	return func() {

		var mstats2 runtime.MemStats

		runtime.ReadMemStats(&mstats2)

		it := mstats2

		mallocs := it.Mallocs - it0.Mallocs
		// frees := it.Frees - it0.Frees

		bytesSize := 0

		if mallocs > 0 {

			log.Printf(" ===== %d Mallocs", mallocs)

			for i := range it.BySize { // AllocObjects is the number of allocated objects.
				has := it.BySize[i].Mallocs - it0.BySize[i].Mallocs
				if has > 0 {
					bytesSize = int(it.BySize[i].Size)
					slog.Info(" \t+++ mem stats", "alloc_size", bytesSize, "count", has)
				}
			}

		}
	}
}

func ExecutePlanForChunk(
	cache *executortypes.ChunkExecutorThreadCache,
	sm *meta.SlabManager,
	plan *query.QueryPlan,
	blockChunk *query.BlockChunk,
	slabMergerContext *executortypes.BlockMergerContext,
	result *executortypes.ChunkFilterProcessResult,
) error {

	cache.Reset()

	// defer allocsDetection()()

	// could be parallelized
	// but synchronization is needed which could be less effective
	// than chunk process parallelization

	// per field/slab processing
	slabMergerContext.Schema = plan.Schema
	slabMergerContext.AbsOffsetStart = blockChunk.GlobalBlockOffset

	for _, filtersGroup := range plan.FilterGroupedByFields {

		filtersSize := len(filtersGroup.Conditions)

		// check if conds
		if filtersSize > executortypes.MaxFiltersPerField {
			return fmt.Errorf("too many filters (%d), max %d", filtersSize, executortypes.MaxFiltersPerField)
		}

		filterColumnRTCache := cache.FilterCache[:filtersSize]

		// cleanup filter cache
		for filterCacheIdx := range filtersSize {
			r := &filterColumnRTCache[filterCacheIdx]
			r.FilterLastBlockHeaderResult = schema.UnknownIntersection
			r.FilterBounds.Deinit()
		}

		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]

		// filters applied to single column
		slabMergerContext.FilterColumn = filtersGroup.Conditions
		slabMergerContext.FilterColumnRuntimeCache = filterColumnRTCache
		slabMergerContext.FilterSize = filtersSize
		slabMergerContext.CurrentBlockProcessingIdx = 0

		// preprocess segments into blocks for column/slab
		blocksPreprocessErr := preprocessSegmentsIntoBlocksAndHeaderFilter(sm, slabMergerContext, blockSegments)
		if blocksPreprocessErr != nil {
			return fmt.Errorf("unable to preprocess blocks from segments: %s", blocksPreprocessErr.Error())
		}

		groupType := filtersGroup.ColumnSchemaInfo.Type
		singleColumnProcessResult, chunkProcessErr := generated.ChunkBlockProcessorSpecificFilterAndType(groupType, slabMergerContext)

		// singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(&slabMergerContext)
		if chunkProcessErr != nil {
			return fmt.Errorf("chunk processing failed : %s", chunkProcessErr.Error())
		} else {

			result.ProcessedBlocks += int64(singleColumnProcessResult.ProcessedBlocks)
			result.SkippedBlocksDueToHeaderFiltering += int64(singleColumnProcessResult.SkippedBlocksDueToHeaderFiltering)
			result.FullSkips += int64(singleColumnProcessResult.FullSkips)

			// slog.Info("chunk done", "idx", blockChunk.GlobalBlockOffset, "col", filtersGroup.ColumnSchemaInfo.Name, "skipped", singleColumnProcessResult.SkippedBlocksDueToHeaderFiltering, "processed", singleColumnProcessResult.ProcessedBlocks)
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

	return nil
}
