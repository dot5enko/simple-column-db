package executor

import (
	"fmt"
	"log/slog"
	"time"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
)

type ChunkFilterProcessResult struct {
	SkippedBlocksDueToHeaderFiltering int64
	ProcessedBlocks                   int64
	FullSkips                         int64

	TotalItems   int64
	WastedMerges int64

	LockTook int64
	PlanTook int64
	PureLock int64

	IoTime int64

	// set on query planner
	TotalChunks        int64
	TotalQueryDuration time.Duration
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

	// runtime.LockOSThread()
	// defer runtime.UnlockOSThread()

	cache.Reset()

	result := ChunkFilterProcessResult{}

	// could be parallelized
	// but synchronization is needed which could be less effective
	// than chunk process parallelization

	// per field/slab processing
	for _, filtersGroup := range plan.FilterGroupedByFields {

		conds := len(filtersGroup.Conditions)
		filterColumnRTCache := cache.FilterCache[:conds]

		// cleanup filter cache
		for filterCacheIdx := range conds {
			r := &filterColumnRTCache[filterCacheIdx]
			r.FilterLastBlockHeaderResult = schema.UnknownIntersection
			r.FilterBounds.Deinit()
		}

		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]
		filtersSize := len(filtersGroup.Conditions)

		slabMergerContext := BlockMergerContext{
			Schema:         plan.Schema,
			AbsOffsetStart: blockChunk.GlobalBlockOffset,

			// filters applied to single column
			FilterColumn: filtersGroup.Conditions,

			// todo use circular buffer per thread?
			FilterColumnRuntimeCache: filterColumnRTCache,

			FilterSize: filtersSize,

			Blocks:       cache.Blocks[:],
			AbsBlockMaps: cache.AbsBlockMaps[:],

			CurrentBlockProcessingIdx: 0,
		}

		// preprocess segments into blocks for column/slab
		blocksPreprocessErr := preprocessSegmentsIntoBlocksAndHeaderFilter(sm, &slabMergerContext, blockSegments)
		if blocksPreprocessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("unable to preprocess blocks from segments: %s", blocksPreprocessErr.Error())
		}

		groupType := filtersGroup.ColumnSchemaInfo.Type

		var singleColumnProcessResult SingleColumnProcessingResult
		var chunkProcessErr error

		switch groupType { // switch on field type
		case schema.Uint16FieldType:
			singleColumnProcessResult, chunkProcessErr = ProcessFiltersOnChunkOfBlocksUnsigned[uint16](&slabMergerContext, &result)
		case schema.Uint32FieldType:
			singleColumnProcessResult, chunkProcessErr = ProcessFiltersOnChunkOfBlocksUnsigned[uint32](&slabMergerContext, &result)
		case schema.Uint64FieldType:
			singleColumnProcessResult, chunkProcessErr = ProcessFiltersOnChunkOfBlocksUnsigned[uint64](&slabMergerContext, &result)
		case schema.Uint8FieldType:
			singleColumnProcessResult, chunkProcessErr = ProcessFiltersOnChunkOfBlocksUnsigned[uint8](&slabMergerContext, &result)
		default:
			return ChunkFilterProcessResult{}, fmt.Errorf("unsupported field type while prcessing filters on chunk of blocks : %s", groupType.String())
		}

		result.SkippedBlocksDueToHeaderFiltering += int64(singleColumnProcessResult.skippedBlocksDueToHeaderFiltering)
		result.ProcessedBlocks += int64(singleColumnProcessResult.processedBlocks)
		result.FullSkips += int64(singleColumnProcessResult.fullSkips)

		singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(&slabMergerContext)
		if chunkProcessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("chunk processing failed : %s", chunkProcessErr.Error())
		} else {

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

func ProcessFiltersOnChunkOfBlocksUnsigned[T ops.UnsignedInts](slabMergerContext *BlockMergerContext) (result SingleColumnProcessingResult, topErr error) {

	return

	/*for fIdx, _filter := range slabMergerContext.FilterColumn {

		filterType := _filter.Filter.Operand
		filter := &_filter.Filter

		switch filterType {
		case query.RANGE:
			operandA := filter.Arguments[0].(T)
			operandB := filter.Arguments[1].(T)

			if operandA > operandB {
				temp := operandB
				operandB = operandA
				operandA = temp

			}

			itemsFiltered = ops.CompareValuesAreInRangeUnsignedIntsBitsetFast(inputArray, operandA, operandB, &outputBitset)
			// log.Printf(" end of input array offset : %v", arrayEndOffset)
		case query.EQ:
			operand := filter.Arguments[0].(T)

			itemsFiltered = ops.CompareNumericValuesAreEqualBitset(inputArray, operand, &outputBitset)

		case query.GT:
			operand := filter.Arguments[0].(T)

			itemsFiltered = ops.CompareValuesAreBiggerBitset(inputArray, operand, &outputBitset)
		case query.LT:
			operand := filter.Arguments[0].(T)

			itemsFiltered = ops.CompareValuesAreSmallerBitset(inputArray, operand, &outputBitset)

		default:
			return itemsFiltered, fmt.Errorf("unsupported operand type=%s while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand.String(), blockData.BlockHeader.DataType.String())
		}

	}
	return result, nil
	*/

}

/*
	for blockRelativeIdx := range slabMergerContext.CurrentBlockProcessingIdx {

			blockGroupMerger := &slabMergerContext.AbsBlockMaps[blockRelativeIdx]
			if blockGroupMerger.FullSkip() {
				result.FullSkips += 1
				continue
			}

			blockData := &slabMergerContext.Blocks[blockRelativeIdx]

			headerMatchResultObj := blockData.HeaderFilterMatchResult[fIdx]
			headerMatchResult := headerMatchResultObj.MatchResult

			isFull := headerMatchResult == schema.FullIntersection

			if isFull {
				result.SkippedBlocksDueToHeaderFiltering += 1

				// blockGroupMerger.WithBitset(nil, false, true)
				continue
			}

			result.ProcessedBlocks += 1

			{
				_, processFilterErr := filters.ProcessUnsignedFilterOnColumnWithType[uint64](filter.Filter, blockData, blockGroupMerger)

				if processFilterErr != nil {
					return fmt.Errorf("error filter processing : %s. sum of bitset = %d, bitcount = %d", processFilterErr.Error(), blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
				}

				// slog.Info(" -- [filtered]", "filteredSize", filteredSize, "header_match_cached", headerMatchResult.String(), "arg", filter, "filter_bounds", headerMatchResultObj.Bounds)
			}

			// slog.Info("perform filter on whole slab", "slab_type", groupType, "operand", filter.Filter.Operand.String(), "block", blockRelativeIdx)
		}*/
