package executor

import (
	"fmt"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/executor/filters"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

func prepareBlockForMerger(
	mergerContext *executortypes.BlockMergerContext,

	slabInfo *schema.DiskSlabHeader,
	blockHeader *schema.DiskHeader,

	slabsManager *meta.SlabManager,
) (err error) {

	skipFilters := 0
	curRelativeBlockId := mergerContext.CurrentBlockProcessingIdx
	mergerContext.CurrentBlockProcessingIdx++

	for idx := range mergerContext.FilterColumn {

		filter := mergerContext.FilterColumn[idx]

		var processFilterErr error
		intersectType := schema.UnknownIntersection

		switch slabInfo.Type {
		case schema.Uint64FieldType:
			intersectType, processFilterErr = filters.ProcessFilterOnBounds[uint64](filter.Filter, &blockHeader.Bounds)
		case schema.Uint8FieldType:
			intersectType, processFilterErr = filters.ProcessFilterOnBounds[uint8](filter.Filter, &blockHeader.Bounds)
		case schema.Float32FieldType:
			intersectType, processFilterErr = filters.ProcessFilterOnBounds[float32](filter.Filter, &blockHeader.Bounds)
		case schema.Float64FieldType:
			intersectType, processFilterErr = filters.ProcessFilterOnBounds[float64](filter.Filter, &blockHeader.Bounds)
		default:
			return fmt.Errorf("unsupported type %v while filtering block headers", slabInfo.Type.String())
		}

		if processFilterErr != nil {
			return fmt.Errorf("error filter processing : %s", processFilterErr.Error())
		} else {

			skipSingleBlock := intersectType == schema.NoIntersection

			filterColumnCache := &mergerContext.FilterColumnRuntimeCache[idx]

			filterColumnCache.FilterLastBlockHeaderResult = intersectType
			filterColumnCache.FilterBounds = blockHeader.Bounds

			if skipSingleBlock {
				skipFilters++
			}
		}
	}

	fullSkipBlock := skipFilters == mergerContext.FilterSize

	blockRT := &mergerContext.Blocks[curRelativeBlockId]
	blockRT.BlockHeader = blockHeader
	// blockRT.SlabHeader = slabInfo

	// increase current block pointer

	if !fullSkipBlock {
		// todo fix
		// mergerContext.IoTime += time.Since(load)

		blockDecodedInfo, blockErr := slabsManager.LoadBlockToRuntimeBlockData(mergerContext.Schema, slabInfo, blockHeader.Uid)

		// log.Printf("--- loaded block %s: @ %p", blockHeader.Uid.String(), blockDecodedInfo.DataTypedArray)

		if blockErr != nil {
			return fmt.Errorf("unable to decode block : %s", blockErr.Error())
		}

		blockRT.Val = blockDecodedInfo
	} else {
		absBlockRTInfo := &mergerContext.AbsBlockMaps[curRelativeBlockId]

		// preallocated for each thread executor
		// check if works correctly
		absBlockRTInfo.Reset()
		absBlockRTInfo.SetFullSkip()
	}

	for filterIdx := range mergerContext.FilterColumn {
		refResult := &blockRT.HeaderFilterMatchResult[filterIdx]

		//
		refResult.MatchResult = mergerContext.FilterColumnRuntimeCache[filterIdx].FilterLastBlockHeaderResult
		refResult.Bounds = mergerContext.FilterColumnRuntimeCache[filterIdx].FilterBounds
	}

	return nil
}

func preprocessSegmentsIntoBlocksAndHeaderFilter(
	sm *meta.SlabManager,
	slabMergerContext *executortypes.BlockMergerContext,
	segments []query.Segment,
) error {

	segmentsLen := len(segments)

	for idx := range segmentsLen {

		segment := &segments[idx]

		slabBlockOffsetStart := segment.StartBlock

		slabInfo, slabErr := sm.LoadSlabHeaderToCache(&slabMergerContext.Schema, segment.Slab)
		if slabErr != nil {
			return fmt.Errorf("unable to load slab : %s", slabErr.Error())
		}

		blockHeaders := slabInfo.BlockHeaders

		// todo remove internal function call here
		// move whole loop into separate func

		for i := 0; i < int(segment.Size); i++ {
			idx := i + slabBlockOffsetStart

			if idx > int(slabInfo.BlocksFinalized) {
				break
			}

			blockHeader := &blockHeaders[idx]

			preparationErr := prepareBlockForMerger(slabMergerContext,
				slabInfo,
				blockHeader,
				sm,
			)
			if preparationErr != nil {

				return fmt.Errorf("unable to prepare block for merging : %s", preparationErr.Error())
			}
		}
	}

	return nil
}

func processFiltersOnPreparedBlocks(mCtx *executortypes.BlockMergerContext) (result executortypes.SingleColumnProcessingResult, topErr error) {

	// get slab bounds
	// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

	for blockRelativeIdx := range mCtx.CurrentBlockProcessingIdx {

		blockData := &mCtx.Blocks[blockRelativeIdx]

		blockGroupMerger := &mCtx.AbsBlockMaps[blockRelativeIdx]
		if blockGroupMerger.FullSkip() {

			result.FullSkips += 1

			continue
		}

		// slog.Info("processing block OK", "block_relative_idx", blockRelativeIdx, "block_data_is_nil", blockData.Val == nil)
		blockDataType := blockData.BlockHeader.DataType

		var filteredSize int

		for fIdx, filter := range mCtx.FilterColumn {

			headerMatchResultObj := blockData.HeaderFilterMatchResult[fIdx]
			headerMatchResult := headerMatchResultObj.MatchResult

			isFull := headerMatchResult == schema.FullIntersection

			if isFull {
				result.SkippedBlocksDueToHeaderFiltering += 1

				blockGroupMerger.WithBitset(nil, false, true)
				continue
			}

			result.ProcessedBlocks += 1

			{
				var processFilterErr error

				// slog.Info("processing filter", "filter_type", blockDataType.String())

				// process filter on a block
				switch blockDataType {
				case schema.Uint64FieldType:
					filteredSize, processFilterErr = filters.ProcessUnsignedFilterOnColumnWithType[uint64](filter.Filter, blockData, blockGroupMerger)
				case schema.Uint8FieldType:
					filteredSize, processFilterErr = filters.ProcessUnsignedFilterOnColumnWithType[uint8](filter.Filter, blockData, blockGroupMerger)
				case schema.Float32FieldType:
					filteredSize, processFilterErr = filters.ProcessFloatFilterOnColumnWithType[float32](filter.Filter, blockData, blockGroupMerger)
				case schema.Float64FieldType:
					filteredSize, processFilterErr = filters.ProcessFloatFilterOnColumnWithType[float64](filter.Filter, blockData, blockGroupMerger)
				default:
					return executortypes.SingleColumnProcessingResult{}, fmt.Errorf("unsupported type %v", blockDataType.String())
				}

				if processFilterErr != nil {
					return executortypes.SingleColumnProcessingResult{}, fmt.Errorf("error filter processing : %s. sum of bitset = %d, bitcount = %d", processFilterErr.Error(), blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
				}

				// slog.Info(" -- [filtered]", "filteredSize", filteredSize, "header_match_cached", headerMatchResult.String(), "arg", filter, "filter_bounds", headerMatchResultObj.Bounds)
			}
		}

		result.MatchedItems += filteredSize
	}

	return
}
