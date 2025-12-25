package executor

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type PlanExecutor struct {
}

type BlockMergerContext struct {
	Schema         schema.Schema
	AbsOffsetStart uint64
	FilterColumn   []query.FilterConditionRuntime
	FilterSize     int

	Blocks                    []BlockRuntimeInfo
	CurrentBlockProcessingIdx int

	AbsBlockMaps []lists.IndiceUnmerged

	QueryPlan *query.QueryPlan
}

func prepareBlockForMerger(
	mergerContext *BlockMergerContext,

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
			intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint64](filter.Filter, blockHeader)
		case schema.Uint8FieldType:
			intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint8](filter.Filter, blockHeader)
		case schema.Float32FieldType:
			intersectType, processFilterErr = ProcessFilterOnBlockHeader[float32](filter.Filter, blockHeader)
		case schema.Float64FieldType:
			intersectType, processFilterErr = ProcessFilterOnBlockHeader[float64](filter.Filter, blockHeader)
		default:
			return fmt.Errorf("unsupported type %v while filtering block headers", slabInfo.Type.String())
		}

		if processFilterErr != nil {
			return fmt.Errorf("error filter processing : %s", processFilterErr.Error())
		} else {

			skipSingleBlock := intersectType == schema.NoIntersection

			filter.Runtime.FilterLastBlockHeaderResult = intersectType

			if skipSingleBlock {
				skipFilters++
			}
		}
	}

	fullSkipBlock := skipFilters == mergerContext.FilterSize

	blockRT := &mergerContext.Blocks[curRelativeBlockId]
	blockRT.BlockHeader = blockHeader
	blockRT.SlabHeader = slabInfo

	// increase current block pointer

	if !fullSkipBlock {
		// todo fix
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

	for filterIdx, filter := range mergerContext.FilterColumn {
		blockRT.HeaderFilterMatchResult[filterIdx] = filter.Runtime.FilterLastBlockHeaderResult
	}

	return nil
}

type SingleColumnProcessingResult struct {
	skippedBlocksDueToHeaderFiltering int
}

func preprocessSegmentsIntoBlocksAndHeaderFilter(
	sm *meta.SlabManager,
	slabMergerContext *BlockMergerContext,
	segments []query.Segment,
) error {

	for _, segment := range segments {

		slabBlockOffsetStart := segment.StartBlock

		slabInfo, slabErr := sm.LoadSlabToCache(slabMergerContext.Schema, segment.Slab)
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

func processFiltersOnPreparedBlocks(mCtx *BlockMergerContext, indicesResultCache []uint16) (result SingleColumnProcessingResult, topErr error) {

	// get slab bounds
	// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

	for blockRelativeIdx := range mCtx.CurrentBlockProcessingIdx {

		blockData := &mCtx.Blocks[blockRelativeIdx]

		blockGroupMerger := &mCtx.AbsBlockMaps[blockRelativeIdx]
		if blockGroupMerger.FullSkip() {
			continue
		}

		// slog.Info("processing block OK", "block_relative_idx", blockRelativeIdx, "block_data_is_nil", blockData.Val == nil)

		blockDataType := blockData.BlockHeader.DataType

		for fIdx, filter := range mCtx.FilterColumn {

			headerMatchResult := blockData.HeaderFilterMatchResult[fIdx]

			isFull := headerMatchResult == schema.FullIntersection

			if isFull {
				result.skippedBlocksDueToHeaderFiltering += 1

				blockGroupMerger.With(nil, false, true)
				continue
			}

			{
				var processFilterErr error
				var filteredSize int

				// process filter on a block
				switch blockDataType {
				case schema.Uint64FieldType:
					filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint64](filter.Filter, blockData, blockGroupMerger, indicesResultCache[:])
				case schema.Uint8FieldType:
					filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint8](filter.Filter, blockData, blockGroupMerger, indicesResultCache[:])
				case schema.Float32FieldType:
					filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float32](filter.Filter, blockData, blockGroupMerger, indicesResultCache[:])
				case schema.Float64FieldType:
					filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float64](filter.Filter, blockData, blockGroupMerger, indicesResultCache[:])
				default:
					return SingleColumnProcessingResult{}, fmt.Errorf("unsupported type %v", blockDataType.String())
				}

				_ = filteredSize

				if processFilterErr != nil {
					return SingleColumnProcessingResult{}, fmt.Errorf("error filter processing : %s. sum of bitset = %d, bitcount = %d", processFilterErr.Error(), blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
				}

				// log.Printf(" -- [filtered] filteredSize : %d. sum of bitset = %d, bitcount = %d", filteredSize, blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
			}
		}
	}

	return
}
