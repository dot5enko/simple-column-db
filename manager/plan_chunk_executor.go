package manager

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type PlanExecutor struct {
}

func preloadChunks(sm *Manager, plan *query.QueryPlan, blockChunk query.BlockChunk) error {

	schemaObject := plan.Schema

	preloadingStart := time.Now()
	for _, filtersGroup := range plan.FilterGroupedByFields {
		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]
		for _, segment := range blockSegments {
			_, err := sm.Slabs.LoadSlabToCache(schemaObject, segment.Slab)
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

	slabsManager *SlabManager,
) (err error) {

	skipFilters := 0
	curRelativeBlockId := mergerContext.CurrentBlockProcessingIdx
	mergerContext.CurrentBlockProcessingIdx++

	for _, filter := range mergerContext.FilterColumn {

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
			// here, so we override old data for sure
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

type ChunkFilterProcessResult struct {
	skippedBlocksDueToHeaderFiltering int

	totalItems   int
	wastedMerges int
}

type SingleColumnProcessingResult struct {
	skippedBlocksDueToHeaderFiltering int
}

func preprocessSegmentsIntoBlocksAndHeaderFilter(
	sm *Manager,
	slabMergerContext *BlockMergerContext,
	segments []query.Segment,
) error {

	for _, segment := range segments {

		slabBlockOffsetStart := segment.StartBlock

		slabInfo, slabErr := sm.Slabs.LoadSlabToCache(slabMergerContext.Schema, segment.Slab)
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
				&sm.Slabs,
			)
			if preparationErr != nil {
				return fmt.Errorf("unable to prepare block for merging : %s", preparationErr.Error())
			}
		}
	}

	return nil
}

func processFiltersOnPreparedBlocks(mCtx *BlockMergerContext, blocks []BlockRuntimeInfo, indicesResultCache []uint16) (result SingleColumnProcessingResult, topErr error) {

	// get slab bounds
	// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

	for blockRelativeIdx := range query.ExecutorChunkSizeBlocks {

		blockData := &blocks[blockRelativeIdx]

		blockGroupMerger := &mCtx.AbsBlockMaps[blockRelativeIdx]
		{
			if blockGroupMerger.FullSkip() {
				continue
			}
		}

		blockDataType := blockData.BlockHeader.DataType
		// slabInfo := blockData.SlabHeader

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

// todo should result something
func executePlanChunk(sm *Manager, plan *query.QueryPlan, blockChunk query.BlockChunk) (ChunkFilterProcessResult, error) {

	// preallocate per executor thread

	// global for all fields/slabs
	absBlockMaps := [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged{}

	// local per column
	blocks := [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo{}
	indicesResultCache := [schema.BlockRowsSize]uint16{}

	// preload all slabs that are in the chunk
	preloadErr := preloadChunks(sm, plan, blockChunk)
	if preloadErr != nil {
		return ChunkFilterProcessResult{}, fmt.Errorf("unable to preload chunks : %s", preloadErr.Error())
	}

	schemaObject := plan.Schema

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
			Schema:         schemaObject,
			AbsOffsetStart: blockChunk.GlobalBlockOffset,

			// filters applied to single column
			FilterColumn: filtersGroup.Conditions,
			FilterSize:   filtersSize,

			Blocks:                    blocks[:],
			CurrentBlockProcessingIdx: 0,

			AbsBlockMaps: absBlockMaps[:],
		}

		// preprocess segments into blocks
		blocksPreprocessErr := preprocessSegmentsIntoBlocksAndHeaderFilter(sm, &slabMergerContext, blockSegments)
		if blocksPreprocessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("unable to preprocess blocks from segments: %s", blocksPreprocessErr.Error())
		}

		// for itIdx := range slabMergerContext.CurrentBlockProcessingIdx {

		// 	cBlock := &blocks[itIdx]
		// 	mergerInfo := &absBlockMaps[itIdx]

		// 	// if cBlock.SlabHeader == nil {
		// 	// 	slog.Info("block info after preprocess", "block_idx", itIdx, "val_is_nil", cBlock.Val == nil, "full_skip", mergerInfo.FullSkip(), "abs_slab_offset", cBlock.SlabHeader.SlabOffsetBlocks)
		// 	// }
		// }

		singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(&slabMergerContext, blocks[:], indicesResultCache[:])
		if chunkProcessErr != nil {
			return ChunkFilterProcessResult{}, fmt.Errorf("chunk processing failed : %s", chunkProcessErr.Error())
		} else {
			result.skippedBlocksDueToHeaderFiltering += singleColumnProcessResult.skippedBlocksDueToHeaderFiltering
		}
	}

	totalItems := 0
	wastedMerges := 0

	// filter merged blocks info
	for _, blockFilterMask := range absBlockMaps {
		if blockFilterMask.Merges() == plan.FilterSize {
			amount := blockFilterMask.ResultBitset.Count()
			totalItems += amount

		} else {
			wastedMerges += blockFilterMask.Merges()
		}
	}

	result.totalItems = totalItems
	result.wastedMerges = wastedMerges

	// todo cleanup
	// absBlockMaps

	return result, nil
}
