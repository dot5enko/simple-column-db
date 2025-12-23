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
		blockSegments := blockChunk.SlabsByFields[filtersGroup.ColumnIdx]
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
	blockRT.Header = blockHeader

	// increase current block pointer

	if !fullSkipBlock {
		blockDecodedInfo, blockErr := slabsManager.LoadBlockToRuntimeBlockData(mergerContext.Schema, slabInfo, blockHeader.Uid)

		// log.Printf("--- loaded block %s: @ %p", blockHeader.Uid.String(), blockDecodedInfo.DataTypedArray)

		if blockErr != nil {
			return fmt.Errorf("unable to decode block : %s", blockErr.Error())
		}

		blockRT.Val = blockDecodedInfo
	} else {
		absBlockRTInfo := mergerContext.AbsBlockMaps[curRelativeBlockId]

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

// todo should result something
func executePlanChunk(sm *Manager, plan *query.QueryPlan, blockChunk query.BlockChunk) (any, error) {

	// preallocate per executor thread

	absBlockMaps := [query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged{}
	blocks := [query.ExecutorChunkSizeBlocks]BlockRuntimeInfo{}

	skippedBlocksFULL := 0

	// preload all slabs that are in the chunk
	preloadErr := preloadChunks(sm, plan, blockChunk)
	if preloadErr != nil {
		return nil, fmt.Errorf("unable to preload chunks : %s", preloadErr.Error())
	}

	schemaObject := plan.Schema

	for _, filtersGroup := range plan.FilterGroupedByFields {

		// per field/slab processing
		//
		// could be parallelized by columns
		// but synchronization is needed

		columnName, filterColumn := filtersGroup.FieldName, filtersGroup.Conditions
		blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[filtersGroup.ColumnIdx]

		filtersSize := len(filterColumn)

		slabMergerContext := BlockMergerContext{
			Schema:         schemaObject,
			AbsOffsetStart: blockChunk.GlobalBlockOffset,

			FilterColumn: filterColumn,
			FilterSize:   filtersSize,

			Blocks:                    blocks[:],
			CurrentBlockProcessingIdx: 0,

			AbsBlockMaps: absBlockMaps[:],
		}

		for _, segment := range blockSegments {

			slabBlockOffsetStart := segment.StartBlock

			slabInfo, slabErr := sm.Slabs.LoadSlabToCache(schemaObject, segment.Slab)
			if slabErr != nil {
				return nil, fmt.Errorf("unable to load slab : %s", slabErr.Error())
			}

			// preallocate for each executor thread, should be  same as absBlockMaps

			blockHeaders := slabInfo.BlockHeaders

			// todo remove internal function call here
			// move whole loop into separate func

			for i := 0; i < int(segment.Size); i++ {
				idx := i + slabBlockOffsetStart

				if idx > int(slabInfo.BlocksFinalized) {
					break
				}

				blockHeader := &blockHeaders[idx]

				preparationErr := prepareBlockForMerger(&slabMergerContext,
					slabInfo,
					blockHeader,
					&sm.Slabs,
				)
				if preparationErr != nil {
					return nil, fmt.Errorf("unable to prepare block for merging : %s", preparationErr.Error())
				}
			}
		}

		for _, slab := range slabsByColumns[columnName] {

			var blocks []BlockRuntimeInfo
			for idx, blockHeader := range slabInfo.BlockHeaders {

				if idx > int(slabInfo.BlocksFinalized) {
					break
				}

				skipFilters := 0
				absBlockOffset := slabInfo.SlabOffsetBlocks + uint64(idx)

				for _, filter := range filterColumn {

					var processFilterErr error
					intersectType := schema.UnknownIntersection

					// process filter on a block header
					switch columnInfo.Type {
					case schema.Uint64FieldType:
						intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint64](filter.filter, blockHeader)
					case schema.Uint8FieldType:
						intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint8](filter.filter, blockHeader)
					case schema.Float32FieldType:
						intersectType, processFilterErr = ProcessFilterOnBlockHeader[float32](filter.filter, blockHeader)
					case schema.Float64FieldType:
						intersectType, processFilterErr = ProcessFilterOnBlockHeader[float64](filter.filter, blockHeader)
					default:
						return nil, fmt.Errorf("unsupported type %v while filtering block headers", columnInfo.Type.String())
					}

					if processFilterErr != nil {
						return nil, fmt.Errorf("error filter processing : %s", processFilterErr.Error())
					} else {

						skipSingleBlock := intersectType == schema.NoIntersection
						// here, so we override old data for sure
						filter.Runtime.FilterLastBlockHeaderResult = intersectType

						if skipSingleBlock {
							skipFilters++
						}
					}
				}

				fullSkipBlock := skipFilters == filtersSize

				if fullSkipBlock {

					skippedBlocksFULL += 1

					// color.Yellow("skipping block %s on header filtering step", blockHeader.Uid.String())
					// do not load this block into memory at all
				}

				blockRT := BlockRuntimeInfo{
					Header:       blockHeader,
					Synchronized: true,
				}

				if !fullSkipBlock {
					blockDecodedInfo, blockErr := sm.Slabs.LoadBlockToRuntimeBlockData(*schemaObject, slabInfo, blockHeader.Uid)

					// log.Printf("--- loaded block %s: @ %p", blockHeader.Uid.String(), blockDecodedInfo.DataTypedArray)

					if blockErr != nil {
						return nil, fmt.Errorf("unable to decode block : %s", blockErr.Error())
					}

					blockRT.Val = blockDecodedInfo
				} else {
					absBlockRTInfo, ok := absBlockMaps[absBlockOffset]

					if !ok {
						absBlockRTInfo = sm.indiceMergerPool.Get().(*lists.IndiceUnmerged)
						absBlockRTInfo.Reset()

						absBlockMaps[absBlockOffset] = absBlockRTInfo
					}

					absBlockRTInfo.SetFullSkip()
				}

				for filterIdx, filter := range filterColumn {
					blockRT.HeaderFilterMatchResult[filterIdx] = filter.Runtime.FilterLastBlockHeaderResult
				}

				blocks = append(blocks, blockRT)
			}

			// get slab bounds
			// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

			for blockIdx, blockData := range blocks {

				absBlockOffset := slabInfo.SlabOffsetBlocks + uint64(blockIdx)

				blockGroupMerger, has := absBlockMaps[absBlockOffset]
				if !has {
					blockGroupMerger = sm.indiceMergerPool.Get().(*lists.IndiceUnmerged)
					absBlockMaps[absBlockOffset] = blockGroupMerger
				} else {
					if blockGroupMerger.FullSkip() {
						continue
					}
				}

				for fIdx, filter := range filterColumn {

					headerMatchResult := blockData.HeaderFilterMatchResult[fIdx]

					isFull := headerMatchResult == schema.FullIntersection

					if isFull {
						skippedBlocksDueToHeaderFiltering += 1

						blockGroupMerger.With(nil, false, true)
						continue
					}

					{
						var processFilterErr error
						var filteredSize int

						// process filter on a block
						switch columnInfo.Type {
						case schema.Uint64FieldType:
							filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Uint8FieldType:
							filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint8](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float32FieldType:
							filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float32](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float64FieldType:
							filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						default:
							return nil, fmt.Errorf("unsupported type %v", columnInfo.Type.String())
						}

						_ = filteredSize

						if processFilterErr != nil {
							return nil, fmt.Errorf("error filter processing : %s. sum of bitset = %d, bitcount = %d", processFilterErr.Error(), blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
						}

						// log.Printf(" -- [filtered] filteredSize : %d. sum of bitset = %d, bitcount = %d", filteredSize, blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
					}
				}
			}
		}
	}

	totalItems := 0
	wastedMerges := 0

	// filter merged blocks info
	for _, blockFilterMask := range absBlockMaps {
		if blockFilterMask.Merges() == len(queryData.Filter) {
			amount := blockFilterMask.ResultBitset.Count()
			totalItems += amount

		} else {
			wastedMerges += blockFilterMask.Merges()
		}

		sm.indiceMergerPool.Put(blockFilterMask)
	}

	clear(absBlockMaps)

	slog.Info("merge info", "skipped_full", skippedBlocksFULL, "wasted_merges", wastedMerges, "skipped_blocks", skippedBlocksDueToHeaderFiltering, "total_filtered", totalItems)

	return nil
}
