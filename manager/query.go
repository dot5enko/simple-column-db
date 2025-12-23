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

	for chunkIdx, blockChunk := range plan.BlockChunks {
		for _, filterGroup := range plan.FilterGroupedByFields {

			blockSegments := blockChunk.SlabsByFields[filterGroup.ColumnIdx]

			slog.Info("processing chunk", "chunk_idx", chunkIdx, "column_name", filterGroup.FieldName, "block_segments", len(blockSegments))

			// paralelize!

			// for _, blockSegment := range blockSegments {

			// }

		}

		// slog.Info("processing chunk", "blocks", len(blockChunk.SlabsByFields))
	}

	/* {
		absBlockMaps := map[uint64]*lists.IndiceUnmerged{}
		skippedBlocksFULL := 0

		for _, filtersGroup := range plan.FilterGroupedByFields {

			columnName, filterColumn := filtersGroup.FieldName, filtersGroup.Conditions

			filtersSize := len(filterColumn)

			for _, slab := range slabsByColumns[columnName] {

				slabInfo, slabErr := sm.Slabs.LoadSlabToCache(*schemaObject, slab)
				if slabErr != nil {
					return nil, fmt.Errorf("unable to load slab : %s", slabErr.Error())
				}

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
							filter.runtime.filterLastBlockHeaderResult = intersectType

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
						blockRT.HeaderFilterMatchResult[filterIdx] = filter.runtime.filterLastBlockHeaderResult
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
	}
	*/
	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
