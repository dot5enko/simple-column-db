package executor

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dot5enko/simple-column-db/lists"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/manager/rtconfig"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
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

func filterDataOnChunk(
	cache *executortypes.ChunkExecutorThreadCache,
	sm *meta.SlabManager,
	plan *query.QueryPlan,
	blockChunk *query.BlockChunk,
	slabMergerContext *executortypes.BlockMergerContext,
	result *executortypes.ChunkFilterProcessResult,
) error {

	cache.Reset()

	// runtime.LockOSThread()
	// defer runtime.UnlockOSThread()

	// defer allocsDetection()()

	// per field/slab processing
	slabMergerContext.Schema = plan.Schema
	slabMergerContext.AbsOffsetStart = blockChunk.GlobalBlockOffset

	// filter chunks
	for _, filtersGroup := range plan.FilterGroupedByFields {

		filtersSize := len(filtersGroup.Conditions)

		// check if limitations are met before this line,
		// in a validation step before loading any data
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

		// groupType := filtersGroup.ColumnSchemaInfo.Type
		// singleColumnProcessResult, chunkProcessErr := generated.ChunkBlockProcessorSpecificFilterAndType(groupType, slabMergerContext)

		singleColumnProcessResult, chunkProcessErr := processFiltersOnPreparedBlocks(slabMergerContext, sm)
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

func precalcIndices(unmergedItems *[query.ExecutorChunkSizeBlocks]lists.IndiceUnmerged) {
	for idx := range query.ExecutorChunkSizeBlocks {
		unmergedItems[idx].GetIndicesCache(true)
	}
}

func processSelectorsOnChunk(
	cache *executortypes.ChunkExecutorThreadCache,
	sm *meta.SlabManager,
	plan *query.QueryPlan,
	blockChunk *query.BlockChunk,
	slabMergerContext *executortypes.BlockMergerContext,
	result *executortypes.ChunkFilterProcessResult,
) error {
	t0 := time.Now()

	chunkItemsFiltered := 0

	for blockIdx := range query.ExecutorChunkSizeBlocks {
		items := cache.AbsBlockMaps[blockIdx].Count()
		if items > 0 {
			chunkItemsFiltered += items
		}
	}

	totalBytesPerChunk := 0

	for _, filtersGroup := range plan.SelectorsGroupedByFields {
		elementSize := filtersGroup.ColumnSchemaInfo.Type.Size()
		memoryRequirements := elementSize * chunkItemsFiltered
		totalBytesPerChunk += memoryRequirements
	}

	precalcIndices(&cache.AbsBlockMaps)

	// collect filtered data
	{
		// process selectors
		for selectorGroupIdx, selectorGroup := range plan.SelectorsGroupedByFields {

			elementSize := selectorGroup.ColumnSchemaInfo.Type.Size()
			memoryRequirements := elementSize * chunkItemsFiltered

			// todo use ring buffer
			// or bounded pool
			curBufferH := &result.SelectorBuffers[selectorGroupIdx]

			if memoryRequirements < 1*1024*1024 {

				bufferHandler := sm.GetRuntimeCache().MegabyteCache.Get()
				curBufferH.BufferHandler = bufferHandler
				curBufferH.Buffer = bufferHandler.Data[:memoryRequirements]
			} else {
				if memoryRequirements < 3*1024*1024 {
					bufferHandler := sm.GetRuntimeCache().ThreeMegabyteCache.Get()
					curBufferH.BufferHandler = bufferHandler
					curBufferH.Buffer = bufferHandler.Data[:memoryRequirements]

				} else {
					bufferHandler := sm.GetRuntimeCache().SlabRuntimeCache.Get()
					curBufferH.BufferHandler = bufferHandler
					curBufferH.Buffer = bufferHandler.Data[:memoryRequirements]
				}
			}

			curBufferH.Size = memoryRequirements

			currentBufferOffset := 0

			blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[selectorGroup.ColumnIdx]
			collectResultsBlockIdx := 0

			/// segments loop
			segmentsLen := len(blockSegments)
			for idx := range segmentsLen {

				segment := &blockSegments[idx]

				slabBlockOffsetStart := segment.StartBlock

				slabInfo, slabErr := sm.LoadSlabHeaderToCache(&slabMergerContext.Schema, segment.Slab)
				if slabErr != nil {
					return fmt.Errorf("unable to load slab : %s", slabErr.Error())
				}

				blockHeaders := slabInfo.BlockHeaders

				for i := 0; i < int(segment.Size); i++ {
					idx := i + slabBlockOffsetStart

					if idx > int(slabInfo.BlocksFinalized) {
						break
					}

					blockHeader := &blockHeaders[idx]

					{
						curRelativeBlockId := collectResultsBlockIdx
						collectResultsBlockIdx += 1

						// blockRT := &slabMergerContext.Blocks[curRelativeBlockId]

						blockDecodedInfo, blockRuntimeDataErr := sm.LoadBlockToRuntimeBlockData(slabMergerContext.Schema, slabInfo, blockHeader.Uid)

						if blockRuntimeDataErr != blockRuntimeDataErr {
							return fmt.Errorf("unable to load block's data into rt cache: %s", blockRuntimeDataErr.Error())
						}

						merger := &cache.AbsBlockMaps[curRelativeBlockId]
						if !merger.FullSkip() {

							// multiple selectors not supported yet
							// this loop always has one element
							for _, singleSelector := range selectorGroup.Selectors {

								handleErr := func() error {

									beforeStart := currentBufferOffset

									defer func() {
										rec := recover()

										if rec != nil {
											color.Red("panicked in ProcessSelectorOnBlock")
											color.Red("%s", rec)

											color.Red("buffer type used : %T. cap = %d", curBufferH.BufferHandler, cap(curBufferH.Buffer))
											color.Red("-- buffer start before : %d, total_size = %d", beforeStart, memoryRequirements)
											color.Red("elementSize * chunkItemsFiltered = %d * %d = %d (memoryReq = %d)", elementSize, chunkItemsFiltered, elementSize*chunkItemsFiltered, memoryRequirements)
											stackDebugRows := 5

											for debugRowIdx := range stackDebugRows {
												_, file, line, ok := runtime.Caller(debugRowIdx + 2)
												if ok {
													fmt.Printf("\t%s:%d\n", file, line)
												}
											}

											os.Exit(0)
										}
									}()

									//// process selectors applicable to current block
									selectorsResult, selectorsApplyErr := ProcessSelectorOnBlock(
										curRelativeBlockId,
										&singleSelector,
										blockHeader.DataType,
										slabMergerContext,
										blockDecodedInfo,
										curBufferH.Buffer[currentBufferOffset:],
									)

									if selectorsApplyErr != nil {
										return selectorsApplyErr
									}

									currentBufferOffset += selectorsResult.BytesSize

									return nil
								}()

								if handleErr != nil {
									return handleErr
								}

							}
						}
					}
				}
			}
			/// segments lopp end
		}
	}

	tookSelectors := time.Since(t0)
	result.TookSelectors = tookSelectors.Nanoseconds()

	return nil
}

type FuncChunkResultMeta struct {
	initialized bool
	Count       int
	Sum         float64
	Avg         float64
	Max         float64
	Min         float64
}

type SelectorsResult struct {
	Result FuncChunkResultMeta

	ItemsSize int
	BytesSize int
}

var floatComparisons atomic.Int64

func ProcessSelectorOnBlock(
	curRelativeBlockId int,
	singleSelector *query.Selector,
	selectorFieldType schema.FieldType,
	slabMergerContext *executortypes.BlockMergerContext,
	rtBlockData *schema.RuntimeBlockData,
	bufferForData []byte,
) (resultObject SelectorsResult, topErr error) {

	funcName := singleSelector.Arguments[0]

	// if selectorGroup.FieldName == "*" {
	// 	// color.Yellow("skipped * selector, not implemented yet")
	// 	continue
	// }

	/////

	{
		// blockData := &slabMergerContext.Blocks[idx]
		mergerBitset := &slabMergerContext.AbsBlockMaps[curRelativeBlockId]

		// func(idx int) {

		if false {
			defer func() {
				selectorName := funcName //fmt.Sprintf("%s(%s)", funcName, selectorGroup.FieldName)

				if r := recover(); r != nil {

					// blockRef := &cache.Blocks[curRelativeBlockId]
					cacheRef := rtBlockData
					valRef := cacheRef == nil

					fmt.Printf("block_idx : %d\n", curRelativeBlockId)
					// color.Yellow("selector group column: field_name=%s type_expected=%s actually_got=%T", selectorGroup.ColumnSchemaInfo.Name, selectorGroup.ColumnSchemaInfo.Type.String(), cacheRef.DataTypedArray)
					color.Red("recovered on <field=%10s><rel_block_id=%4d>, slab = %s, valRef = nil (%v). merger.Count = %4d", selectorName, curRelativeBlockId, nil, valRef, mergerBitset.Count())

					// debugHistory := blockRef.GetDebugHistory()

					// prevTime := debugHistory[0].Time

					// for i := 0; i < len(debugHistory); i++ {

					// 	entry := debugHistory[i]

					// 	diff := entry.Time.Sub(prevTime)
					// 	prevTime = entry.Time

					// 	dType := entry.DataType.String()
					// 	if entry.Action == 0 {
					// 		dType = "-"
					// 	}

					// 	fmt.Printf(" action=%d data_type: %15s (thread=%d) block_idx : %3d, slab : %16s from prev : %5.2f. \n", entry.Action, dType, entry.Thread, entry.BlockIdx, entry.Slab, diff.Seconds()*1000000)
					// }

					// show events history
					stackDebugRows := 3

					for debugRowIdx := range stackDebugRows {
						_, file, line, ok := runtime.Caller(debugRowIdx + 2)
						if ok {
							fmt.Printf("\t%s:%d\n", file, line)
						}
					}

					os.Exit(0)

				}
			}()
		}

		if mergerBitset.FullSkip() || !mergerBitset.Initialized() {
			// continue
			return
		}

		directArrayAccess, arraySize := rtBlockData.DirectAccess()

		var itemsCount int

		chunkResultMeta := &resultObject.Result

		switch rtBlockData.Header.DataType {
		case schema.Int8FieldType:

			switch funcName {
			case "count":

				itemsCount = mergerBitset.Count()
				chunkResultMeta.initialized = true
				chunkResultMeta.Count = int(itemsCount)
			default:
				panic(fmt.Sprintf("function on int8 not implemented: %s", funcName))
			}

		case schema.Uint64FieldType:

			arrInputWhole := directArrayAccess.([]uint64)
			arrInput := arrInputWhole[:arraySize]

			if len(bufferForData) == 0 {
				color.Yellow("trying to address 0 element with 0 size, arraySize = %d", arraySize)
			}

			uint64Buffer := unsafe.Slice((*uint64)(unsafe.Pointer(&bufferForData[0])), arraySize)

			indicesList := mergerBitset.GetIndicesCache(false)

			if len(indicesList) == rtconfig.ROWS_PER_BLOCK {
				copy(uint64Buffer, arrInput)
				itemsCount = rtconfig.ROWS_PER_BLOCK
			} else {
				itemsCount = ops.CollectByIndices(arrInput, indicesList, uint64Buffer[:])
			}

			switch funcName {
			case "avg":

				var sum uint64
				for _, v := range uint64Buffer[:itemsCount] {
					sum += v
				}

				chunkResultMeta.initialized = true
				chunkResultMeta.Sum = float64(sum)
				chunkResultMeta.Count = int(itemsCount)
				chunkResultMeta.Avg = chunkResultMeta.Sum / float64(itemsCount)

			default:
				panic(fmt.Sprintf("unknown function in selector u64 : %s", funcName))
			}

		case schema.Float32FieldType:
			arrInputWhole := directArrayAccess.([]float32)
			arrInput := arrInputWhole[:arraySize]

			float32Buffer := unsafe.Slice((*float32)(unsafe.Pointer(&bufferForData[0])), arraySize)
			itemsCount = ops.CollectByIndices(arrInput, mergerBitset.GetIndicesCache(false), float32Buffer[:])

			switch funcName {
			case "avg":

				var sum float64

				for i := 0; i < itemsCount; i++ {
					sum += float64(float32Buffer[i])
				}

				chunkResultMeta.initialized = true
				chunkResultMeta.Sum = float64(sum)
				chunkResultMeta.Count = int(itemsCount)
				chunkResultMeta.Avg = chunkResultMeta.Sum / float64(itemsCount)

			default:
				panic(fmt.Sprintf("unknown function in selector f64 : %s", funcName))
			}

		default:
			color.Red("unsupported type %v, while processing selector %#+v", rtBlockData.Header.DataType.String(), singleSelector.Arguments)
			panic("unsupported type ")
		}

		// color.Green("<query=%d/chunk%d> found %d item in block[%d/rel=%d] %s = %v", plan.Id, blockChunk.GlobalBlockOffset, itemsCount, blockOffset, relBlockId, selectorName, chunkFuncResult)

		if itemsCount != mergerBitset.Count() {
			color.Yellow(fmt.Sprintf("bitsets count mismatch, expected %d got %d", mergerBitset.Count(), itemsCount))
		}

		resultObject.ItemsSize = itemsCount
		resultObject.BytesSize = int(itemsCount) * selectorFieldType.Size()

		// }(curRelativeBlockId)

	}
	/////

	return resultObject, nil
}
