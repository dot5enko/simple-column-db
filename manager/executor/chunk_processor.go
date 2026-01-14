package executor

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
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

	// per field/slab processing
	slabMergerContext.Schema = plan.Schema
	slabMergerContext.AbsOffsetStart = blockChunk.GlobalBlockOffset

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

	{

		t0 := time.Now()

		// should be optimized by using one biggest for all
		// and casting with unsafe pointer to needed type
		// and buf should be a part of thread's cache

		// process selectors
		for _, selectorGroup := range plan.SelectorsGroupedByFields {

			blockSegments := blockChunk.ChunkSegmentsByFieldIndexMap[selectorGroup.ColumnIdx]
			collectResultsBlockIdx := 0

			///
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

						merger := cache.AbsBlockMaps[curRelativeBlockId]
						if !merger.FullSkip() {

							//// process selectors applicable to current block
							selectorsResult, selectorsApplyErr := ProcessMultipleSelectorsOnSingleBlock(curRelativeBlockId, &selectorGroup, slabMergerContext, blockDecodedInfo, cache)
							if selectorsApplyErr != nil {
								return selectorsApplyErr
							}

							_ = selectorsResult

							// for _, it := range selectorsResult.Results {
							// 	log.Printf(" --- filters[%d] items count : %d, sum : %.2f", curRelativeBlockId, it.Count, it.Sum)
							// }
							////

						}

					}
				}
			}

			///
		}

		tookSelectors := time.Since(t0)
		result.TookSelectors = tookSelectors.Nanoseconds()

		// calc final result for this selector
		// {

		// 	finalResultMeta := FuncChunkResultMeta{}

		// 	switch funcName {

		// 	case "avg":

		// 		for _, meta := range chunkResultEntries {
		// 			if !meta.initialized {
		// 				break
		// 			}
		// 			finalResultMeta.initialized = true
		// 			finalResultMeta.Sum += meta.Sum
		// 			finalResultMeta.Count += meta.Count

		// 		}

		// 		finalResultMeta.Avg = finalResultMeta.Sum / float64(finalResultMeta.Count)

		// 	default:
		// 		panic(fmt.Sprintf("unknown function in selector : %s, while aggregating results", funcName))
		// 	}

		// 	color.Green("<query=%d/chunk%d>  selector %s = %.2f", plan.Id, blockChunk.GlobalBlockOffset, selectorName, finalResultMeta.Avg)

		// }

	}

	result.TotalItems = int64(totalItems)
	result.WastedMerges = int64(wastedMerges)

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
	Results []FuncChunkResultMeta
}

var floatComparisons atomic.Int64

func ProcessMultipleSelectorsOnSingleBlock(
	curRelativeBlockId int,
	selectorGroup *query.SelectorGroupedRT,
	slabMergerContext *executortypes.BlockMergerContext,
	rtBlockData *schema.RuntimeBlockData,
	cache *executortypes.ChunkExecutorThreadCache,
) (SelectorsResult, error) {

	resultObject := SelectorsResult{
		Results: make([]FuncChunkResultMeta, len(selectorGroup.Selectors)),
	}

	if len(selectorGroup.Selectors) > 1 {
		log.Printf("selectors on same field : %d ", len(selectorGroup.Selectors))
	}

	for selectorIdx, singleSelector := range selectorGroup.Selectors {

		funcName := singleSelector.Arguments[0]
		selectorName := fmt.Sprintf("%s(%s)", funcName, selectorGroup.FieldName)

		// if selectorGroup.FieldName == "*" {
		// 	// color.Yellow("skipped * selector, not implemented yet")
		// 	continue
		// }

		/////

		{
			// blockData := &slabMergerContext.Blocks[idx]
			mergerBitset := &slabMergerContext.AbsBlockMaps[curRelativeBlockId]

			func(idx int) {

				defer func() {
					if r := recover(); r != nil {

						// blockRef := &cache.Blocks[curRelativeBlockId]
						cacheRef := rtBlockData
						valRef := cacheRef == nil

						fmt.Printf("block_idx : %d\n", idx)
						color.Yellow("selector group column: field_name=%s type_expected=%s actually_got=%T", selectorGroup.ColumnSchemaInfo.Name, selectorGroup.ColumnSchemaInfo.Type.String(), cacheRef.DataTypedArray)
						color.Red("recovered on <field=%10s><rel_block_id=%4d>, slab = %s, valRef = nil (%v). merger.Count = %4d", selectorName, idx, nil, valRef, mergerBitset.Count())

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

				if mergerBitset.FullSkip() || mergerBitset.Count() == -1 {
					// continue
					return
				}

				directArrayAccess, arraySize := rtBlockData.DirectAccess()

				var itemsCount int64

				chunkResultMeta := &resultObject.Results[selectorIdx]

				switch selectorGroup.ColumnSchemaInfo.Type {
				case schema.Int8FieldType:

					switch funcName {
					case "count":

						itemsCount = int64(mergerBitset.Count())
						chunkResultMeta.initialized = true
						chunkResultMeta.Count = int(itemsCount)
					default:
						panic(fmt.Sprintf("function on int8 not implemented: %s", funcName))
					}

				case schema.Uint64FieldType:

					arrInputWhole := directArrayAccess.([]uint64)
					arrInput := arrInputWhole[:arraySize]

					uint64Buffer := unsafe.Slice((*uint64)(unsafe.Pointer(&cache.SelectorBuffer[0])), arraySize)

					itemsCount = int64(ops.CollectByBitset(arrInput, &mergerBitset.ResultBitset, uint64Buffer[:]))

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

					float32Buffer := unsafe.Slice((*float32)(unsafe.Pointer(&cache.SelectorBuffer[0])), arraySize)

					itemsCount = int64(ops.CollectByBitset(arrInput, &mergerBitset.ResultBitset, float32Buffer[:]))

					switch funcName {
					case "avg":

						var sum float64

						// totalComparisons := floatComparisons.Add(itemsCount)
						// log.Printf("f32 comparisons : %d", totalComparisons)

						for i := int64(0); i < itemsCount; i++ {
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
					color.Red("unsupported type %v, while processing selector %#+v", selectorGroup.ColumnSchemaInfo.Type.String(), singleSelector.Arguments)
					panic("unsupported type ")
				}

				// color.Green("<query=%d/chunk%d> found %d item in block[%d/rel=%d] %s = %v", plan.Id, blockChunk.GlobalBlockOffset, itemsCount, blockOffset, relBlockId, selectorName, chunkFuncResult)

				if itemsCount != int64(mergerBitset.Count()) {
					color.Yellow(fmt.Sprintf("bitsets count mismatch, expected %d got %d", mergerBitset.Count(), itemsCount))
				}
			}(curRelativeBlockId)

		}
		/////

	}

	return resultObject, nil
}
