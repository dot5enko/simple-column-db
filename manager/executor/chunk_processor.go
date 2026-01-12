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
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/ops/generated"
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

	if false {
		// should be optimized by using one biggest for all
		// and casting with unsafe pointer to needed type
		// and buf should be a part of thread's cache

		var uint64Buffer [schema.BlockRowsSize]uint64
		var float32Buffer [schema.BlockRowsSize]float32

		// process selectors
		for _, selectorGroup := range plan.SelectorsGroupedByFields {
			chunkBlocks := blockChunk.ChunkSegmentsByFieldIndexMap[selectorGroup.ColumnIdx]

			relBlockId := 0

			for _, singleSelector := range selectorGroup.Selectors {

				funcName := singleSelector.Arguments[0]
				selectorName := fmt.Sprintf("%s(%s)", funcName, selectorGroup.FieldName)

				if selectorGroup.FieldName == "*" {
					color.Yellow("skipped * selector, not implemented yet")
					continue
				}

				type FuncChunkResultMeta struct {
					initialized bool
					Count       int
					Sum         float64
					Avg         float64
					Max         float64
					Min         float64
				}

				var chunkResultEntries [query.ExecutorChunkSizeBlocks]FuncChunkResultMeta

				for _, segment := range chunkBlocks {

					for segmentOffset := range segment.Size {

						func() {
							mergerBitset := cache.AbsBlockMaps[relBlockId]

							defer func() {
								if r := recover(); r != nil {

									valRef := cache.Blocks[relBlockId].Val == nil

									color.Yellow("selector group column: %s %s %T", selectorGroup.ColumnSchemaInfo.Name, selectorGroup.ColumnSchemaInfo.Type.String(), cache.Blocks[relBlockId].Val.DataTypedArray)
									color.Red("recovered on <field=%s><rel_block_id=%d> valRef = nil (%v). merger.Count = %d", selectorName, relBlockId, valRef, mergerBitset.Count())

									stackDebugRows := 3

									for debugRowIdx := range stackDebugRows {
										_, file, line, ok := runtime.Caller(debugRowIdx + 2)
										if ok {
											fmt.Printf("\t%s:%d\n", file, line)
										}
									}

								}
								relBlockId += 1

							}()

							if mergerBitset.FullSkip() || mergerBitset.Count() == -1 {
								// continue
								return
							}

							blockOffset := segment.StartBlock + segmentOffset

							directArrayAccess, arraySize := cache.Blocks[relBlockId].Val.DirectAccess()

							var itemsCount int64

							chunkResultMeta := &chunkResultEntries[relBlockId]

							switch selectorGroup.ColumnSchemaInfo.Type {
							case schema.Uint64FieldType:

								arrInputWhole := directArrayAccess.([]uint64)
								arrInput := arrInputWhole[:arraySize]

								itemsCount = int64(ops.CollectByBitset(arrInput, &mergerBitset.ResultBitset, uint64Buffer[:]))
							case schema.Float32FieldType:
								arrInputWhole := directArrayAccess.([]float32)
								arrInput := arrInputWhole[:arraySize]

								itemsCount = int64(ops.CollectByBitset(arrInput, &mergerBitset.ResultBitset, float32Buffer[:]))

								switch funcName {
								case "avg":

									var sum float64
									for _, v := range float32Buffer {
										sum += float64(v)
									}

									chunkResultMeta.initialized = true
									chunkResultMeta.Sum = sum
									chunkResultMeta.Count = int(itemsCount)
									chunkResultMeta.Avg = sum / float64(itemsCount)

								default:
									panic(fmt.Sprintf("unknown function in selector : %s", funcName))
								}

							default:
								panic(fmt.Sprintf("unsupported type %v, while processing selector %#+v", selectorGroup.ColumnSchemaInfo.Type.String(), singleSelector.Arguments))
							}

							// color.Green("<query=%d/chunk%d> found %d item in block[%d/rel=%d] %s = %v", plan.Id, blockChunk.GlobalBlockOffset, itemsCount, blockOffset, relBlockId, selectorName, chunkFuncResult)

							if itemsCount != int64(mergerBitset.Count()) {
								color.Yellow(fmt.Sprintf("bitsets count mismatch, expected %d got %d", mergerBitset.Count(), itemsCount))
							}
							_ = blockOffset

							// relBlockId += 1
						}()
					}
				}

				// calc final result for this selector
				{

					finalResultMeta := FuncChunkResultMeta{}

					switch funcName {

					case "avg":

						for _, meta := range chunkResultEntries {
							if !meta.initialized {
								break
							}
							finalResultMeta.initialized = true
							finalResultMeta.Sum += meta.Sum
							finalResultMeta.Count += meta.Count

						}

						finalResultMeta.Avg = finalResultMeta.Sum / float64(finalResultMeta.Count)

					default:
						panic(fmt.Sprintf("unknown function in selector : %s, while aggregating results", funcName))
					}

					color.Green("<query=%d/chunk%d>  selector %s = %.2f", plan.Id, blockChunk.GlobalBlockOffset, selectorName, finalResultMeta.Avg)

				}

			}
		}
	}

	result.TotalItems = int64(totalItems)
	result.WastedMerges = int64(wastedMerges)

	return nil
}
