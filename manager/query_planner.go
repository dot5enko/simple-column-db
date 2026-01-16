package manager

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/manager/executor/filters"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type QueryPlanner struct {
	pool *cache.TypedRingBuffer[query.Query]
}

func NewQueryPlanner(predefinedCacheSize int) *QueryPlanner {
	return &QueryPlanner{
		pool: cache.NewTypedRingBuffer[query.Query](predefinedCacheSize),
	}
}

var timeWasted atomic.Int32

func (qp *QueryPlanner) Plan(
	schemaName string,
	queryData query.Query,
	metaManager *meta.MetaManager,
	slabManager *meta.SlabManager,
	options *query.QueryOptions,
) (query.QueryPlan, error) {

	schemaObject := metaManager.GetSchema(schemaName)

	if schemaObject == nil {
		return query.QueryPlan{}, query.ErrSchemaNotFound
	} else {

		type PlanFieldInfo struct {
			name       string
			where      string
			filter     bool
			filterCond *query.FilterCondition
		}

		fieldsAffected := map[string]PlanFieldInfo{}
		fieldsList := []PlanFieldInfo{}

		selectOnlyFields := map[string]bool{}

		// fields processing
		{
			for _, filter := range queryData.Filter {
				fieldsAffected[filter.Field] = PlanFieldInfo{
					name:       filter.Field,
					where:      fmt.Sprintf("filter %#+v", filter.Operand),
					filter:     true,
					filterCond: &filter,
				}
			}

			for _, selector := range queryData.Select {
				fieldName := (selector.Arguments[len(selector.Arguments)-1]).(string)

				if fieldName != "*" {
					_, old := fieldsAffected[fieldName]
					if !old {

						selectOnlyFields[fieldName] = true

						fieldsAffected[fieldName] = PlanFieldInfo{
							name:  fieldName,
							where: fmt.Sprintf("select %s", selector.Alias),
						}
					}
				}
			}

			for _, fieldInfo := range fieldsAffected {
				fieldsList = append(fieldsList, fieldInfo)
			}
		}
		// end of fields processing

		// check that all fields are valid
		for _, fieldInfo := range fieldsList {

			found := false
			for _, it := range schemaObject.Columns {
				if it.Name == fieldInfo.name {
					found = true
					break
				}
			}

			if !found {
				return query.QueryPlan{}, fmt.Errorf("column `%v` not found on schema `%v` (%s)", fieldInfo.name, schemaName, fieldInfo.where)
			}
		}

		filtersByColumns := map[string][]query.FilterConditionRuntime{}
		filterByColumnsArray := []query.FilterGroupedRT{}

		{
			// group filters by columns

			for _, filter := range queryData.Filter {
				old, isOk := filtersByColumns[filter.Field]
				if !isOk {
					old = []query.FilterConditionRuntime{}
				}

				fvRt := query.FilterConditionRuntime{
					Filter: filter,
				}
				fvRt.GetUniqueId(schemaObject.Name)

				filtersByColumns[filter.Field] = append(old, fvRt)
			}

			for fname, it := range filtersByColumns {

				var columnInfo schema.SchemaColumn
				columnIdx := 0

				// all fields must exist, as they were checked above
				for idx, it := range schemaObject.Columns {
					if it.Name == fname {
						columnInfo = it
						columnIdx = idx
						break
					}
				}

				filterByColumnsArray = append(filterByColumnsArray, query.FilterGroupedRT{
					FieldName:        fname,
					Conditions:       it,
					ColumnSchemaInfo: &columnInfo,
					ColumnIdx:        columnIdx,
				})
			}

			// sort by name
			// for consistency of results
			slices.SortStableFunc(filterByColumnsArray, func(a, b query.FilterGroupedRT) int {
				return strings.Compare(a.FieldName, b.FieldName)
			})
		}

		//selectors by columns
		selectorsByColumns := map[string][]query.Selector{}
		selectorsByColumnsArray := []query.SelectorGroupedRT{}
		{

			for _, filter := range queryData.Select {

				columnName := filter.Arguments[len(filter.Arguments)-1].(string)

				old, isOk := selectorsByColumns[columnName]
				if !isOk {
					old = []query.Selector{}
				}

				selectorsByColumns[columnName] = append(old, filter)
			}

			for fname, selectorsByField := range selectorsByColumns {

				if len(selectorsByField) > 1 {
					return query.QueryPlan{}, fmt.Errorf("multiple selectors on same column not supported (yet?)")
				}

				if fname == "*" {
					slog.Debug("selector on * skipped, not implemented")
					continue
				}

				var columnInfo schema.SchemaColumn
				columnIdx := 0

				// all fields must exist, as they were checked above
				for idx, it := range schemaObject.Columns {
					if it.Name == fname {
						columnInfo = it
						columnIdx = idx
						break
					}
				}

				selectorsByColumnsArray = append(selectorsByColumnsArray, query.SelectorGroupedRT{
					FieldName:        fname,
					ColumnIdx:        columnIdx,
					ColumnSchemaInfo: &columnInfo,
					Selectors:        selectorsByField,
				})
			}

			// sort by name
			// for consistency of results
			slices.SortStableFunc(selectorsByColumnsArray, func(a, b query.SelectorGroupedRT) int {
				return strings.Compare(a.FieldName, b.FieldName)
			})
		}

		// total size of blocks in all segments == ExecutorChunkSizeBlocks

		newSingleChunk := func() *query.SingleChunk {
			return &query.SingleChunk{Segments: make([]query.Segment, 0, query.ExecutorChunkSizeBlocks)}
		}

		maxChunks := 0

		type SkipArrayCacheEntry struct {
			Full    int8
			Partial int8
			None    int8
		}

		rtCache, err := metaManager.GetCacheForSchema(schemaObject)
		if err != nil {
			return query.QueryPlan{}, fmt.Errorf("error getting rt cache for schema: %s", err.Error())
		}

		absBlocksFullSkipArray := make([]SkipArrayCacheEntry, rtCache.MaxBlocks)

		// filter slab headers
		blockPrunningStart := time.Now()
		for _, filtersGroup := range filterByColumnsArray {
			slabs := rtCache.SlabsByColumns[filtersGroup.FieldName]

			for _, slabUid := range slabs {
				for _, filter := range filtersGroup.Conditions {

					slabInfo, slabLoadErr := slabManager.LoadSlabHeaderToCache(schemaObject, slabUid)
					if slabLoadErr != nil {
						return query.QueryPlan{}, fmt.Errorf("error loading slab into cache : %s", slabLoadErr.Error())
					}

					blockHeaders := slabInfo.BlockHeaders
					for i := 0; i < int(slabInfo.BlocksFinalized); i++ {

						blockHeader := &blockHeaders[i]

						if i > int(slabInfo.BlocksFinalized) {
							break
						}

						var matchResult schema.BoundsFilterMatchResult
						var matchErr error

						ftype := filtersGroup.ColumnSchemaInfo.Type

						switch ftype {
						case schema.Uint64FieldType:
							matchResult, matchErr = filters.ProcessFilterOnBounds[uint64](filter.Filter, &blockHeader.Bounds)
						case schema.Float32FieldType:
							matchResult, matchErr = filters.ProcessFilterOnBounds[float32](filter.Filter, &blockHeader.Bounds)

						default:
							panic(fmt.Sprintf("unsupported type in query planner : %s (field_name : %s)", ftype.String(), filtersGroup.FieldName))
						}

						if matchErr != nil {
							return query.QueryPlan{}, fmt.Errorf("error filtering bounds on block header : %s", matchErr.Error())
						}

						absOffset := i + int(slabInfo.SlabOffsetBlocks)

						if matchResult == schema.NoIntersection {
							absBlocksFullSkipArray[absOffset].None += 1
						}

						if matchResult == schema.FullIntersection {
							absBlocksFullSkipArray[absOffset].Full += 1
						}

						if matchResult == schema.PartialIntersection || matchResult == schema.UnknownIntersection {
							absBlocksFullSkipArray[absOffset].Partial += 1
						}

					}
				}
			}
		}

		blockPrunningTook := time.Since(blockPrunningStart).Seconds() * 1000.0

		if false {

			blocksToSkip := 0
			blocksOk := 0
			blocksFull := 0

			for _, skip := range absBlocksFullSkipArray {
				if skip.None > 0 {
					blocksToSkip += 1
				} else {

					// slog.Info("normal block", "blocks_info", []int8{
					// 	skip.None, skip.Full, skip.Partial,
					// })

					if skip.Full == 3 {
						blocksFull += 1
					} else {
						blocksOk += 1
					}
				}
			}
			slog.Info("blocks prunned", "took", fmt.Sprintf("%.4fms", blockPrunningTook), "prunned_blocks", blocksToSkip, "good_blocks", blocksOk, "full_blocks", blocksFull)
		}

		perColumnChunks := make(map[int]*query.ColumnChunks, len(schemaObject.Columns))

		for columnIdx, columnDef := range schemaObject.Columns {

			found := false

			for _, selectingField := range fieldsList {
				if selectingField.name == columnDef.Name {
					found = true
					break
				}
			}

			if !found {
				continue
			}

			// before := time.Now()

			blocksPerSlab := int(columnDef.Type.BlocksPerSlab())

			curChunkSlabs, ok := perColumnChunks[columnIdx]
			if !ok {
				curChunkSlabs = &query.ColumnChunks{List: make([]query.SingleChunk, 0, query.ExecutorChunkSizeBlocks)}
				perColumnChunks[columnIdx] = curChunkSlabs
			}

			curChunkSlabsItem := newSingleChunk()

			absSlabBase := 0

			for _, slabUid := range columnDef.Slabs {

				block := 0
				for block < blocksPerSlab {

					// skip blacklisted blocks
					absBlockIdx := absSlabBase + block
					if absBlocksFullSkipArray[absBlockIdx].None > 0 {
						block++
						continue
					}

					leftoverChunk := query.ExecutorChunkSizeBlocks - curChunkSlabsItem.BlocksFilled
					if leftoverChunk == 0 {
						curChunkSlabs.List = append(curChunkSlabs.List, *curChunkSlabsItem)

						curChunkSlabsItem = newSingleChunk()
						continue
					}

					start := block
					size := 0

					for block < blocksPerSlab && size < leftoverChunk {
						absBlockIdx = absSlabBase + block
						if absBlocksFullSkipArray[absBlockIdx].None > 0 {
							break
						}
						size++
						block++
					}

					if size > 0 {
						curChunkSlabsItem.Segments = append(curChunkSlabsItem.Segments, query.Segment{
							Slab:       slabUid,
							StartBlock: start,
							Size:       size,
						})
						curChunkSlabsItem.BlocksFilled += size
					}
				}

				absSlabBase += blocksPerSlab
			}

			if curChunkSlabsItem.BlocksFilled > 0 {
				curChunkSlabs.List = append(curChunkSlabs.List, *curChunkSlabsItem)
			}

			if len(curChunkSlabs.List) > maxChunks {
				maxChunks = len(curChunkSlabs.List)
			}

			// if !found {
			// 	tTook := time.Since(before)

			// 	_ = tTook
			// 	wastedTotal := timeWasted.Add(int32(tTook))
			// 	if wastedTotal > 10000000 {
			// 		color.Cyan("wasting cpu time: %.3fms ", time.Duration(wastedTotal).Seconds()*1000)
			// 	}
			// }
		}

		chunks := make([]query.BlockChunk, maxChunks)
		fieldsCount := len(schemaObject.Columns)

		for columnIdx, perColumnChunk := range perColumnChunks {

			for chunkIdx, chunk := range perColumnChunk.List {

				curChunkObject := &chunks[chunkIdx]

				if curChunkObject.ChunkSegmentsByFieldIndexMap == nil {

					// include selector unique fields
					curChunkObject.ChunkSegmentsByFieldIndexMap = make([][]query.Segment, fieldsCount)
					allocatedchunks += fieldsCount
					curChunkObject.GlobalBlockOffset = uint64(chunkIdx) * query.ExecutorChunkSizeBlocks
				}

				curChunkObject.ChunkSegmentsByFieldIndexMap[columnIdx] = chunk.Segments
			}
		}

		selectOnlyFieldsArr := make([]bool, fieldsCount)

		for fieldName := range selectOnlyFields {
			idx := -1

			for schemaIdx, schemaColumn := range schemaObject.Columns {
				if fieldName == schemaColumn.Name {
					idx = schemaIdx
					break
				}
			}

			selectOnlyFieldsArr[idx] = selectOnlyFields[schemaObject.Columns[idx].Name]
		}

		return query.QueryPlan{
			Schema:                   *schemaObject,
			SelectOnlyColumns:        selectOnlyFieldsArr,
			FilterGroupedByFields:    filterByColumnsArray,
			BlockChunks:              chunks,
			FilterSize:               len(queryData.Filter),
			SelectorsGroupedByFields: selectorsByColumnsArray,
			Id:                       queryGlobalId.Add(1),
		}, nil

	}

}

var queryGlobalId atomic.Int64

var allocatedchunks = 0

func AllocatedChunks() int { return allocatedchunks }
