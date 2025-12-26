package manager

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dot5enko/simple-column-db/manager/executor/filters"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type QueryPlanner struct {
}

func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

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

		// check that all fields are valid
		for _, filter := range queryData.Filter {

			found := false
			for _, it := range schemaObject.Columns {
				if it.Name == filter.Field {
					found = true
					break
				}
			}

			if !found {
				return query.QueryPlan{}, fmt.Errorf("column `%v` not found on schema `%v`", filter.Field, schemaName)
			}
		}

		// slabs
		slabsFiltered := []uuid.UUID{}
		// skippedBlocksDueToHeaderFiltering := 0

		// full scan of all slabs and their blocks
		slabsByColumns := map[string][]uuid.UUID{}

		type ColumnPrecachedInfo struct {
			BlocksPerSlab int16
		}
		columnPrecalculatedInfo := map[string]ColumnPrecachedInfo{}

		// collect slabs
		maxBlocks := 0
		for _, it := range schemaObject.Columns {

			fieldBlocksPerSlab := it.Type.BlocksPerSlab()
			columnPrecalculatedInfo[it.Name] = ColumnPrecachedInfo{
				BlocksPerSlab: fieldBlocksPerSlab,
			}

			if len(it.Slabs) > 0 {

				// global
				slabsFiltered = append(slabsFiltered, it.Slabs...)

				old, isOk := slabsByColumns[it.Name]
				if !isOk {
					old = []uuid.UUID{}
					slabsByColumns[it.Name] = old
				}

				slabsByColumns[it.Name] = append(old, it.Slabs...)

				slabsSize := len(slabsByColumns[it.Name])
				blocksAtMax := slabsSize * int(fieldBlocksPerSlab)
				if blocksAtMax > maxBlocks {
					maxBlocks = blocksAtMax
				}
			}
		}

		// group filters by columns
		filtersByColumns := map[string][]query.FilterConditionRuntime{}
		for _, filter := range queryData.Filter {
			old, isOk := filtersByColumns[filter.Field]
			if !isOk {
				old = []query.FilterConditionRuntime{}
			}

			filtersByColumns[filter.Field] = append(old, query.FilterConditionRuntime{
				Filter: filter,
			})
		}

		filterByColumnsArray := []query.FilterGroupedRT{}
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

		// total size of blocks in all segments == ExecutorChunkSizeBlocks

		perColumnChunks := map[int]*query.ColumnChunks{}

		newSingleChunk := func() *query.SingleChunk {
			return &query.SingleChunk{Segments: []query.Segment{}}
		}

		maxChunks := 0

		absBlocksFullSkipArray := make([]uint8, maxBlocks)

		// filter slab headers
		// blockPrunningStart := time.Now()
		for _, filtersGroup := range filterByColumnsArray {
			slabs := slabsByColumns[filtersGroup.FieldName]

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

						if matchResult == schema.NoIntersection {
							absOffset := i + int(slabInfo.SlabOffsetBlocks)
							absBlocksFullSkipArray[absOffset] = 1
						}
					}
				}
			}
		}

		// blockPrunningTook := time.Since(blockPrunningStart).Seconds() * 1000.0

		blocksToSkip := 0
		blocksOk := 0
		for _, skip := range absBlocksFullSkipArray {
			if skip == 1 {
				blocksToSkip += 1
			} else {
				blocksOk += 1
			}
		}

		// slog.Info("blocks prunned", "took", fmt.Sprintf("%.4fms", blockPrunningTook), "prunned_blocks", blocksToSkip, "good_blocks", blocksOk)

		// chunk generator
		if false {
			for columnIdx, columnDef := range schemaObject.Columns {

				blocksPerSlab := columnDef.Type.BlocksPerSlab()

				curChunkSlabs, ok := perColumnChunks[columnIdx]
				if !ok {
					curChunkSlabs = &query.ColumnChunks{List: []query.SingleChunk{}}
					perColumnChunks[columnIdx] = curChunkSlabs
				}

				var curChunkSlabsItem = newSingleChunk()

				for _, slabUid := range columnDef.Slabs {
					leftoverBlocks := int(blocksPerSlab)
					used := 0

					for leftoverBlocks > 0 {

						curSize := query.ExecutorChunkSizeBlocks

						if leftoverBlocks <= query.ExecutorChunkSizeBlocks {
							curSize = leftoverBlocks
						}

						leftoverCurrentChunk := query.ExecutorChunkSizeBlocks - curChunkSlabsItem.BlocksFilled

						if curSize > leftoverCurrentChunk {
							curSize = leftoverCurrentChunk
						}

						segment := query.Segment{
							Slab:       slabUid,
							StartBlock: used,
							Size:       curSize,
						}

						leftoverBlocks -= curSize
						used += curSize
						curChunkSlabsItem.BlocksFilled += curSize

						curChunkSlabsItem.Segments = append(curChunkSlabsItem.Segments, segment)

						if curChunkSlabsItem.BlocksFilled > query.ExecutorChunkSizeBlocks {
							panic(fmt.Sprintf("this should not happen. never. Number of blocks filled %d, exceeds executor chunk size %d", curChunkSlabsItem.BlocksFilled, query.ExecutorChunkSizeBlocks))
						}

						if curChunkSlabsItem.BlocksFilled == query.ExecutorChunkSizeBlocks {
							curChunkSlabs.List = append(curChunkSlabs.List, *curChunkSlabsItem)
							curChunkSlabsItem = newSingleChunk()
						}
					}
				}

				curChunks := len(curChunkSlabs.List)
				if curChunks > maxChunks {
					maxChunks = curChunks
				}
			}
		}

		for columnIdx, columnDef := range schemaObject.Columns {

			blocksPerSlab := int(columnDef.Type.BlocksPerSlab())

			curChunkSlabs, ok := perColumnChunks[columnIdx]
			if !ok {
				curChunkSlabs = &query.ColumnChunks{List: []query.SingleChunk{}}
				perColumnChunks[columnIdx] = curChunkSlabs
			}

			curChunkSlabsItem := newSingleChunk()

			absSlabBase := 0

			for _, slabUid := range columnDef.Slabs {

				block := 0
				for block < blocksPerSlab {

					// skip blacklisted blocks
					absBlockIdx := absSlabBase + block
					if absBlocksFullSkipArray[absBlockIdx] != 0 {
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
						if absBlocksFullSkipArray[absBlockIdx] != 0 {
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
		}

		chunks := make([]query.BlockChunk, maxChunks)
		fieldsCount := len(schemaObject.Columns)

		for columnIdx, perColumnChunk := range perColumnChunks {

			for chunkIdx, chunk := range perColumnChunk.List {

				curChunkObject := &chunks[chunkIdx]

				if curChunkObject.ChunkSegmentsByFieldIndexMap == nil {
					curChunkObject.ChunkSegmentsByFieldIndexMap = make([][]query.Segment, fieldsCount)
					curChunkObject.GlobalBlockOffset = uint64(chunkIdx) * query.ExecutorChunkSizeBlocks
				}

				curChunkObject.ChunkSegmentsByFieldIndexMap[columnIdx] = chunk.Segments
			}
		}

		return query.QueryPlan{
			Schema:                *schemaObject,
			FilterGroupedByFields: filterByColumnsArray,
			BlockChunks:           chunks,
			FilterSize:            len(queryData.Filter),
		}, nil

	}

}
