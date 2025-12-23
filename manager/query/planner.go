package query

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

var (
	ErrSchemaNotFound = fmt.Errorf("schema not found")
)

type (
	QueryPlanner struct {
	}

	Segment struct {
		Slab uuid.UUID

		StartBlock int
		Size       int
	}

	BlockChunk struct {
		GlobalBlockOffset uint64

		// for each field there will be an array of segments
		// thats why we need a "map" here, for speed we use numeric array instead
		// indices correspond to the order of fields in schema object
		ChunkSegmentsByFieldIndexMap [][]Segment
	}

	QueryPlan struct {
		Schema                schema.Schema
		FilterGroupedByFields []FilterGroupedRT
		BlockChunks           []BlockChunk
	}
)

const ExecutorChunkSizeBlocks = 20

func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

func (qp *QueryPlanner) Plan(
	schemaName string,
	queryData Query,
	metaManager *meta.MetaManager,

) (QueryPlan, error) {
	schemaObject := metaManager.GetSchema(schemaName)
	if schemaObject == nil {
		return QueryPlan{}, ErrSchemaNotFound
	} else {

		// should be big enough to hold all the entries to
		// todo replace with bitset
		// mergeIndicesCache := make([]uint16, schema.BlockRowsSize*len(query.Filter))
		// var indicesCounter [schema.BlockRowsSize]uint16

		// check fields before filtering data
		for _, filter := range queryData.Filter {

			found := false
			for _, it := range schemaObject.Columns {
				if it.Name == filter.Field {
					found = true
					break
				}
			}

			if !found {
				return QueryPlan{}, fmt.Errorf("column `%v` not found on schema `%v`", filter.Field, schemaName)
			}
		}

		// slabs

		slabsFiltered := []uuid.UUID{}
		// skippedBlocksDueToHeaderFiltering := 0

		// full scan of all slabs and their blocks
		slabsByColumns := map[string][]uuid.UUID{}

		for _, it := range schemaObject.Columns {
			if len(it.Slabs) > 0 {

				// global
				slabsFiltered = append(slabsFiltered, it.Slabs...)

				old, isOk := slabsByColumns[it.Name]
				if !isOk {
					old = []uuid.UUID{}
					slabsByColumns[it.Name] = old
				}

				// todo filter by header bounds, etc
				slabsByColumns[it.Name] = append(old, it.Slabs...)
			}
		}

		// group filters by columns
		filtersByColumns := map[string][]FilterConditionRuntime{}
		for _, filter := range queryData.Filter {
			old, isOk := filtersByColumns[filter.Field]
			if !isOk {
				old = []FilterConditionRuntime{}
			}

			filtersByColumns[filter.Field] = append(old, FilterConditionRuntime{
				Filter:  filter,
				Runtime: &RuntimeFilterCache{},
			})
		}

		filterByColumnsArray := []FilterGroupedRT{}
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

			filterByColumnsArray = append(filterByColumnsArray, FilterGroupedRT{
				FieldName:        fname,
				Conditions:       it,
				ColumnSchemaInfo: &columnInfo,
				ColumnIdx:        columnIdx,
			})
		}

		// sort by name
		// for consistency of results
		slices.SortStableFunc(filterByColumnsArray, func(a, b FilterGroupedRT) int {
			return strings.Compare(a.FieldName, b.FieldName)
		})

		// total size of blocks in all segments == ExecutorChunkSizeBlocks
		type SingleChunk struct {
			segments      []Segment
			blocks_filled int
		}

		type ColumnChunks struct {
			List []SingleChunk
		}

		perColumnChunks := map[int]*ColumnChunks{}

		newSingleChunk := func() *SingleChunk {
			return &SingleChunk{segments: []Segment{}}
		}

		maxChunks := 0

		for columnIdx, columnDef := range schemaObject.Columns {

			blocksPerSlab := columnDef.Type.BlocksPerSlab()

			curChunkSlabs, ok := perColumnChunks[columnIdx]
			if !ok {
				curChunkSlabs = &ColumnChunks{List: []SingleChunk{}}
				perColumnChunks[columnIdx] = curChunkSlabs
			}

			var curChunkSlabsItem = newSingleChunk()

			for _, slabUid := range columnDef.Slabs {
				leftoverBlocks := int(blocksPerSlab)
				used := 0

				for leftoverBlocks > 0 {

					curSize := ExecutorChunkSizeBlocks

					if leftoverBlocks <= ExecutorChunkSizeBlocks {
						curSize = leftoverBlocks
					}

					leftoverCurrentChunk := ExecutorChunkSizeBlocks - curChunkSlabsItem.blocks_filled

					if curSize > leftoverCurrentChunk {
						curSize = leftoverCurrentChunk
					}

					segment := Segment{
						Slab:       slabUid,
						StartBlock: used,
						Size:       curSize,
					}

					leftoverBlocks -= curSize
					used += curSize
					curChunkSlabsItem.blocks_filled += curSize

					curChunkSlabsItem.segments = append(curChunkSlabsItem.segments, segment)

					if curChunkSlabsItem.blocks_filled > ExecutorChunkSizeBlocks {
						panic(fmt.Sprintf("this should not happen. never. Number of blocks filled %d, exceeds executor chunk size %d", curChunkSlabsItem.blocks_filled, ExecutorChunkSizeBlocks))
					}

					if curChunkSlabsItem.blocks_filled == ExecutorChunkSizeBlocks {
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

		chunks := make([]BlockChunk, maxChunks)
		fieldsCount := len(schemaObject.Columns)

		for columnIdx, perColumnChunk := range perColumnChunks {

			for chunkIdx, chunk := range perColumnChunk.List {

				curChunkObject := &chunks[chunkIdx]

				if curChunkObject.ChunkSegmentsByFieldIndexMap == nil {
					curChunkObject.ChunkSegmentsByFieldIndexMap = make([][]Segment, fieldsCount)
					curChunkObject.GlobalBlockOffset = uint64(chunkIdx) * ExecutorChunkSizeBlocks
				}

				curChunkObject.ChunkSegmentsByFieldIndexMap[columnIdx] = chunk.segments
			}
		}

		return QueryPlan{
			Schema:                *schemaObject,
			FilterGroupedByFields: filterByColumnsArray,
			BlockChunks:           chunks,
		}, nil

	}

}
