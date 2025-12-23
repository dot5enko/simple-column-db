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

	SlabField struct {
		Slab uuid.UUID

		StartBlock int
		Size       int
	}

	BlockChunk struct {
		SlabsByFields [][]SlabField
	}

	QueryPlan struct {
		FilterGroupedByFields []FilterGroupedRT
		BlockChunks           []BlockChunk
	}
)

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
				filter:  filter,
				runtime: &RuntimeFilterCache{},
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

		chunkSizeBlocks := 20

		type OneChunkSlabs struct {
			items []SlabField
			used  int
		}

		type ColumnChunks struct {
			List []OneChunkSlabs
		}

		perColumnChunks := map[int]*ColumnChunks{}

		newSingleChunk := func() *OneChunkSlabs {
			return &OneChunkSlabs{items: []SlabField{}}
		}

		maxChunks := 0

		for columnIdx, columnDef := range schemaObject.Columns {

			blocksPerSlab := columnDef.Type.BlocksPerSlab()

			curChunkSlabs, ok := perColumnChunks[columnIdx]
			if !ok {
				curChunkSlabs = &ColumnChunks{List: []OneChunkSlabs{}}
				perColumnChunks[columnIdx] = curChunkSlabs
			}

			var curChunkSlabsItem = newSingleChunk()

			for _, slabUid := range columnDef.Slabs {
				leftoverBlocks := int(blocksPerSlab)
				used := 0

				for leftoverBlocks > 0 {

					curSize := chunkSizeBlocks

					if leftoverBlocks <= chunkSizeBlocks {
						curSize = leftoverBlocks
					}

					leftoverCurrentChunk := chunkSizeBlocks - curChunkSlabsItem.used

					if curSize > leftoverCurrentChunk {
						curSize = leftoverCurrentChunk
					}

					slabFieldInfo := SlabField{
						Slab:       slabUid,
						StartBlock: used,
						Size:       curSize,
					}

					leftoverBlocks -= curSize
					used += curSize
					curChunkSlabsItem.used += curSize

					curChunkSlabsItem.items = append(curChunkSlabsItem.items, slabFieldInfo)

					if curChunkSlabsItem.used > chunkSizeBlocks {
						panic("this should not happen")
					}

					if curChunkSlabsItem.used == chunkSizeBlocks {
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

				if chunks[chunkIdx].SlabsByFields == nil {
					chunks[chunkIdx].SlabsByFields = make([][]SlabField, fieldsCount)
				}

				chunks[chunkIdx].SlabsByFields[columnIdx] = chunk.items
			}
		}

		return QueryPlan{
			FilterGroupedByFields: filterByColumnsArray,
			BlockChunks:           chunks,
		}, nil

	}

}
