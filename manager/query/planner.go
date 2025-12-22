package query

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

var (
	ErrSchemaNotFound = fmt.Errorf("schema not found")
)

type QueryPlanner struct {
}

func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{}
}

func (qp *QueryPlanner) Plan(
	schemaName string,
	queryData Query,
	metaManager *meta.MetaManager,
) (any, error) {
	schemaObject := metaManager.GetSchema(schemaName)
	if schemaObject == nil {
		return nil, ErrSchemaNotFound
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
				return nil, fmt.Errorf("column `%v` not found on schema `%v`", filter.Field, schemaName)
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

		type RuntimeFilterCache struct {
			column                      schema.SchemaColumn
			filterLastBlockHeaderResult schema.BoundsFilterMatchResult
		}

		type FilterConditionRuntime struct {
			filter  FilterCondition
			runtime *RuntimeFilterCache
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

	}

	panic("not implemented")
	return nil, nil
}
