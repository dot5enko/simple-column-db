package query

import "github.com/dot5enko/simple-column-db/schema"

type RuntimeFilterCache struct {
	// column                      schema.SchemaColumn
	filterLastBlockHeaderResult schema.BoundsFilterMatchResult
}

type FilterConditionRuntime struct {
	filter  FilterCondition
	runtime *RuntimeFilterCache
}

type FilterGroupedRT struct {
	FieldName string

	ColumnSchemaInfo *schema.SchemaColumn
	ColumnIdx        int

	Conditions []FilterConditionRuntime
}
