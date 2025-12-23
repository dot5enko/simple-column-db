package query

import "github.com/dot5enko/simple-column-db/schema"

type RuntimeFilterCache struct {
	// column                      schema.SchemaColumn
	FilterLastBlockHeaderResult schema.BoundsFilterMatchResult
}

type FilterConditionRuntime struct {
	Filter  FilterCondition
	Runtime *RuntimeFilterCache
}

type FilterGroupedRT struct {
	FieldName string

	ColumnSchemaInfo *schema.SchemaColumn
	ColumnIdx        int

	Conditions []FilterConditionRuntime
}
