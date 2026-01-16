package query

import (
	"encoding/binary"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/schema"
)

type RuntimeFilterCache struct {
	// column                      schema.SchemaColumn
	FilterLastBlockHeaderResult schema.BoundsFilterMatchResult
	FilterBounds                schema.BoundsFloat
}

type FilterConditionRuntime struct {
	Filter FilterCondition

	UniqueId     schema.FilterIdType
	calculatedId bool
	// Runtime *RuntimeFilterCache
}

func (r *FilterConditionRuntime) GetUniqueId(schemaName string) schema.FilterIdType {
	if r.calculatedId == false {

		writer := bits.NewEncodeBuffer(r.UniqueId[:], binary.LittleEndian)

		writer.WriteByte(uint8(r.Filter.Operand))
		writer.Write([]byte(schemaName))
		writer.Write([]byte(r.Filter.Field))

		for _, cond := range r.Filter.Arguments {

			switch v := cond.(type) {
			case uint64:
				writer.PutUint64(v)
			case uint32:
				writer.PutUint32(v)
			case uint16:
				writer.PutUint16(v)
			case uint8:
				writer.WriteByte(v)
			case int64:
				writer.PutInt64(v)
			case int32:
				writer.PutInt32(v)
			case int16:
				writer.PutUint16(uint16(v))
			case float32:
				writer.PutFloat32(v)
			case float64:
				writer.PutFloat64(v)
			}

		}

	}

	return r.UniqueId
}

type FilterGroupedRT struct {
	FieldName string

	ColumnSchemaInfo *schema.SchemaColumn
	ColumnIdx        int

	Conditions []FilterConditionRuntime
}

type SelectorGroupedRT struct {
	FieldName        string
	ColumnIdx        int
	ColumnSchemaInfo *schema.SchemaColumn

	Selectors []Selector
}
