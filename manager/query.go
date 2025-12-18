package manager

import (
	"errors"
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type CondOperand byte

const (
	EQ CondOperand = iota
	GT
	LT
	RANGE
)

type FilterCondition struct {
	Field     string
	Operand   CondOperand
	Arguments []any
}

type SelectorType byte

const (
	SelectFunction SelectorType = iota
)

type Selector struct {
	Type      SelectorType
	Arguments []any

	Alias string
}

type Query struct {
	Filter []FilterCondition
	Select []Selector
}

func (sm *Manager) Get(
	schemaName string,
	query Query,
) ([]map[string]any, error) {

	result := []map[string]any{}

	schemaObject, ok := sm.schemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("schema not found")
	} else {

		// should be big enough to hold all the entries to
		// todo replace with bitset
		mergeIndicesCache := make([]uint16, schema.BlockRowsSize*len(query.Filter))

		var indicesResultCache [schema.BlockRowsSize]uint16
		var indicesCounter [schema.BlockRowsSize]uint16

		// check fields before filtering data
		for _, filter := range query.Filter {

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

		// todo cache
		// this is a blockmanager responsibility to load blocks from disk if they are not loaded yet

		slabsFiltered := []uuid.UUID{}

		// full scan of all slabs and their blocks
		for _, it := range schemaObject.Columns {
			if len(it.Slabs) > 0 {

				// todo filter by header bounds, etc
				slabsFiltered = append(slabsFiltered, it.Slabs...)
			}
		}

		for _, slab := range slabsFiltered {

			slabInfo, slabErr := sm.Slabs.LoadSlabToCache(*schemaObject, slab)
			if slabErr != nil {
				return nil, fmt.Errorf("unable to load slab : %s", slabErr.Error())
			}

			blockGroupMerger := lists.NewUnmerged(mergeIndicesCache)

			var blocks []BlockRuntimeInfo
			// filter slab blocks by filter

			for _, blockHeader := range slabInfo.BlockHeaders {

				// filter by headers if possible
				blockDecodedInfo, blockErr := sm.Slabs.LoadBlockToRuntimeBlockData(*schemaObject, slabInfo, blockHeader.Uid)

				if blockErr != nil {
					return nil, fmt.Errorf("unable to decode block : %s", blockErr.Error())
				}

				blocks = append(blocks, BlockRuntimeInfo{
					Val:          blockDecodedInfo,
					Header:       blockHeader,
					Synchronized: true,
				})
			}

			for _, blockData := range blocks {
				for _, filter := range query.Filter {

					var columnInfo schema.SchemaColumn

					// cache
					for _, it := range schemaObject.Columns {
						if it.Name == filter.Field {
							columnInfo = it
							break
						}
					}

					// process filter on a block
					switch columnInfo.Type {
					case schema.Uint64FieldType:
						ProcessNumericFilterOnColumnWithType[uint64](filter, &blockData, blockGroupMerger, indicesResultCache[:])
					case schema.Uint8FieldType:
						ProcessNumericFilterOnColumnWithType[uint8](filter, &blockData, blockGroupMerger, indicesResultCache[:])
					case schema.Float32FieldType:
						ProcessNumericFilterOnColumnWithType[float32](filter, &blockData, blockGroupMerger, indicesResultCache[:])
					case schema.Float64FieldType:
						ProcessNumericFilterOnColumnWithType[float64](filter, &blockData, blockGroupMerger, indicesResultCache[:])
					default:
						return nil, fmt.Errorf("unsupported type %v", columnInfo.Type.String())
					}

				}
			}

			// we can use here indicesResultCache again as we copied the result into blockGroupMerger buf
			mergedSize := blockGroupMerger.Merge(indicesCounter[:], indicesResultCache[:])
			mergedIndices := indicesResultCache[:mergedSize]

			log.Printf("filterd indices in block: %v", mergedIndices)
		}
	}

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)

func ProcessNumericFilterOnColumnWithType[T ops.NumericTypes](
	filter FilterCondition,
	blockData *BlockRuntimeInfo,
	merger *lists.IndiceUnmerged,
	indicesCache []uint16,
) error {

	var itemsFiltered int

	runtimeBlockInfo, rtBlockInfoOk := blockData.Val.(*schema.RuntimeBlockData)
	if !rtBlockInfoOk {
		return ErrRuntimeBlockInfoTypeIsIncorrect
	}

	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	switch filter.Operand {
	case RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		itemsFiltered = ops.CompareValuesAreInRange(inputArray, operandA, operandB, indicesCache)

	case EQ:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareNumericValuesAreEqual(inputArray, operand, indicesCache)

	case GT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreBigger(inputArray, operand, indicesCache)

	default:
		return fmt.Errorf("unsupported operand %v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, blockData.Header.DataType.String())
	}

	merger.With(indicesCache[:itemsFiltered])

	return nil

}
