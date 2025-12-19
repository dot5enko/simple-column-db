package manager

import (
	"errors"
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
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

		// slabs

		slabsFiltered := []uuid.UUID{}

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
		filtersByColumns := map[string][]FilterCondition{}
		for _, filter := range query.Filter {
			old, isOk := filtersByColumns[filter.Field]
			if !isOk {
				old = []FilterCondition{}
			}

			filtersByColumns[filter.Field] = append(old, filter)
		}

		// spew.Dump("filter by columns", filtersByColumns)
		// spew.Dump("slabs filtered", slabsByColumns)

		for columnName, filterColumn := range filtersByColumns {
			for _, slab := range slabsByColumns[columnName] {

				color.Red(" -- slab processing by field name : %s. slab %s ", columnName, slab.String())

				slabInfo, slabErr := sm.Slabs.LoadSlabToCache(*schemaObject, slab)
				if slabErr != nil {
					return nil, fmt.Errorf("unable to load slab : %s", slabErr.Error())
				}

				var blocks []BlockRuntimeInfo
				// filter slab blocks by filter

				for idx, blockHeader := range slabInfo.BlockHeaders {
					if idx > int(slabInfo.BlocksFinalized) {
						break
					}

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

					blockGroupMerger := lists.NewUnmerged(mergeIndicesCache)

					for _, filter := range filterColumn {

						var columnInfo schema.SchemaColumn

						// cache
						for _, it := range schemaObject.Columns {
							if it.Name == filter.Field {
								columnInfo = it
								break
							}
						}

						// if filter.Field != columnInfo.Name {
						// 	continue
						// }

						var processFilterErr error

						// process filter on a block
						switch columnInfo.Type {
						case schema.Uint64FieldType:
							processFilterErr = ProcessNumericFilterOnColumnWithType[uint64](slabInfo, filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Uint8FieldType:
							processFilterErr = ProcessNumericFilterOnColumnWithType[uint8](slabInfo, filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float32FieldType:
							processFilterErr = ProcessNumericFilterOnColumnWithType[float32](slabInfo, filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float64FieldType:
							processFilterErr = ProcessNumericFilterOnColumnWithType[float64](slabInfo, filter, &blockData, blockGroupMerger, indicesResultCache[:])
						default:
							return nil, fmt.Errorf("unsupported type %v", columnInfo.Type.String())
						}

						if processFilterErr != nil {
							return nil, fmt.Errorf("error filter processing : %s", processFilterErr.Error())
						}

					}

					// we can use here indicesResultCache again as we copied the result into blockGroupMerger buf
					mergedSize := blockGroupMerger.Merge(indicesCounter[:], indicesResultCache[:])
					mergedIndices := indicesResultCache[:mergedSize]

					log.Printf("filterd indices in block: %v", len(mergedIndices))
				}

			}

		}
	}

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)

func ProcessNumericFilterOnColumnWithType[T ops.NumericTypes](
	slab *schema.DiskSlabHeader,
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

	// log.Printf("[slab %s] processing numeric filter on column %v, type = %s", slab.Uid.String(), filter.Field, blockData.Header.DataType.String())

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	switch filter.Operand {
	case RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRange(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		// if itemsFiltered > 0 {
		// 	log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.Header.Uid.String())
		// 	color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.Header.Uid.String(), blockData.Header.Bounds.Min, blockData.Header.Bounds.Max)
		// }
	case EQ:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareNumericValuesAreEqual(inputArray, operand, indicesCache)

	case GT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreBigger(inputArray, operand, indicesCache)

	default:
		return fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, blockData.Header.DataType.String())
	}

	merger.With(indicesCache[:itemsFiltered])

	return nil

}
