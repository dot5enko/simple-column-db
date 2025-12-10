package schemamanager

// import (
// 	"fmt"

// 	"github.com/dot5enko/simple-column-db/block"
// 	"github.com/dot5enko/simple-column-db/ops"
// 	"github.com/dot5enko/simple-column-db/schema"
// )

// type BlockRuntimeInfo struct {
// 	Val          any
// 	Synchronized bool
// 	Header       block.DiskHeader
// }

// type SchemaManager struct {
// 	schemas map[string]schema.Schema
// 	blocks  map[block.BlockUniqueId]BlockRuntimeInfo
// }

// type CondOperand byte

// const (
// 	EQ CondOperand = iota
// 	GT
// 	LT
// 	RANGE
// )

// type FilterCondition struct {
// 	Field     string
// 	Operand   CondOperand
// 	Arguments []any
// }

// type SelectorType byte

// const (
// 	SelectField SelectorType = iota
// 	SelectFunction
// )

// type Selector struct {
// 	Type      SelectorType
// 	Arguments []any

// 	Alias string
// }

// type Query struct {
// 	Filter []FilterCondition
// 	Select []Selector
// }

// const blockSize = 4000

// func (sm *SchemaManager) Get(
// 	schemaName string,
// 	query Query,
// ) ([]map[string]any, error) {

// 	result := []map[string]any{}

// 	schemaObject, ok := sm.schemas[schemaName]
// 	if !ok {
// 		return nil, fmt.Errorf("schema not found")
// 	} else {
// 		for _, filter := range query.Filter {

// 			var columnInfo *schema.SchemaColumn

// 			for _, it := range schemaObject.Columns {
// 				if it.Name == filter.Field {
// 					columnInfo = &it
// 					break
// 				}
// 			}

// 			if columnInfo == nil {
// 				return nil, fmt.Errorf("column `%v` not found on schema `%v`", filter.Field, schemaName)
// 			}

// 			// todo cache
// 			// this is a blockmanager responsibility to load blocks from disk if they are not loaded yet

// 			var filterResultUint64 [blockSize]uint64
// 			var blockDataUint64 [blockSize]uint64

// 			for _, columnBlock := range schemaObject.Blocks {

// 				fieldBlockUid := block.NewBlockUniqueId(columnBlock, columnInfo.Id)

// 				// block manager code
// 				blockData, blockOk := sm.blocks[fieldBlockUid]
// 				{
// 					if !blockOk {
// 						return nil, fmt.Errorf("block not found while processing query : %s", fieldBlockUid.MustUid().String())
// 					}

// 					if !(blockData.Synchronized) {
// 						return nil, fmt.Errorf("block %s not synchronized from disk", fieldBlockUid.MustUid().String())
// 					}
// 				}

// 				blockFiltersToMerge := [][]uint64{}

// 				// process filter on a block
// 				switch columnInfo.Type {
// 				case schema.Int8FieldType,
// 					schema.Uint64FieldType:

// 					switch filter.Operand {
// 					case RANGE:
// 						operandA := filter.Arguments[0].(uint64)
// 						operandB := filter.Arguments[1].(uint64)

// 						runtimeBlockInfo, rtBlockInfoOk := blockData.Val.(*block.RuntimeBlockData[uint64])
// 						if !rtBlockInfoOk {
// 							return nil, fmt.Errorf("runtime block info type is incorrect")
// 						}

// 						// todo do not copy
// 						runtimeBlockInfo.ExportData(blockDataUint64[:])

// 						itemsFiltered := ops.CompareValuesAreInRange(blockDataUint64[:], operandA, operandB, filterResultUint64[:])
// 					case EQ:
// 						operand := filter.Arguments[0].(uint64)

// 						runtimeBlockInfo, rtBlockInfoOk := blockData.Val.(*block.RuntimeBlockData[uint64])
// 						if !rtBlockInfoOk {
// 							return nil, fmt.Errorf("runtime block info type is incorrect")
// 						}

// 						// todo do not copy
// 						runtimeBlockInfo.ExportData(blockDataUint64[:])
// 						itemsFiltered := ops.CompareNumericValuesAreEqual(blockDataUint64[:], operand, filterResultUint64[:])

// 						filteredRows := filterResultUint64[:itemsFiltered]
// 					default:
// 						return nil, fmt.Errorf("unsupported operand %v", filter.Operand)
// 					}
// 				default:
// 					return nil, fmt.Errorf("unsupported type %v", columnInfo.Type)
// 				}

// 			}

// 		}
// 	}

// 	return result, nil
// }
