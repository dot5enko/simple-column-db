package manager

import (
	"errors"
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/lists"
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

func (c CondOperand) String() string {
	switch c {
	case EQ:
		return "EQ"
	case GT:
		return "GT"
	case LT:
		return "LT"
	case RANGE:
		return "RANGE"
	default:
		panic(fmt.Sprintf("unknown operand %v", c))
	}
}

type FilterCondition struct {
	Field     string
	Operand   CondOperand
	Arguments []any
}

func (fc FilterCondition) ArgumentFloatValue(idx int) float64 {

	arg := fc.Arguments[idx]

	switch v := arg.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case uint64:
		return float64(v)
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case float32:
		return float64(v)
	default:
		panic(fmt.Sprintf("filter cond argument is not numeric: %T", arg))
	}
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

		type RuntimeFilterCache struct {
			column schema.SchemaColumn
		}

		type FilterConditionRuntime struct {
			filter  FilterCondition
			runtime *RuntimeFilterCache
		}

		// group filters by columns
		filtersByColumns := map[string][]FilterConditionRuntime{}
		for _, filter := range query.Filter {
			old, isOk := filtersByColumns[filter.Field]
			if !isOk {
				old = []FilterConditionRuntime{}
			}

			filtersByColumns[filter.Field] = append(old, FilterConditionRuntime{
				filter:  filter,
				runtime: &RuntimeFilterCache{},
			})
		}

		// spew.Dump("filter by columns", filtersByColumns)
		// spew.Dump("slabs filtered", slabsByColumns)

		for columnName, filterColumn := range filtersByColumns {

			var columnInfo schema.SchemaColumn

			// cache
			for _, it := range schemaObject.Columns {
				if it.Name == columnName {
					columnInfo = it

					break
				}
			}

			filtersSize := len(filterColumn)

			for _, slab := range slabsByColumns[columnName] {

				// color.Red(" -- slab processing by field name : %s. slab %s ", columnName, slab.String())

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

					skipFilters := 0

					for _, filter := range filterColumn {
						var processFilterErr error

						skipSingleBlock := false

						// process filter on a block header
						switch columnInfo.Type {
						case schema.Uint64FieldType:
							skipSingleBlock, processFilterErr = ProcessFilterOnBlockHEader[uint64](filter.filter, blockHeader)
						case schema.Uint8FieldType:
							skipSingleBlock, processFilterErr = ProcessFilterOnBlockHEader[uint8](filter.filter, blockHeader)
						case schema.Float32FieldType:
							skipSingleBlock, processFilterErr = ProcessFilterOnBlockHEader[float32](filter.filter, blockHeader)
						case schema.Float64FieldType:
							skipSingleBlock, processFilterErr = ProcessFilterOnBlockHEader[float64](filter.filter, blockHeader)
						default:
							return nil, fmt.Errorf("unsupported type %v while filtering block headers", columnInfo.Type.String())
						}

						if processFilterErr != nil {
							return nil, fmt.Errorf("error filter processing : %s", processFilterErr.Error())
						} else {
							if skipSingleBlock {
								skipFilters++
							}
						}
					}

					if skipFilters == filtersSize {
						// color.Yellow("skipping block %s on header filtering step", blockHeader.Uid.String())
						continue
					}

					// filter by headers if possible
					blockDecodedInfo, blockErr := sm.Slabs.LoadBlockToRuntimeBlockData(*schemaObject, slabInfo, blockHeader.Uid)

					// log.Printf("--- loaded block %s: @ %p", blockHeader.Uid.String(), blockDecodedInfo.DataTypedArray)

					if blockErr != nil {
						return nil, fmt.Errorf("unable to decode block : %s", blockErr.Error())
					}

					blocks = append(blocks, BlockRuntimeInfo{
						Val:          blockDecodedInfo,
						Header:       blockHeader,
						Synchronized: true,
					})
				}

				// get slab bounds
				// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

				for blockIdx, blockData := range blocks {

					blockGroupMerger := lists.NewUnmerged(mergeIndicesCache)

					for _, filter := range filterColumn {

						var processFilterErr error

						// process filter on a block
						switch columnInfo.Type {
						case schema.Uint64FieldType:
							processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Uint8FieldType:
							processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint8](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float32FieldType:
							processFilterErr = ProcessFloatFilterOnColumnWithType[float32](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
						case schema.Float64FieldType:
							processFilterErr = ProcessFloatFilterOnColumnWithType[float64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
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

					absBlockOffset := int(slabInfo.SlabOffsetBlocks) + blockIdx

					if len(mergedIndices) < 0 {
						log.Printf("filterd indices in block[%s][%d]: %v. [abs block offset : %d]", columnName, blockIdx, len(mergedIndices), absBlockOffset)
					}
				}

			}

		}
	}

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
