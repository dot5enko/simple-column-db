package manager

import (
	"errors"
	"fmt"
	"log/slog"

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
		// mergeIndicesCache := make([]uint16, schema.BlockRowsSize*len(query.Filter))
		// var indicesCounter [schema.BlockRowsSize]uint16

		var indicesResultCache [schema.BlockRowsSize]uint16

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
		skippedBlocksDueToHeaderFiltering := 0

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

		absBlockMaps := map[uint64]*lists.IndiceUnmerged{}
		skippedBlocksFULL := 0

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

				slabInfo, slabErr := sm.Slabs.LoadSlabToCache(*schemaObject, slab)
				if slabErr != nil {
					return nil, fmt.Errorf("unable to load slab : %s", slabErr.Error())
				}

				var blocks []BlockRuntimeInfo
				for idx, blockHeader := range slabInfo.BlockHeaders {

					if idx > int(slabInfo.BlocksFinalized) {
						break
					}

					skipFilters := 0
					absBlockOffset := slabInfo.SlabOffsetBlocks + uint64(idx)

					for _, filter := range filterColumn {

						var processFilterErr error
						intersectType := schema.UnknownIntersection

						// process filter on a block header
						switch columnInfo.Type {
						case schema.Uint64FieldType:
							intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint64](filter.filter, blockHeader)
						case schema.Uint8FieldType:
							intersectType, processFilterErr = ProcessFilterOnBlockHeader[uint8](filter.filter, blockHeader)
						case schema.Float32FieldType:
							intersectType, processFilterErr = ProcessFilterOnBlockHeader[float32](filter.filter, blockHeader)
						case schema.Float64FieldType:
							intersectType, processFilterErr = ProcessFilterOnBlockHeader[float64](filter.filter, blockHeader)
						default:
							return nil, fmt.Errorf("unsupported type %v while filtering block headers", columnInfo.Type.String())
						}

						if processFilterErr != nil {
							return nil, fmt.Errorf("error filter processing : %s", processFilterErr.Error())
						} else {

							skipSingleBlock := intersectType == schema.NoIntersection
							// here, so we override old data for sure
							filter.runtime.filterLastBlockHeaderResult = intersectType

							if skipSingleBlock {
								skipFilters++
							}
						}
					}

					fullSkipBlock := skipFilters == filtersSize

					if fullSkipBlock {

						skippedBlocksFULL += 1

						// color.Yellow("skipping block %s on header filtering step", blockHeader.Uid.String())
						// do not load this block into memory at all
					}

					blockRT := BlockRuntimeInfo{
						Header:       blockHeader,
						Synchronized: true,
					}

					if !fullSkipBlock {
						blockDecodedInfo, blockErr := sm.Slabs.LoadBlockToRuntimeBlockData(*schemaObject, slabInfo, blockHeader.Uid)

						// log.Printf("--- loaded block %s: @ %p", blockHeader.Uid.String(), blockDecodedInfo.DataTypedArray)

						if blockErr != nil {
							return nil, fmt.Errorf("unable to decode block : %s", blockErr.Error())
						}

						blockRT.Val = blockDecodedInfo
					} else {
						absBlockRTInfo, ok := absBlockMaps[absBlockOffset]
						if !ok {
							absBlockRTInfo = lists.NewUnmerged()
							absBlockMaps[absBlockOffset] = absBlockRTInfo
						}

						absBlockRTInfo.SetFullSkip()
					}

					for filterIdx, filter := range filterColumn {
						blockRT.HeaderFilterMatchResult[filterIdx] = filter.runtime.filterLastBlockHeaderResult
					}

					blocks = append(blocks, blockRT)
				}

				// get slab bounds
				// curBlocksPerSlab := slabInfo.Type.BlocksPerSlab()

				for blockIdx, blockData := range blocks {

					absBlockOffset := slabInfo.SlabOffsetBlocks + uint64(blockIdx)

					blockGroupMerger, has := absBlockMaps[absBlockOffset]
					if !has {
						blockGroupMerger = lists.NewUnmerged()
						absBlockMaps[absBlockOffset] = blockGroupMerger
					} else {
						if blockGroupMerger.FullSkip() {
							continue
						}
					}

					for fIdx, filter := range filterColumn {

						headerMatchResult := blockData.HeaderFilterMatchResult[fIdx]

						isFull := headerMatchResult == schema.FullIntersection

						if isFull {
							skippedBlocksDueToHeaderFiltering += 1

							blockGroupMerger.With(nil, false, true)
							continue
						}

						{
							var processFilterErr error
							var filteredSize int

							// process filter on a block
							switch columnInfo.Type {
							case schema.Uint64FieldType:
								filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
							case schema.Uint8FieldType:
								filteredSize, processFilterErr = ProcessUnsignedFilterOnColumnWithType[uint8](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
							case schema.Float32FieldType:
								filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float32](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
							case schema.Float64FieldType:
								filteredSize, processFilterErr = ProcessFloatFilterOnColumnWithType[float64](slabInfo, filter.filter, &blockData, blockGroupMerger, indicesResultCache[:])
							default:
								return nil, fmt.Errorf("unsupported type %v", columnInfo.Type.String())
							}

							_ = filteredSize

							if processFilterErr != nil {
								return nil, fmt.Errorf("error filter processing : %s. sum of bitset = %d, bitcount = %d", processFilterErr.Error(), blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
							}

							// log.Printf(" -- [filtered] filteredSize : %d. sum of bitset = %d, bitcount = %d", filteredSize, blockGroupMerger.ResultBitset.Sum(), blockGroupMerger.ResultBitset.Count())
						}
					}
				}
			}
		}

		totalItems := 0
		wastedMerges := 0

		// filter merged blocks info
		for _, blockFilterMask := range absBlockMaps {
			if blockFilterMask.Merges() == len(query.Filter) {
				amount := blockFilterMask.ResultBitset.Count()
				totalItems += amount

			} else {
				wastedMerges += blockFilterMask.Merges()
			}
		}

		slog.Info("merge info", "skipped_full", skippedBlocksFULL, "wasted_merges", wastedMerges, "skipped_blocks", skippedBlocksDueToHeaderFiltering, "total_filtered", totalItems)

	}

	return result, nil
}

var (
	ErrRuntimeBlockInfoTypeIsIncorrect = errors.New("runtime block info type is incorrect")
)
