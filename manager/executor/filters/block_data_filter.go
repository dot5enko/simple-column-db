package filters

import (
	"fmt"
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
)

var totalCompares atomic.Int32

// comparesCount := totalCompares.Add(1)

// // percent := float64(itemsFiltered) / float64(rtconfig.ROWS_PER_BLOCK)
// // if percent > 0.5 {
// // , itemsFiltered, percent*100.0
// // , %d => %.2f
// color.Green(" -- total eq compares on int %d", comparesCount)
// // }

func ProcessUnsignedFilterOnColumnWithType[T ops.UnsignedInts](
	cache *executortypes.ChunkExecutorThreadCache,
	filterRT *query.FilterConditionRuntime,
	runtimeBlockInfo *schema.RuntimeBlockData,
	merger *lists.IndiceUnmerged,

) (int, error) {

	filter := filterRT.Filter

	bid := schema.ConstructUniqueBlockIdForColumn(runtimeBlockInfo.Slab, uint8(runtimeBlockInfo.BlockIndice))

	cached := cache.GetCachedFilter(filterRT.UniqueId, bid)
	if cached != nil {
		merger.WithBitset(cached, false, false)
		return cached.Count(), nil
	}

	var itemsFiltered int
	var outputBitset bits.Bitfield

	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	// slog.Info("perform UINT filter on columns ", "filter", filter.Operand.String(), "block", blockData.BlockHeader.StartOffset)

	switch filter.Operand {
	case query.RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRangeUnsignedIntsBitsetFast(inputArray, operandA, operandB, &outputBitset)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)
	case query.EQ:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareNumericValuesAreEqualBitset(inputArray, operand, &outputBitset)

	case query.GT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreBiggerBitset(inputArray, operand, &outputBitset)

	case query.LT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreSmallerBitset(inputArray, operand, &outputBitset)

	default:
		return itemsFiltered, fmt.Errorf("unsupported operand type=%s while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand.String(), runtimeBlockInfo.Header.DataType.String())
	}

	cache.PutCached(filterRT.UniqueId, bid, &outputBitset)
	merger.WithBitset(&outputBitset, false, false)

	return itemsFiltered, nil

}

// func ProcessSignedFilterOnColumnWithType[T ops.SignedInts](
// 	slab *schema.DiskSlabHeader,
// 	filter query.FilterCondition,
// 	blockData *executortypes.BlockRuntimeInfo,
// 	merger *lists.IndiceUnmerged,
// 	indicesCache []uint16,
// ) (int, error) {

// 	var itemsFiltered int

// 	var outputBitset bits.Bitfield

// 	runtimeBlockInfo := blockData.Val
// 	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

// 	arrayCasted := directBlockArray.([]T)
// 	inputArray := arrayCasted[:arrayEndOffset]

// 	switch filter.Operand {
// 	case query.RANGE:
// 		operandA := filter.Arguments[0].(T)
// 		operandB := filter.Arguments[1].(T)

// 		if operandA > operandB {
// 			temp := operandB
// 			operandB = operandA
// 			operandA = temp

// 		}

// 		itemsFiltered = ops.CompareValuesAreInRangeSignedIntsBitset(inputArray, operandA, operandB, &outputBitset)

// 	case query.EQ:
// 		operand := filter.Arguments[0].(T)

// 		itemsFiltered = ops.CompareNumericValuesAreEqualBitset(inputArray, operand, &outputBitset)

// 	case query.GT:
// 		operand := filter.Arguments[0].(T)

// 		itemsFiltered = ops.CompareNumericValuesAreEqualBitset(inputArray, operand, &outputBitset)
// 	case query.LT:
// 		operand := filter.Arguments[0].(T)

// 		itemsFiltered = ops.CompareValuesAreSmallerBitset(inputArray, operand, &outputBitset)

// 	default:
// 		return itemsFiltered, fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, blockData.BlockHeader.DataType.String())
// 	}

// 	merger.WithBitset(&outputBitset, false, false)

// 	return itemsFiltered, nil

// }

func ProcessFloatFilterOnColumnWithType[T ops.Floats](
	// slab *schema.DiskSlabHeader,
	filter query.FilterCondition,
	// blockData *executortypes.BlockRuntimeInfo,
	runtimeBlockInfo *schema.RuntimeBlockData,
	merger *lists.IndiceUnmerged,
) (int, error) {

	var itemsFiltered int
	var outBitset bits.Bitfield

	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	// slog.Info("perform FLOAT filter on columns ", "filter", filter.Operand.String(), "block", blockData.BlockHeader.StartOffset)

	switch filter.Operand {
	case query.RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRangeFloatsBitsetUnrolled(inputArray, operandA, operandB, &outBitset)

	case query.EQ:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareNumericValuesAreEqualBitset(inputArray, operand, &outBitset)

	case query.GT:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareValuesAreBiggerBitset(inputArray, operand, &outBitset)
	case query.LT:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareValuesAreSmallerBitset(inputArray, operand, &outBitset)

	default:
		return itemsFiltered, fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, runtimeBlockInfo.Header.DataType.String())
	}

	merger.WithBitset(&outBitset, false, false)

	return itemsFiltered, nil
}
