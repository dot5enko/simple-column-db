package filters

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
)

func ProcessUnsignedFilterOnColumnWithType[T ops.UnsignedInts](
	filter query.FilterCondition,
	runtimeBlockInfo *schema.RuntimeBlockData,
	merger *lists.IndiceUnmerged,

) (int, error) {

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
