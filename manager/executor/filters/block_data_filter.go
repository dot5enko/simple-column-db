package filters

import (
	"fmt"
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/bits"
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
	filterRT *query.FilterConditionRuntime,
	runtimeBlockInfo *schema.RuntimeBlockData,
) (int, bits.Bitfield, error) {

	filter := filterRT.Filter

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
		return itemsFiltered, outputBitset, fmt.Errorf("unsupported operand type=%s while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand.String(), runtimeBlockInfo.Header.DataType.String())
	}

	return itemsFiltered, outputBitset, nil

}

func ProcessFloatFilterOnColumnWithType[T ops.Floats](
	filterRT *query.FilterConditionRuntime,
	runtimeBlockInfo *schema.RuntimeBlockData,
) (int, bits.Bitfield, error) {

	filter := filterRT.Filter
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
		return itemsFiltered, outBitset, fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, runtimeBlockInfo.Header.DataType.String())
	}

	return itemsFiltered, outBitset, nil
}
