package filters

import (
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/lists"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

func ProcessUnsignedFilterOnColumnWithType[T ops.UnsignedInts](
	filter query.FilterCondition,
	blockData *executortypes.BlockRuntimeInfo,
	merger *lists.IndiceUnmerged,
	indicesCache []uint16,
) (int, error) {

	var itemsFiltered int

	runtimeBlockInfo := blockData.Val
	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	switch filter.Operand {
	case query.RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRangeUnsignedInts(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if false && itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.BlockHeader.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.BlockHeader.Uid.String(), blockData.BlockHeader.Bounds.Min, blockData.BlockHeader.Bounds.Max)
		}
	case query.EQ:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareNumericValuesAreEqual(inputArray, operand, indicesCache)

	case query.GT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreBigger(inputArray, operand, indicesCache)
	case query.LT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreSmaller(inputArray, operand, indicesCache)

	default:
		return itemsFiltered, fmt.Errorf("unsupported operand type=%s while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand.String(), blockData.BlockHeader.DataType.String())
	}

	merger.With(indicesCache[:itemsFiltered], false, false)

	return itemsFiltered, nil

}

func ProcessSignedFilterOnColumnWithType[T ops.SignedInts](
	slab *schema.DiskSlabHeader,
	filter query.FilterCondition,
	blockData *executortypes.BlockRuntimeInfo,
	merger *lists.IndiceUnmerged,
	indicesCache []uint16,
) (int, error) {

	var itemsFiltered int

	runtimeBlockInfo := blockData.Val
	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	// log.Printf("[slab %s] processing numeric filter on column %v, type = %s", slab.Uid.String(), filter.Field, blockData.BlockHeader.DataType.String())

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	switch filter.Operand {
	case query.RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRangeSignedInts(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if false && itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.BlockHeader.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.BlockHeader.Uid.String(), blockData.BlockHeader.Bounds.Min, blockData.BlockHeader.Bounds.Max)
		}
	case query.EQ:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareNumericValuesAreEqual(inputArray, operand, indicesCache)

	case query.GT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreBigger(inputArray, operand, indicesCache)
	case query.LT:
		operand := filter.Arguments[0].(T)

		itemsFiltered = ops.CompareValuesAreSmaller(inputArray, operand, indicesCache)

	default:
		return itemsFiltered, fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, blockData.BlockHeader.DataType.String())
	}

	merger.With(indicesCache[:itemsFiltered], false, false)

	return itemsFiltered, nil

}

func ProcessFloatFilterOnColumnWithType[T ops.Floats](
	// slab *schema.DiskSlabHeader,
	filter query.FilterCondition,
	blockData *executortypes.BlockRuntimeInfo,
	merger *lists.IndiceUnmerged,
	indicesCache []uint16,
) (int, error) {

	var itemsFiltered int

	runtimeBlockInfo := blockData.Val
	directBlockArray, arrayEndOffset := runtimeBlockInfo.DirectAccess()

	// log.Printf("[slab %s] filter: %v, type = %s. offset %p[%d]. block %p",
	// 	slab.Uid.String(),
	// 	filter.Field,
	// 	blockData.BlockHeader.DataType.String(),
	// 	directBlockArray,
	// 	arrayEndOffset,
	// 	runtimeBlockInfo,
	// )

	arrayCasted := directBlockArray.([]T)
	inputArray := arrayCasted[:arrayEndOffset]

	switch filter.Operand {
	case query.RANGE:
		operandA := filter.Arguments[0].(T)
		operandB := filter.Arguments[1].(T)

		if operandA > operandB {
			temp := operandB
			operandB = operandA
			operandA = temp

		}

		itemsFiltered = ops.CompareValuesAreInRangeFloats(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if false && itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.BlockHeader.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.BlockHeader.Uid.String(), blockData.BlockHeader.Bounds.Min, blockData.BlockHeader.Bounds.Max)
			valuesFiltered := []T{}

			for _, i := range indicesCache[:itemsFiltered] {
				valuesFiltered = append(valuesFiltered, inputArray[i])
			}

			color.Green("-- filtered : %#+v", valuesFiltered)
		}

	case query.EQ:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareNumericValuesAreEqual(inputArray, operand, indicesCache)

	case query.GT:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareValuesAreBigger(inputArray, operand, indicesCache)
	case query.LT:

		operand := filter.Arguments[0].(T)
		itemsFiltered = ops.CompareValuesAreSmaller(inputArray, operand, indicesCache)

	default:
		return itemsFiltered, fmt.Errorf("unsupported operand type=%v while ProcessNumericFilterOnColumnWithType[%s]", filter.Operand, blockData.BlockHeader.DataType.String())
	}

	merger.With(indicesCache[:itemsFiltered], false, false)

	return itemsFiltered, nil
}
