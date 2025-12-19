package manager

import (
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

func ProcessUnsignedFilterOnColumnWithType[T ops.UnsignedInts](
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

		itemsFiltered = ops.CompareValuesAreInRangeUnsignedInts(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.Header.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.Header.Uid.String(), blockData.Header.Bounds.Min, blockData.Header.Bounds.Max)
		}
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

func ProcessSignedFilterOnColumnWithType[T ops.SignedInts](
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

		itemsFiltered = ops.CompareValuesAreInRangeSignedInts(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.Header.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.Header.Uid.String(), blockData.Header.Bounds.Min, blockData.Header.Bounds.Max)
		}
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

func ProcessFloatFilterOnColumnWithType[T ops.Floats](
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

		itemsFiltered = ops.CompareValuesAreInRangeFloats(inputArray, operandA, operandB, indicesCache)
		// log.Printf(" end of input array offset : %v", arrayEndOffset)

		if itemsFiltered > 0 {
			log.Printf("filtered %v items from block by range %s. ", itemsFiltered, blockData.Header.Uid.String())
			color.Red(" operands %v <-> %v. %s block range : [%e: max %e]", operandA, operandB, blockData.Header.Uid.String(), blockData.Header.Bounds.Min, blockData.Header.Bounds.Max)
		}
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
