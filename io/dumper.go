package io

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

type BlockTypes interface {
	uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int | float64 | float32
}

func DumpNumbersArrayBlock[T BlockTypes](writer io.Writer, arr []T) error {

	var arrSample T
	valueSize := reflect.ValueOf(arrSample).Type().Size()

	// Reinterpret array as byte slice
	byteLen := len(arr) * int(valueSize)
	b := unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), byteLen)

	_, err := writer.Write(b)
	// log.Printf(" >> written %d bytes", writtenBytes)

	return err
}

func DumpNumbersArrayBlockAny(writer io.Writer, arr any) error {
	switch v := arr.(type) {

	case []uint8:
		_, err := writer.Write(v)
		return err
	case []int8:
		return DumpNumbersArrayBlock[int8](writer, v)
	case []uint16:
		return DumpNumbersArrayBlock[uint16](writer, v)
	case []uint32:
		return DumpNumbersArrayBlock[uint32](writer, v)
	case []uint64:
		return DumpNumbersArrayBlock[uint64](writer, v)
	case []float32:
		return DumpNumbersArrayBlock[float32](writer, v)
	case []float64:
		return DumpNumbersArrayBlock[float64](writer, v)

	default:
		return fmt.Errorf("unsupported type %T while dumping to disk (any array) ", arr)
	}
}

// func DumpRuntimeBlockToDisk[T BlockTypes](path string, block *schema.RuntimeBlockData[T]) error {

// 	var copied []T = make([]T, block.Items)

// 	itemsExported := block.ExportData(copied)
// 	if itemsExported != block.Items {
// 		return fmt.Errorf("exported only %d items from runtime block instead of %d", itemsExported, block.Items)
// 	}

// 	fw, _ := os.Open(path)
// 	defer fw.Close()

// 	return DumpNumbersArrayBlock(fw, copied)
// }
