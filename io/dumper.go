package io

import (
	"io"
	"log"
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

	writtenBytes, err := writer.Write(b)

	log.Printf(" >> written %d bytes", writtenBytes)

	return err
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
