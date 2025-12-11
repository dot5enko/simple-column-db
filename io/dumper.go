package io

import (
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"unsafe"

	"github.com/dot5enko/simple-column-db/block"
)

type BlockTypes interface {
	uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int
}

func DumpNumbersArrayBlock[T BlockTypes](writer io.Writer, arr []T) error {

	var arrSample T
	valueSize := reflect.ValueOf(arrSample).Type().Elem().Size()

	// Reinterpret array as byte slice
	byteLen := len(arr) * int(valueSize)
	b := unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), byteLen)

	writtenBytes, err := writer.Write(b)

	log.Printf("written %d bytes", writtenBytes)

	return err
}

func DumpRuntimeBlockToDisk[T BlockTypes](path string, block *block.RuntimeBlockData[T]) error {

	var copied []T = make([]T, block.Items)

	itemsExported := block.ExportData(copied)
	if itemsExported != block.Items {
		return fmt.Errorf("exported only %d items from runtime block instead of %d", itemsExported, block.Items)
	}

	fw, _ := os.Open(path)
	defer fw.Close()

	return DumpNumbersArrayBlock(fw, copied)
}
