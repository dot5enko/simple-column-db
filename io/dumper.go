package io

import (
	"fmt"
	"log"
	"os"
	"unsafe"

	"github.com/dot5enko/simple-column-db/block"
)

type BlockTypes interface {
	uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int
}

func DumpNumbersArrayBlock[T BlockTypes](path string, arr []T) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Reinterpret array as byte slice
	byteLen := len(arr) * 8
	b := unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), byteLen)

	var writtenBytes int
	writtenBytes, err = f.Write(b)

	log.Printf("written %d bytes @ %s", writtenBytes, path)

	return err
}

func DumpRuntimeBlockToDisk[T BlockTypes](path string, block *block.RuntimeBlockData[T]) error {

	var copied []T = make([]T, block.Items)

	itemsExported := block.ExportData(copied)
	if itemsExported != block.Items {
		return fmt.Errorf("exported only %d items from runtime block instead of %d", itemsExported, block.Items)
	}

	return DumpNumbersArrayBlock(path, copied)
}
