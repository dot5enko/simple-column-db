package manager

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type layoutFieldInfo struct {
	index int
	typ   schema.FieldType

	slab *SlabCacheItem

	Data any
}

func (m *Manager) Ingest(data []any, layout []string, schemaName string) error {

	// get the schema object from name
	schemaObject, exists := m.schemas[schemaName]

	if !exists {
		return errors.New("schema not found")
	}

	var fieldsLayout []layoutFieldInfo = make([]layoutFieldInfo, len(layout))

	// itemsCount := len(data)

	// check layout matches schema columns names
	for idx, col := range schemaObject.Columns {
		found := false
		for _, l := range layout {
			if col.Name == l {
				found = true
				break
			}
		}

		if !found {
			return errors.New("layout does not match schema, no column " + col.Name + " found in data")
		} else {

			var slabHeader *SlabCacheItem

			// slab exists
			if col.ActiveSlab != uuid.Nil {

				_, loadSlabErr := m.Slabs.LoadSlabToCache(schemaObject, col.ActiveSlab, m)
				if loadSlabErr != nil {
					return loadSlabErr
				}

				slabHeader = m.Slabs.GetSlabFromCache(col.ActiveSlab)
			} else {
				return fmt.Errorf("no active slab found for column %s", col.Name)
			}

			fInfo := layoutFieldInfo{
				index: idx,
				typ:   col.Type,
				slab:  slabHeader,
			}

			fieldsLayout[idx] = fInfo
		}
	}

	panic("not implemented")

	// for idx, field := range fieldsLayout {

	// 	switch field.typ {
	// 	case schema.Uint64FieldType:
	// 		resultColumn, collectErr := CollectTypedDataToArray[uint64](data, field.typ, idx)
	// 		if collectErr != nil {
	// 			return collectErr
	// 		}

	// 		field.Data = resultColumn

	// 	}

	// }

	// for i := 0; i < len(data); i++ {

	// 	block := m.GetLastUnfinishedBlock(schema.Uid)

	// 	if block == nil {
	// 		return errors.New("no unfinished block")
	// 	}
	// }

	// if no unfinished block create one

	return nil

}

func CollectTypedDataToArray[T any](inputRows []any, outputColumn []T, typ schema.FieldType, columnindex int) error {

	for i, v := range inputRows {

		rowDecoded := inputRows[i].([]any)[columnindex]

		switch t := rowDecoded.(type) {
		case T:
			outputColumn[i] = t
		default:
			return errors.New(fmt.Sprintf("invalid type %s expected %s", reflect.TypeOf(v), reflect.TypeOf(outputColumn)))
		}
	}
	return nil
}

func CollectTypedDataToArrayFromBinaryBuffer[T uint64](
	binReader *bits.BitsReader,
	outputColumn any,
	typ schema.FieldType,
	colOffset, rowSize, rows int,
) error {

	switch typ {
	case schema.Uint64FieldType:

		converted, convertOk := outputColumn.([]uint64)

		if !convertOk {
			panic("output column must be array of Ouput")
		}

		for index := 0; index < rows; index++ {
			skipErr := binReader.Skip(colOffset)
			if skipErr != nil {
				panic(fmt.Sprintf(" unable to skip %d : %s", colOffset, skipErr.Error()))
			}

			val := binReader.MustReadU64()

			curOffset := colOffset + 8
			offsetToMove := rowSize - curOffset

			if offsetToMove > 0 {
				binReader.Skip(offsetToMove)
			}

			converted[index] = uint64(val)
		}
	default:
		panic(fmt.Sprintf("unsupported type: %s when CollectTypedDataToArrayFromBinaryBuffer", typ.String()))
	}

	return nil
}

func CollectTypedDataToArrayFromBinaryBufferFast[T uint64](
	binReader []byte,
	outputColumn any,
	typ schema.FieldType,
	colOffset, rowSize, rows int,
	buf []byte,
) error {

	switch typ {
	case schema.Uint64FieldType:

		converted, convertOk := outputColumn.([]uint64)

		if !convertOk {
			panic("output column must be array of Ouput")
		}

		curOffset := 0
		readOffset := 0

		const readSize = 8

		for index := 0; index < rows; index++ {

			readOffset += colOffset

			copy(buf[curOffset:], binReader[readOffset:readOffset+readSize])

			readOffset += readSize
			curOffset += 8

			curOffset := colOffset + 8
			offsetToMove := rowSize - curOffset

			readOffset += offsetToMove
		}

		// using unsafe copy because we know that buffer size is correct

		hdr := unsafe.Slice((*uint64)(unsafe.Pointer(&buf[0])), rows)
		copiedN := copy(converted[:], hdr[:])

		if copiedN != rows {
			panic(fmt.Sprintf("unable to copy all elements: got %d, expected %d)", rows, copiedN))
		}

	default:
		panic(fmt.Sprintf("unsupported type: %s when CollectTypedDataToArrayFromBinaryBuffer", typ.String()))
	}

	return nil
}
