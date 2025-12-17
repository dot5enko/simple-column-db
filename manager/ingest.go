package manager

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

type layoutFieldInfo struct {
	index int
	typ   schema.FieldType
	name  string

	slab       *SlabCacheItem
	dataOffset int

	DataArray any

	ingested int
	leftover int
}

func (m *Manager) Ingest(schemaName string, data *IngestBuffer) error {

	// get the schema object from name
	schemaObject, exists := m.schemas[schemaName]

	if !exists {
		return errors.New("schema not found")
	}

	var fieldsLayout []*layoutFieldInfo = make([]*layoutFieldInfo, len(data.FieldsLayout))

	rowSize := 0

	// check layout matches schema columns names
	for idx, col := range schemaObject.Columns {
		found := false
		for _, l := range data.FieldsLayout {
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

				_, loadSlabErr := m.Slabs.LoadSlabToCache(*schemaObject, col.ActiveSlab)
				if loadSlabErr != nil {
					return loadSlabErr
				}

				slabHeader = m.Slabs.GetSlabFromCache(col.ActiveSlab)
			} else {
				return fmt.Errorf("no active slab found for column %s", col.Name)
			}

			rowSize += col.Type.Size()

			fInfo := layoutFieldInfo{
				index:      idx,
				typ:        col.Type,
				slab:       slabHeader,
				dataOffset: rowSize,
				name:       col.Name,
			}

			fieldsLayout[idx] = &fInfo
		}
	}

	dataBuffer := data.dataBuffer
	itemsCount := len(dataBuffer) / rowSize

	for _, field := range fieldsLayout {
		switch field.typ {
		case schema.Uint8FieldType:

			collectErr := CollectColumnsFromRow[uint8](itemsCount, field, dataBuffer, rowSize)
			if collectErr != nil {
				return collectErr
			}
		case schema.Uint64FieldType:

			collectErr := CollectColumnsFromRow[uint64](itemsCount, field, dataBuffer, rowSize)
			if collectErr != nil {
				return collectErr
			}

		case schema.Float32FieldType:

			collectErr := CollectColumnsFromRow[float32](itemsCount, field, dataBuffer, rowSize)
			if collectErr != nil {
				return collectErr
			}

		default:
			panic(fmt.Sprintf("unsupported type: %s when ingest", field.typ.String()))
		}
	}

	// that should be internal api
	// ingestColumnarInternal(columnData)
	for _, field := range fieldsLayout {

		for field.leftover > 0 {

			sh := field.slab.header

			if sh.BlocksFinalized >= sh.BlocksTotal {
				newSlab, newSlabCreationErr := m.Slabs.NewSlabForColumn(*schemaObject, schemaObject.Columns[field.index])
				if newSlabCreationErr != nil {
					return newSlabCreationErr
				}

				{
					col := &schemaObject.Columns[field.index]

					if col.Slabs == nil {
						col.Slabs = []uuid.UUID{}
					}

					col.Slabs = append(col.Slabs, newSlab.Uid)
					col.ActiveSlab = newSlab.Uid

					storeErr := m.storeSchemeToDisk(*schemaObject)
					if storeErr != nil {
						return fmt.Errorf("unable to update schema config on disk: %s", storeErr.Error())
					}

				}

				sh = newSlab
			}

			curBlock := sh.BlockHeaders[sh.BlocksFinalized]

			// check if slab has free blocks

			ingestedToBlock, blockFinished, blockErr := m.Slabs.IngestIntoBlock(
				*schemaObject,
				field.slab.header,
				curBlock.Uid,
				field.DataArray,
				field.ingested,
			)

			if blockErr != nil {
				return blockErr
			} else {
				field.ingested += ingestedToBlock
				field.leftover -= ingestedToBlock

				color.Green(" > [%s] ingested %d, left %d ", field.name, ingestedToBlock, field.leftover)

				if blockFinished {

					if sh.BlocksFinalized < sh.BlocksTotal {
						sh.BlockHeaders[field.slab.header.BlocksFinalized] = schema.NewBlockHeader(field.typ)
					}
				}

			}

		}
	}

	return nil

}

func CollectColumnsFromRow[T any](
	itemsCount int,
	field *layoutFieldInfo,
	dataBuffer []byte,
	rowSize int,
) error {

	outputInts := make([]T, itemsCount)
	outBuffer := make([]byte, itemsCount*field.typ.Size())

	collectErr := CollectTypedDataToArrayFromBinaryBufferFast[T](dataBuffer,
		outputInts[:], field.typ,
		field.dataOffset, rowSize, itemsCount,
		outBuffer[:],
	)

	if collectErr != nil {
		return collectErr
	}

	field.DataArray = outputInts
	field.ingested = 0
	field.leftover = len(outputInts)

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

func CollectTypedDataToArrayFromBinaryBufferFast[T any](
	binReader []byte,
	outputColumn any,
	typ schema.FieldType,
	colOffset, rowSize, rows int,
	buf []byte,
) error {

	switch typ {
	case schema.Uint64FieldType:
	case schema.Float32FieldType:

		converted, convertOk := outputColumn.([]T)

		if !convertOk {
			panic("output column must be array of Ouput")
		}

		curOffset := 0
		readOffset := 0

		readSize := typ.Size()

		for index := 0; index < rows; index++ {

			readOffset += colOffset

			copy(buf[curOffset:], binReader[readOffset:readOffset+readSize])

			readOffset += readSize
			curOffset += readSize

			curOffset := colOffset + readSize
			offsetToMove := rowSize - curOffset

			readOffset += offsetToMove
		}

		// using unsafe copy because we know that buffer size is correct

		hdr := unsafe.Slice((*T)(unsafe.Pointer(&buf[0])), rows)
		copiedN := copy(converted[:], hdr[:])

		if copiedN != rows {
			panic(fmt.Sprintf("unable to copy all elements: got %d, expected %d)", rows, copiedN))
		}

	default:
		panic(fmt.Sprintf("unsupported type: %s when CollectTypedDataToArrayFromBinaryBuffer", typ.String()))
	}

	return nil
}
