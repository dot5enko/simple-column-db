package schema

import (
	"fmt"
	"reflect"
	"sync"
)

type RuntimeBlockData struct {
	Header DiskHeader

	lock sync.RWMutex

	DataTypedArray any
	Cap            int
	Items          int
}

func writeTypedArray[T any](b *RuntimeBlockData, dataArray any, startOffset int) (int, error) {
	typedArray, typedOk := b.DataTypedArray.([]T)
	inputArray, inputOk := dataArray.([]T)

	inputArray = inputArray[startOffset:]

	if !typedOk || !inputOk {
		return 0, fmt.Errorf("wrong type in runtime block: input type: %s, expected type : %s", reflect.TypeOf(inputArray), reflect.TypeOf(typedArray))
	}

	copied := copy(typedArray[b.Items:], inputArray)
	return copied, nil
}

func (b *RuntimeBlockData) Write(dataArray any, dataArrayStartOffset int, typ FieldType) (written int, topErr error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	switch typ {
	case Uint64FieldType:
		written, topErr = writeTypedArray[uint64](b, dataArray, dataArrayStartOffset)
	case Uint8FieldType:
		written, topErr = writeTypedArray[uint8](b, dataArray, dataArrayStartOffset)
	case Float32FieldType:
		written, topErr = writeTypedArray[float32](b, dataArray, dataArrayStartOffset)
	case Uint16FieldType:
		written, topErr = writeTypedArray[uint16](b, dataArray, dataArrayStartOffset)
	case Float64FieldType:
		written, topErr = writeTypedArray[float64](b, dataArray, dataArrayStartOffset)
	case Uint32FieldType:
		written, topErr = writeTypedArray[uint32](b, dataArray, dataArrayStartOffset)
	default:
		panic(fmt.Sprintf("unsupported type when writing to RuntimeBlockData: %s", typ.String()))
	}

	if topErr == nil {
		b.Items += written
	}

	return
}

// func (b *RuntimeBlockData) ExportData(out []T) int {
// 	b.lock.RLock()
// 	defer b.lock.RUnlock()

// 	return copy(out, b.Data[:b.Items])
// }

func (b *RuntimeBlockData) DirectAccess() (typedDataArray any, endOffset int) {
	return b.DataTypedArray, b.Items
}

func NewRuntimeBlockDataFromSlice(dataArray any, itemCount int) *RuntimeBlockData {

	// todo validation ?

	return &RuntimeBlockData{
		Cap:            itemCount,
		Items:          itemCount,
		DataTypedArray: dataArray,
	}
}
