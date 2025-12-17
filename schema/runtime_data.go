package schema

import (
	"fmt"
	"log"
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

func writeTypedArray[T NumericTypes](b *RuntimeBlockData, dataArray any, startOffset int) (int, error, BoundsFloat) {
	typedArray, typedOk := b.DataTypedArray.([]T)
	inputArray, inputOk := dataArray.([]T)

	inputArray = inputArray[startOffset:]

	if !typedOk || !inputOk {
		return 0, fmt.Errorf("wrong type in runtime block: input type: %s, expected type : %s", reflect.TypeOf(inputArray), reflect.TypeOf(typedArray)), BoundsFloat{}
	}
	log.Printf(" >>>> about to copy %d items from array of size. dest len : %d. items : %d. cap : %d", len(inputArray), len(typedArray[b.Items:]), b.Items, b.Cap)
	copied := copy(typedArray[b.Items:b.Cap], inputArray)

	bounds := GetMaxMinBoundsFloat(inputArray[:copied])

	return copied, nil, bounds
}

func (b *RuntimeBlockData) Write(dataArray any, dataArrayStartOffset int, typ FieldType) (written int, topErr error, bounds BoundsFloat) {
	b.lock.Lock()
	defer b.lock.Unlock()

	switch typ {
	case Uint64FieldType:
		written, topErr, bounds = writeTypedArray[uint64](b, dataArray, dataArrayStartOffset)
	case Uint8FieldType:
		written, topErr, bounds = writeTypedArray[uint8](b, dataArray, dataArrayStartOffset)
	case Float32FieldType:
		written, topErr, bounds = writeTypedArray[float32](b, dataArray, dataArrayStartOffset)
	case Uint16FieldType:
		written, topErr, bounds = writeTypedArray[uint16](b, dataArray, dataArrayStartOffset)
	case Float64FieldType:
		written, topErr, bounds = writeTypedArray[float64](b, dataArray, dataArrayStartOffset)
	case Uint32FieldType:
		written, topErr, bounds = writeTypedArray[uint32](b, dataArray, dataArrayStartOffset)
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

	return &RuntimeBlockData{
		Cap:            BlockRowsSize,
		Items:          itemCount, // todo make it possible to have different sizes for different blocks ?
		DataTypedArray: dataArray,
	}
}
