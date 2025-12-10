package bits

import (
	"reflect"
	"unsafe"
)

// []uint64 | []uint16 | []uint8 | []uint32 | []int64 | []int32 | []int16 | []int8 | []int
func MapBytesToArray[T any](data []byte, count int) *T {

	var arrSample T
	valueSize := reflect.ValueOf(arrSample).Type().Elem().Size()

	if len(data) < count*int(valueSize) {
		panic("not enough data")
	}

	return (*T)(unsafe.Pointer(&data[0]))
}
