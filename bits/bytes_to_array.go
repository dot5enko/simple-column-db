package bits

import (
	"unsafe"
)

func MapBytesToArray[T any](data []byte, count int) []T {
	var zero T
	size := int(unsafe.Sizeof(zero))

	if len(data) < count*size {
		panic("not enough data")
	}

	hdr := unsafe.Slice((*T)(unsafe.Pointer(&data[0])), count)
	return hdr
}
