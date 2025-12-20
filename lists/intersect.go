package lists

import (
	"github.com/dot5enko/simple-column-db/bits"
)

func Intersect[T uint64 | uint16](a, b, out []T, cache map[T]uint8) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	clear(cache)
	var other []T

	if len(a) < len(b) {

		other = b
		for _, v := range a {
			cache[v] = 0
		}
	} else {
		other = a
		for _, v := range b {
			cache[v] = 0
		}
	}

	filled := 0
	for _, v := range other {
		if _, ok := cache[v]; ok {
			out[filled] = v
			filled++
		}
	}

	return filled
}

func IntersectIndicesFastTwoList(a, b, cache, cache2, out []uint16) int {

	clear(cache2)

	cachePos := 0
	copy(cache[cachePos:], a)
	cachePos += len(a)
	copy(cache[cachePos:], b)
	cachePos += len(b)

	filled := 0

	for _, v := range cache[:cachePos] {
		old := cache2[v]

		if old == 1 {
			out[filled] = v
			filled++
		}

		cache2[v] = old + 1
	}

	return filled
}

func convertArrayIndicesToBitset(arr []uint16) (bitset bits.Bitfield) {

	bitset.FromSorted(arr)

	return bitset
}

func convertArrayIndicesToBitsetWithBounds(arr []uint16, boundsStart, boundsEnd uint16) (bitset bits.Bitfield) {

	bitset.FromSortedWithBounds(arr, boundsStart, boundsEnd)

	return bitset
}

func IntersectIndicesFastTwoListBitset(a, b, out []uint16) int {

	aBitset := convertArrayIndicesToBitset(a)
	bBitset := convertArrayIndicesToBitset(b)

	// t.StopTimer()
	// defer t.StartTimer()

	resultBitset := bits.MergeAND(aBitset, bBitset)

	if resultBitset.Any() {
		return resultBitset.ToIndices(out)
	}

	return resultBitset.ToIndices(out)
}

func IntersectIndicesFastTwoListBitsetOptimized(a, b, out []uint16, aStart, bStart, aEnd, bEnd uint16) int {

	aBitset := convertArrayIndicesToBitsetWithBounds(a, aStart, aEnd)
	bBitset := convertArrayIndicesToBitsetWithBounds(b, bStart, bEnd)

	// t.StopTimer()
	// defer t.StartTimer()

	resultBitset := bits.MergeAND(aBitset, bBitset)

	if resultBitset.Any() {
		return resultBitset.ToIndices(out)
	}

	return resultBitset.ToIndices(out)

}

func IntersectIndicesFastNList[T any](cache, cache2, out []uint16, inputs ...[]uint16) int {

	clear(cache2)

	cachePos := 0
	for _, input := range inputs {
		copy(cache[cachePos:], input)
		cachePos += len(input)
	}

	targetSize := uint16(len(inputs) - 1)

	filled := 0

	for _, v := range cache[:cachePos] {
		old := cache2[v]

		if old == targetSize {
			out[filled] = v
			filled++
		}

		cache2[v] = old + 1
	}

	return filled
}
