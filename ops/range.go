package ops

import "github.com/dot5enko/simple-column-db/bits"

type NumericTypes interface {
	SignedInts | UnsignedInts | Floats
}

type SignedInts interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type UnsignedInts interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type Floats interface {
	~float32 | ~float64
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func CompareValuesAreInRangeSignedInts[T SignedInts](
	arr []T, from, to T, out []uint16,
) int {
	if to <= from {
		return 0
	}

	n := len(arr)
	filled := 0
	rng := to - from
	i := 0

	for ; i+7 < n; i += 8 {
		a0 := arr[i+0]
		a1 := arr[i+1]
		a2 := arr[i+2]
		a3 := arr[i+3]
		a4 := arr[i+4]
		a5 := arr[i+5]
		a6 := arr[i+6]
		a7 := arr[i+7]

		m0 := (a0 - from) < rng
		m1 := (a1 - from) < rng
		m2 := (a2 - from) < rng
		m3 := (a3 - from) < rng
		m4 := (a4 - from) < rng
		m5 := (a5 - from) < rng
		m6 := (a6 - from) < rng
		m7 := (a7 - from) < rng

		if m0 {
			out[filled] = uint16(i + 0)
			filled++
		}
		if m1 {
			out[filled] = uint16(i + 1)
			filled++
		}
		if m2 {
			out[filled] = uint16(i + 2)
			filled++
		}
		if m3 {
			out[filled] = uint16(i + 3)
			filled++
		}
		if m4 {
			out[filled] = uint16(i + 4)
			filled++
		}
		if m5 {
			out[filled] = uint16(i + 5)
			filled++
		}
		if m6 {
			out[filled] = uint16(i + 6)
			filled++
		}
		if m7 {
			out[filled] = uint16(i + 7)
			filled++
		}
	}

	for ; i < n; i++ {
		a := arr[i]
		if (a - from) < rng {
			out[filled] = uint16(i)
			filled++
		}
	}

	return filled
}

func CompareValuesAreInRangeSignedIntsBitset[T SignedInts](
	arr []T, from, to T, out *bits.Bitfield,
) int {

	n := len(arr)
	rng := to - from
	i := 0

	for ; i+63 < n; i += 64 {
		a := arr[i:] // bounds-check elimination
		var m uint64

		if (a[0] - from) < rng {
			m |= 1 << 0
		}
		if (a[1] - from) < rng {
			m |= 1 << 1
		}
		if (a[2] - from) < rng {
			m |= 1 << 2
		}
		if (a[3] - from) < rng {
			m |= 1 << 3
		}
		if (a[4] - from) < rng {
			m |= 1 << 4
		}
		if (a[5] - from) < rng {
			m |= 1 << 5
		}
		if (a[6] - from) < rng {
			m |= 1 << 6
		}
		if (a[7] - from) < rng {
			m |= 1 << 7
		}

		if (a[8] - from) < rng {
			m |= 1 << 8
		}
		if (a[9] - from) < rng {
			m |= 1 << 9
		}
		if (a[10] - from) < rng {
			m |= 1 << 10
		}
		if (a[11] - from) < rng {
			m |= 1 << 11
		}
		if (a[12] - from) < rng {
			m |= 1 << 12
		}
		if (a[13] - from) < rng {
			m |= 1 << 13
		}
		if (a[14] - from) < rng {
			m |= 1 << 14
		}
		if (a[15] - from) < rng {
			m |= 1 << 15
		}

		if (a[16] - from) < rng {
			m |= 1 << 16
		}
		if (a[17] - from) < rng {
			m |= 1 << 17
		}
		if (a[18] - from) < rng {
			m |= 1 << 18
		}
		if (a[19] - from) < rng {
			m |= 1 << 19
		}
		if (a[20] - from) < rng {
			m |= 1 << 20
		}
		if (a[21] - from) < rng {
			m |= 1 << 21
		}
		if (a[22] - from) < rng {
			m |= 1 << 22
		}
		if (a[23] - from) < rng {
			m |= 1 << 23
		}

		if (a[24] - from) < rng {
			m |= 1 << 24
		}
		if (a[25] - from) < rng {
			m |= 1 << 25
		}
		if (a[26] - from) < rng {
			m |= 1 << 26
		}
		if (a[27] - from) < rng {
			m |= 1 << 27
		}
		if (a[28] - from) < rng {
			m |= 1 << 28
		}
		if (a[29] - from) < rng {
			m |= 1 << 29
		}
		if (a[30] - from) < rng {
			m |= 1 << 30
		}
		if (a[31] - from) < rng {
			m |= 1 << 31
		}

		if (a[32] - from) < rng {
			m |= 1 << 32
		}
		if (a[33] - from) < rng {
			m |= 1 << 33
		}
		if (a[34] - from) < rng {
			m |= 1 << 34
		}
		if (a[35] - from) < rng {
			m |= 1 << 35
		}
		if (a[36] - from) < rng {
			m |= 1 << 36
		}
		if (a[37] - from) < rng {
			m |= 1 << 37
		}
		if (a[38] - from) < rng {
			m |= 1 << 38
		}
		if (a[39] - from) < rng {
			m |= 1 << 39
		}

		if (a[40] - from) < rng {
			m |= 1 << 40
		}
		if (a[41] - from) < rng {
			m |= 1 << 41
		}
		if (a[42] - from) < rng {
			m |= 1 << 42
		}
		if (a[43] - from) < rng {
			m |= 1 << 43
		}
		if (a[44] - from) < rng {
			m |= 1 << 44
		}
		if (a[45] - from) < rng {
			m |= 1 << 45
		}
		if (a[46] - from) < rng {
			m |= 1 << 46
		}
		if (a[47] - from) < rng {
			m |= 1 << 47
		}

		if (a[48] - from) < rng {
			m |= 1 << 48
		}
		if (a[49] - from) < rng {
			m |= 1 << 49
		}
		if (a[50] - from) < rng {
			m |= 1 << 50
		}
		if (a[51] - from) < rng {
			m |= 1 << 51
		}
		if (a[52] - from) < rng {
			m |= 1 << 52
		}
		if (a[53] - from) < rng {
			m |= 1 << 53
		}
		if (a[54] - from) < rng {
			m |= 1 << 54
		}
		if (a[55] - from) < rng {
			m |= 1 << 55
		}

		if (a[56] - from) < rng {
			m |= 1 << 56
		}
		if (a[57] - from) < rng {
			m |= 1 << 57
		}
		if (a[58] - from) < rng {
			m |= 1 << 58
		}
		if (a[59] - from) < rng {
			m |= 1 << 59
		}
		if (a[60] - from) < rng {
			m |= 1 << 60
		}
		if (a[61] - from) < rng {
			m |= 1 << 61
		}
		if (a[62] - from) < rng {
			m |= 1 << 62
		}
		if (a[63] - from) < rng {
			m |= 1 << 63
		}

		out[i>>6] |= m
	}

	// tail
	for ; i < n; i++ {
		if (arr[i] - from) < rng {
			out.Set(i)
		}
	}

	return out.Count()
}

func CompareValuesAreInRangeUnsignedInts[T UnsignedInts](
	arr []T, from, to T, out []uint16,
) int {
	if to <= from {
		return 0
	}

	n := len(arr)
	filled := 0
	rng := to - from
	i := 0

	for ; i+7 < n; i += 8 {
		a0 := arr[i+0]
		a1 := arr[i+1]
		a2 := arr[i+2]
		a3 := arr[i+3]
		a4 := arr[i+4]
		a5 := arr[i+5]
		a6 := arr[i+6]
		a7 := arr[i+7]

		if (a0 - from) < rng {
			out[filled] = uint16(i + 0)
			filled++
		}
		if (a1 - from) < rng {
			out[filled] = uint16(i + 1)
			filled++
		}
		if (a2 - from) < rng {
			out[filled] = uint16(i + 2)
			filled++
		}
		if (a3 - from) < rng {
			out[filled] = uint16(i + 3)
			filled++
		}
		if (a4 - from) < rng {
			out[filled] = uint16(i + 4)
			filled++
		}
		if (a5 - from) < rng {
			out[filled] = uint16(i + 5)
			filled++
		}
		if (a6 - from) < rng {
			out[filled] = uint16(i + 6)
			filled++
		}
		if (a7 - from) < rng {
			out[filled] = uint16(i + 7)
			filled++
		}
	}

	for ; i < n; i++ {
		if (arr[i] - from) < rng {
			out[filled] = uint16(i)
			filled++
		}
	}

	return filled
}

func CompareValuesAreInRangeUnsignedIntsBitsetSlow[T UnsignedInts](
	arr []T, from, to T, out *bits.Bitfield,
) int {
	if to <= from {
		return 0
	}

	n := len(arr)
	rng := to - from
	i := 0

	for ; i+7 < n; i += 8 {
		a0 := arr[i+0]
		a1 := arr[i+1]
		a2 := arr[i+2]
		a3 := arr[i+3]
		a4 := arr[i+4]
		a5 := arr[i+5]
		a6 := arr[i+6]
		a7 := arr[i+7]

		if (a0 - from) < rng {
			out.Set(int(i + 0))
		}
		if (a1 - from) < rng {
			out.Set(int(i + 1))
		}
		if (a2 - from) < rng {
			out.Set(int(i + 2))
		}
		if (a3 - from) < rng {
			out.Set(int(i + 3))
		}
		if (a4 - from) < rng {
			out.Set(int(i + 4))
		}
		if (a5 - from) < rng {
			out.Set(int(i + 5))
		}
		if (a6 - from) < rng {
			out.Set(int(i + 6))
		}
		if (a7 - from) < rng {
			out.Set(int(i + 7))
		}
	}

	for ; i < n; i++ {
		if (arr[i] - from) < rng {
			out.Set(int(i))
		}
	}

	return out.Count()
}

func CompareValuesAreInRangeUnsignedIntsBitsetFast[T UnsignedInts](
	arr []T, from, to T, out *bits.Bitfield,
) int {
	if to <= from {
		return 0
	}

	n := len(arr)
	rng := to - from

	i := 0
	for ; i+63 < n; i += 64 {
		a := arr[i:] // helps bounds-check elimination
		var m uint64

		if (a[0] - from) < rng {
			m |= 1 << 0
		}
		if (a[1] - from) < rng {
			m |= 1 << 1
		}
		if (a[2] - from) < rng {
			m |= 1 << 2
		}
		if (a[3] - from) < rng {
			m |= 1 << 3
		}
		if (a[4] - from) < rng {
			m |= 1 << 4
		}
		if (a[5] - from) < rng {
			m |= 1 << 5
		}
		if (a[6] - from) < rng {
			m |= 1 << 6
		}
		if (a[7] - from) < rng {
			m |= 1 << 7
		}

		if (a[8] - from) < rng {
			m |= 1 << 8
		}
		if (a[9] - from) < rng {
			m |= 1 << 9
		}
		if (a[10] - from) < rng {
			m |= 1 << 10
		}
		if (a[11] - from) < rng {
			m |= 1 << 11
		}
		if (a[12] - from) < rng {
			m |= 1 << 12
		}
		if (a[13] - from) < rng {
			m |= 1 << 13
		}
		if (a[14] - from) < rng {
			m |= 1 << 14
		}
		if (a[15] - from) < rng {
			m |= 1 << 15
		}

		if (a[16] - from) < rng {
			m |= 1 << 16
		}
		if (a[17] - from) < rng {
			m |= 1 << 17
		}
		if (a[18] - from) < rng {
			m |= 1 << 18
		}
		if (a[19] - from) < rng {
			m |= 1 << 19
		}
		if (a[20] - from) < rng {
			m |= 1 << 20
		}
		if (a[21] - from) < rng {
			m |= 1 << 21
		}
		if (a[22] - from) < rng {
			m |= 1 << 22
		}
		if (a[23] - from) < rng {
			m |= 1 << 23
		}

		if (a[24] - from) < rng {
			m |= 1 << 24
		}
		if (a[25] - from) < rng {
			m |= 1 << 25
		}
		if (a[26] - from) < rng {
			m |= 1 << 26
		}
		if (a[27] - from) < rng {
			m |= 1 << 27
		}
		if (a[28] - from) < rng {
			m |= 1 << 28
		}
		if (a[29] - from) < rng {
			m |= 1 << 29
		}
		if (a[30] - from) < rng {
			m |= 1 << 30
		}
		if (a[31] - from) < rng {
			m |= 1 << 31
		}

		if (a[32] - from) < rng {
			m |= 1 << 32
		}
		if (a[33] - from) < rng {
			m |= 1 << 33
		}
		if (a[34] - from) < rng {
			m |= 1 << 34
		}
		if (a[35] - from) < rng {
			m |= 1 << 35
		}
		if (a[36] - from) < rng {
			m |= 1 << 36
		}
		if (a[37] - from) < rng {
			m |= 1 << 37
		}
		if (a[38] - from) < rng {
			m |= 1 << 38
		}
		if (a[39] - from) < rng {
			m |= 1 << 39
		}

		if (a[40] - from) < rng {
			m |= 1 << 40
		}
		if (a[41] - from) < rng {
			m |= 1 << 41
		}
		if (a[42] - from) < rng {
			m |= 1 << 42
		}
		if (a[43] - from) < rng {
			m |= 1 << 43
		}
		if (a[44] - from) < rng {
			m |= 1 << 44
		}
		if (a[45] - from) < rng {
			m |= 1 << 45
		}
		if (a[46] - from) < rng {
			m |= 1 << 46
		}
		if (a[47] - from) < rng {
			m |= 1 << 47
		}

		if (a[48] - from) < rng {
			m |= 1 << 48
		}
		if (a[49] - from) < rng {
			m |= 1 << 49
		}
		if (a[50] - from) < rng {
			m |= 1 << 50
		}
		if (a[51] - from) < rng {
			m |= 1 << 51
		}
		if (a[52] - from) < rng {
			m |= 1 << 52
		}
		if (a[53] - from) < rng {
			m |= 1 << 53
		}
		if (a[54] - from) < rng {
			m |= 1 << 54
		}
		if (a[55] - from) < rng {
			m |= 1 << 55
		}

		if (a[56] - from) < rng {
			m |= 1 << 56
		}
		if (a[57] - from) < rng {
			m |= 1 << 57
		}
		if (a[58] - from) < rng {
			m |= 1 << 58
		}
		if (a[59] - from) < rng {
			m |= 1 << 59
		}
		if (a[60] - from) < rng {
			m |= 1 << 60
		}
		if (a[61] - from) < rng {
			m |= 1 << 61
		}
		if (a[62] - from) < rng {
			m |= 1 << 62
		}
		if (a[63] - from) < rng {
			m |= 1 << 63
		}

		word := i >> 6
		out[word] |= m
	}

	// tail
	for ; i < n; i++ {
		if (arr[i] - from) < rng {
			out.Set(i)
		}
	}

	return out.Count()
}

func CompareValuesAreInRangeFloats[T Floats](
	arr []T, from, to T, out []uint16,
) int {
	if to <= from {
		return 0
	}

	n := len(arr)
	filled := 0
	i := 0

	for ; i+7 < n; i += 8 {
		a0 := arr[i+0]
		a1 := arr[i+1]
		a2 := arr[i+2]
		a3 := arr[i+3]
		a4 := arr[i+4]
		a5 := arr[i+5]
		a6 := arr[i+6]
		a7 := arr[i+7]

		if a0 >= from && a0 < to {
			out[filled] = uint16(i + 0)
			filled++
		}
		if a1 >= from && a1 < to {
			out[filled] = uint16(i + 1)
			filled++
		}
		if a2 >= from && a2 < to {
			out[filled] = uint16(i + 2)
			filled++
		}
		if a3 >= from && a3 < to {
			out[filled] = uint16(i + 3)
			filled++
		}
		if a4 >= from && a4 < to {
			out[filled] = uint16(i + 4)
			filled++
		}
		if a5 >= from && a5 < to {
			out[filled] = uint16(i + 5)
			filled++
		}
		if a6 >= from && a6 < to {
			out[filled] = uint16(i + 6)
			filled++
		}
		if a7 >= from && a7 < to {
			out[filled] = uint16(i + 7)
			filled++
		}
	}

	for ; i < n; i++ {
		a := arr[i]
		if a >= from && a < to {
			out[filled] = uint16(i)
			filled++
		}
	}

	return filled
}

func CompareValuesAreInRangeFloatsBitsetUnrolled[T Floats](
	arr []T, from, to T, out *bits.Bitfield,
) int {

	n := len(arr)
	i := 0

	for ; i+63 < n; i += 64 {
		a := arr[i:] // bounds-check elimination
		var m uint64

		if a[0] >= from && a[0] < to {
			m |= 1 << 0
		}
		if a[1] >= from && a[1] < to {
			m |= 1 << 1
		}
		if a[2] >= from && a[2] < to {
			m |= 1 << 2
		}
		if a[3] >= from && a[3] < to {
			m |= 1 << 3
		}
		if a[4] >= from && a[4] < to {
			m |= 1 << 4
		}
		if a[5] >= from && a[5] < to {
			m |= 1 << 5
		}
		if a[6] >= from && a[6] < to {
			m |= 1 << 6
		}
		if a[7] >= from && a[7] < to {
			m |= 1 << 7
		}

		if a[8] >= from && a[8] < to {
			m |= 1 << 8
		}
		if a[9] >= from && a[9] < to {
			m |= 1 << 9
		}
		if a[10] >= from && a[10] < to {
			m |= 1 << 10
		}
		if a[11] >= from && a[11] < to {
			m |= 1 << 11
		}
		if a[12] >= from && a[12] < to {
			m |= 1 << 12
		}
		if a[13] >= from && a[13] < to {
			m |= 1 << 13
		}
		if a[14] >= from && a[14] < to {
			m |= 1 << 14
		}
		if a[15] >= from && a[15] < to {
			m |= 1 << 15
		}

		if a[16] >= from && a[16] < to {
			m |= 1 << 16
		}
		if a[17] >= from && a[17] < to {
			m |= 1 << 17
		}
		if a[18] >= from && a[18] < to {
			m |= 1 << 18
		}
		if a[19] >= from && a[19] < to {
			m |= 1 << 19
		}
		if a[20] >= from && a[20] < to {
			m |= 1 << 20
		}
		if a[21] >= from && a[21] < to {
			m |= 1 << 21
		}
		if a[22] >= from && a[22] < to {
			m |= 1 << 22
		}
		if a[23] >= from && a[23] < to {
			m |= 1 << 23
		}

		if a[24] >= from && a[24] < to {
			m |= 1 << 24
		}
		if a[25] >= from && a[25] < to {
			m |= 1 << 25
		}
		if a[26] >= from && a[26] < to {
			m |= 1 << 26
		}
		if a[27] >= from && a[27] < to {
			m |= 1 << 27
		}
		if a[28] >= from && a[28] < to {
			m |= 1 << 28
		}
		if a[29] >= from && a[29] < to {
			m |= 1 << 29
		}
		if a[30] >= from && a[30] < to {
			m |= 1 << 30
		}
		if a[31] >= from && a[31] < to {
			m |= 1 << 31
		}

		if a[32] >= from && a[32] < to {
			m |= 1 << 32
		}
		if a[33] >= from && a[33] < to {
			m |= 1 << 33
		}
		if a[34] >= from && a[34] < to {
			m |= 1 << 34
		}
		if a[35] >= from && a[35] < to {
			m |= 1 << 35
		}
		if a[36] >= from && a[36] < to {
			m |= 1 << 36
		}
		if a[37] >= from && a[37] < to {
			m |= 1 << 37
		}
		if a[38] >= from && a[38] < to {
			m |= 1 << 38
		}
		if a[39] >= from && a[39] < to {
			m |= 1 << 39
		}

		if a[40] >= from && a[40] < to {
			m |= 1 << 40
		}
		if a[41] >= from && a[41] < to {
			m |= 1 << 41
		}
		if a[42] >= from && a[42] < to {
			m |= 1 << 42
		}
		if a[43] >= from && a[43] < to {
			m |= 1 << 43
		}
		if a[44] >= from && a[44] < to {
			m |= 1 << 44
		}
		if a[45] >= from && a[45] < to {
			m |= 1 << 45
		}
		if a[46] >= from && a[46] < to {
			m |= 1 << 46
		}
		if a[47] >= from && a[47] < to {
			m |= 1 << 47
		}

		if a[48] >= from && a[48] < to {
			m |= 1 << 48
		}
		if a[49] >= from && a[49] < to {
			m |= 1 << 49
		}
		if a[50] >= from && a[50] < to {
			m |= 1 << 50
		}
		if a[51] >= from && a[51] < to {
			m |= 1 << 51
		}
		if a[52] >= from && a[52] < to {
			m |= 1 << 52
		}
		if a[53] >= from && a[53] < to {
			m |= 1 << 53
		}
		if a[54] >= from && a[54] < to {
			m |= 1 << 54
		}
		if a[55] >= from && a[55] < to {
			m |= 1 << 55
		}

		if a[56] >= from && a[56] < to {
			m |= 1 << 56
		}
		if a[57] >= from && a[57] < to {
			m |= 1 << 57
		}
		if a[58] >= from && a[58] < to {
			m |= 1 << 58
		}
		if a[59] >= from && a[59] < to {
			m |= 1 << 59
		}
		if a[60] >= from && a[60] < to {
			m |= 1 << 60
		}
		if a[61] >= from && a[61] < to {
			m |= 1 << 61
		}
		if a[62] >= from && a[62] < to {
			m |= 1 << 62
		}
		if a[63] >= from && a[63] < to {
			m |= 1 << 63
		}

		out[i>>6] |= m
	}

	// tail
	for ; i < n; i++ {
		a := arr[i]
		if a >= from && a < to {
			out.Set(i)
		}
	}

	return out.Count()
}
