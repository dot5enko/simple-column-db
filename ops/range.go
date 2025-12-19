package ops

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
