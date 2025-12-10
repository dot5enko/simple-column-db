package ops

func CompareNumericValuesAreEqual[T uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int | float64 | float32](arr []T, cmp T, out []T) int {
	n := len(arr)
	var filled int = 0
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

		im0 := b2i(a0 == cmp)
		im1 := b2i(a1 == cmp)
		im2 := b2i(a2 == cmp)
		im3 := b2i(a3 == cmp)
		im4 := b2i(a4 == cmp)
		im5 := b2i(a5 == cmp)
		im6 := b2i(a6 == cmp)
		im7 := b2i(a7 == cmp)

		out[filled] = T(i + 0)
		filled += im0
		out[filled] = T(i + 1)
		filled += im1
		out[filled] = T(i + 2)
		filled += im2
		out[filled] = T(i + 3)
		filled += im3
		out[filled] = T(i + 4)
		filled += im4
		out[filled] = T(i + 5)
		filled += im5
		out[filled] = T(i + 6)
		filled += im6
		out[filled] = T(i + 7)
		filled += im7

	}

	// Tail element
	for ; i < n; i++ {
		if arr[i] == cmp {
			out[filled] = T(i)
			filled++
		}
	}
	return filled
}
