package ops

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func CompareValuesAreInRange[T NumericTypes](arr []T, cmpFrom, cmpTo T, out []uint16) int {
	n := len(arr)
	var filled int = 0
	i := 0

	rng := cmpTo - cmpFrom

	for ; i+7 < n; i += 8 {

		a0 := arr[i+0]
		a1 := arr[i+1]
		a2 := arr[i+2]
		a3 := arr[i+3]
		a4 := arr[i+4]
		a5 := arr[i+5]
		a6 := arr[i+6]
		a7 := arr[i+7]

		m0 := (a0 - cmpFrom) < rng
		m1 := (a1 - cmpFrom) < rng
		m2 := (a2 - cmpFrom) < rng
		m3 := (a3 - cmpFrom) < rng
		m4 := (a4 - cmpFrom) < rng
		m5 := (a5 - cmpFrom) < rng
		m6 := (a6 - cmpFrom) < rng
		m7 := (a7 - cmpFrom) < rng

		im0 := b2i(m0)
		im1 := b2i(m1)
		im2 := b2i(m2)
		im3 := b2i(m3)
		im4 := b2i(m4)
		im5 := b2i(m5)
		im6 := b2i(m6)
		im7 := b2i(m7)

		out[filled] = uint16(i + 0)
		filled += im0
		out[filled] = uint16(i + 1)
		filled += im1
		out[filled] = uint16(i + 2)
		filled += im2
		out[filled] = uint16(i + 3)
		filled += im3
		out[filled] = uint16(i + 4)
		filled += im4
		out[filled] = uint16(i + 5)
		filled += im5
		out[filled] = uint16(i + 6)
		filled += im6
		out[filled] = uint16(i + 7)
		filled += im7

	}

	// Tail element
	for ; i < n; i++ {
		if arr[i] > cmpFrom && arr[i] < cmpTo {
			out[filled] = uint16(i)
			filled++
		}
	}
	return filled
}
