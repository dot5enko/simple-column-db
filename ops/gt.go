package ops

func CompareValuesAreBigger(arr []uint64, cmp uint64, out []uint64) int {
	n := len(arr)
	filled := 0
	i := 0

	for ; i+7 < n; i += 8 {
		a0, a1 := arr[i], arr[i+1]
		a2, a3 := arr[i+2], arr[i+3]
		a4, a5 := arr[i+4], arr[i+5]
		a6, a7 := arr[i+6], arr[i+7]

		if a0 > cmp {

			out[filled] = uint64(i)
			filled++
		}

		if a1 > cmp {
			out[filled] = uint64(i + 1)
			filled++
		}
		if a2 > cmp {
			out[filled] = uint64(i + 2)
			filled++
		}
		if a3 > cmp {
			out[filled] = uint64(i + 3)
			filled++
		}
		if a4 > cmp {
			out[filled] = uint64(i + 4)
			filled++
		}
		if a5 > cmp {
			out[filled] = uint64(i + 5)
			filled++
		}
		if a6 > cmp {
			out[filled] = uint64(i + 6)
			filled++
		}
		if a7 > cmp {
			out[filled] = uint64(i + 7)
			filled++
		}
	}

	// Tail element
	for ; i < n; i++ {
		if arr[i] > cmp {
			out[filled] = uint64(i)
			filled++
		}
	}
	return filled
}

func CompareValuesAreSmaller(arr []uint64, cmp uint64, out []uint64) int {
	n := len(arr)
	filled := 0
	i := 0

	for ; i+7 < n; i += 8 {
		a0, a1 := arr[i], arr[i+1]
		a2, a3 := arr[i+2], arr[i+3]
		a4, a5 := arr[i+4], arr[i+5]
		a6, a7 := arr[i+6], arr[i+7]
		if a0 < cmp {
			out[filled] = uint64(i)
			filled++
		}
		if a1 < cmp {
			out[filled] = uint64(i + 1)
			filled++
		}
		if a2 < cmp {
			out[filled] = uint64(i + 2)
			filled++
		}
		if a3 < cmp {
			out[filled] = uint64(i + 3)
			filled++
		}
		if a4 < cmp {
			out[filled] = uint64(i + 4)
			filled++
		}
		if a5 < cmp {
			out[filled] = uint64(i + 5)
			filled++
		}
		if a6 < cmp {
			out[filled] = uint64(i + 6)
			filled++
		}
		if a7 < cmp {
			out[filled] = uint64(i + 7)
			filled++
		}

	}

	// Tail element
	for ; i < n; i++ {
		if arr[i] < cmp {
			out[filled] = uint64(i)
			filled++
		}
	}
	return filled
}
