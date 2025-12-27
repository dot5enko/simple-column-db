package ops

import "github.com/dot5enko/simple-column-db/bits"

func CompareValuesAreSmaller[T NumericTypes](arr []T, cmp T, out []uint16) int {
	n := len(arr)
	filled := 0
	i := 0

	for ; i+7 < n; i += 8 {
		a0, a1 := arr[i], arr[i+1]
		a2, a3 := arr[i+2], arr[i+3]
		a4, a5 := arr[i+4], arr[i+5]
		a6, a7 := arr[i+6], arr[i+7]
		if a0 < cmp {
			out[filled] = uint16(i)
			filled++
		}
		if a1 < cmp {
			out[filled] = uint16(i + 1)
			filled++
		}
		if a2 < cmp {
			out[filled] = uint16(i + 2)
			filled++
		}
		if a3 < cmp {
			out[filled] = uint16(i + 3)
			filled++
		}
		if a4 < cmp {
			out[filled] = uint16(i + 4)
			filled++
		}
		if a5 < cmp {
			out[filled] = uint16(i + 5)
			filled++
		}
		if a6 < cmp {
			out[filled] = uint16(i + 6)
			filled++
		}
		if a7 < cmp {
			out[filled] = uint16(i + 7)
			filled++
		}

	}

	// Tail element
	for ; i < n; i++ {
		if arr[i] < cmp {
			out[filled] = uint16(i)
			filled++
		}
	}
	return filled
}

func CompareValuesAreSmallerBitset[T NumericTypes](
	arr []T, cmp T, out *bits.Bitfield,
) int {
	n := len(arr)
	i := 0

	for ; i+63 < n; i += 64 {
		a := arr[i:]
		var m uint64

		if a[0] < cmp {
			m |= 1 << 0
		}
		if a[1] < cmp {
			m |= 1 << 1
		}
		if a[2] < cmp {
			m |= 1 << 2
		}
		if a[3] < cmp {
			m |= 1 << 3
		}
		if a[4] < cmp {
			m |= 1 << 4
		}
		if a[5] < cmp {
			m |= 1 << 5
		}
		if a[6] < cmp {
			m |= 1 << 6
		}
		if a[7] < cmp {
			m |= 1 << 7
		}

		if a[8] < cmp {
			m |= 1 << 8
		}
		if a[9] < cmp {
			m |= 1 << 9
		}
		if a[10] < cmp {
			m |= 1 << 10
		}
		if a[11] < cmp {
			m |= 1 << 11
		}
		if a[12] < cmp {
			m |= 1 << 12
		}
		if a[13] < cmp {
			m |= 1 << 13
		}
		if a[14] < cmp {
			m |= 1 << 14
		}
		if a[15] < cmp {
			m |= 1 << 15
		}

		if a[16] < cmp {
			m |= 1 << 16
		}
		if a[17] < cmp {
			m |= 1 << 17
		}
		if a[18] < cmp {
			m |= 1 << 18
		}
		if a[19] < cmp {
			m |= 1 << 19
		}
		if a[20] < cmp {
			m |= 1 << 20
		}
		if a[21] < cmp {
			m |= 1 << 21
		}
		if a[22] < cmp {
			m |= 1 << 22
		}
		if a[23] < cmp {
			m |= 1 << 23
		}

		if a[24] < cmp {
			m |= 1 << 24
		}
		if a[25] < cmp {
			m |= 1 << 25
		}
		if a[26] < cmp {
			m |= 1 << 26
		}
		if a[27] < cmp {
			m |= 1 << 27
		}
		if a[28] < cmp {
			m |= 1 << 28
		}
		if a[29] < cmp {
			m |= 1 << 29
		}
		if a[30] < cmp {
			m |= 1 << 30
		}
		if a[31] < cmp {
			m |= 1 << 31
		}

		if a[32] < cmp {
			m |= 1 << 32
		}
		if a[33] < cmp {
			m |= 1 << 33
		}
		if a[34] < cmp {
			m |= 1 << 34
		}
		if a[35] < cmp {
			m |= 1 << 35
		}
		if a[36] < cmp {
			m |= 1 << 36
		}
		if a[37] < cmp {
			m |= 1 << 37
		}
		if a[38] < cmp {
			m |= 1 << 38
		}
		if a[39] < cmp {
			m |= 1 << 39
		}

		if a[40] < cmp {
			m |= 1 << 40
		}
		if a[41] < cmp {
			m |= 1 << 41
		}
		if a[42] < cmp {
			m |= 1 << 42
		}
		if a[43] < cmp {
			m |= 1 << 43
		}
		if a[44] < cmp {
			m |= 1 << 44
		}
		if a[45] < cmp {
			m |= 1 << 45
		}
		if a[46] < cmp {
			m |= 1 << 46
		}
		if a[47] < cmp {
			m |= 1 << 47
		}

		if a[48] < cmp {
			m |= 1 << 48
		}
		if a[49] < cmp {
			m |= 1 << 49
		}
		if a[50] < cmp {
			m |= 1 << 50
		}
		if a[51] < cmp {
			m |= 1 << 51
		}
		if a[52] < cmp {
			m |= 1 << 52
		}
		if a[53] < cmp {
			m |= 1 << 53
		}
		if a[54] < cmp {
			m |= 1 << 54
		}
		if a[55] < cmp {
			m |= 1 << 55
		}

		if a[56] < cmp {
			m |= 1 << 56
		}
		if a[57] < cmp {
			m |= 1 << 57
		}
		if a[58] < cmp {
			m |= 1 << 58
		}
		if a[59] < cmp {
			m |= 1 << 59
		}
		if a[60] < cmp {
			m |= 1 << 60
		}
		if a[61] < cmp {
			m |= 1 << 61
		}
		if a[62] < cmp {
			m |= 1 << 62
		}
		if a[63] < cmp {
			m |= 1 << 63
		}

		out[i>>6] |= m
	}

	// tail
	for ; i < n; i++ {
		if arr[i] < cmp {
			out.Set(i)
		}
	}

	return out.Count()
}
