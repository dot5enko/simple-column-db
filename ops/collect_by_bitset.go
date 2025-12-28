package ops

import (
	bitsStd "math/bits"

	"github.com/dot5enko/simple-column-db/bits"
)

func CollectByBitset[T any](
	arr []T,
	bits *bits.Bitfield,
	out []T,
) int {
	n := len(arr)
	o := 0

	for w := 0; w < len(bits); w++ {
		word := bits[w]
		for word != 0 {
			b := bitsStd.TrailingZeros64(word)
			i := (w << 6) + b
			if i >= n {
				return o
			}
			out[o] = arr[i]
			o++
			word &= word - 1 // clear lowest bit
		}
	}

	return o
}
