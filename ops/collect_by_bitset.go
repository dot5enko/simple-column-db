package ops

import (
	bitsStd "math/bits"

	"github.com/dot5enko/simple-column-db/bits"
)

func CollectByBitset0[T any](
	arr []T,
	bitset *bits.Bitfield,
	out []T,
) int {
	n := len(arr)
	o := 0

	for w := 0; w < bits.BitfieldWordsLength; w++ {
		word := bitset[w]
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

func CollectByIndices[T any](
	arr []T,
	indices []uint16,
	out []T,
) int {
	o := 0

	for indice := range indices {
		out[o] = arr[indice]
		o++
	}

	return o
}

func CollectByBitset[T any](
	arr []T,
	bitset *bits.Bitfield,
	out []T,
) int {
	n := len(arr)
	if n == 0 {
		return 0
	}

	o := 0
	fullWords := n >> 6
	tail := n & 63
	full := ^uint64(0)

	// var totalCopies int

	for w := 0; w < fullWords; w++ {
		word := bitset[w]
		base := w << 6

		if word == full {
			copy(out[o:], arr[base:base+64])
			o += 64
			// totalCopies += 1

			continue
		}

		for word != 0 {

			b := bitsStd.TrailingZeros64(word)
			out[o] = arr[base+b]
			o++
			word &= word - 1
		}
	}

	if tail > 0 {
		panic("shouldn't happen, bitset is 512 words")
	}

	// slowCopies := o - totalCopies*64

	// if totalCopies > 0 {
	// color.Yellow("copy info total: %6d, fast: %5d [%5d], slow : %5d", o, totalCopies, totalCopies*64, slowCopies)
	// }

	return o
}
