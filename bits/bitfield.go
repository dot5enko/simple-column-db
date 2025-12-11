package bits

import "math/bits"

type Bitfield [64 * 8]uint64

func (b *Bitfield) Set(bit int) {
	word := bit >> 6 // bit / 64
	mask := uint64(1) << (bit & 63)
	b[word] |= mask
}

func (b *Bitfield) Clear(bit int) {
	word := bit >> 6
	mask := uint64(1) << (bit & 63)
	b[word] &^= mask
}

func (b *Bitfield) SetTo(bit int, v uint64) {
	word := bit >> 6
	mask := uint64(1) << (bit & 63)
	if v == 1 {
		b[word] |= mask // set
	} else {
		b[word] &^= mask // clear
	}
}

func (b *Bitfield) FromSorted(bits []uint16) {
	arr := b[:] // removes bounds checks in indexing
	if len(bits) == 0 {
		return
	}

	currWord := bits[0] >> 6
	mask := uint64(0)

	for _, bit := range bits {
		w := bit >> 6
		if w != currWord {
			arr[currWord] |= mask
			currWord = w
			mask = 0
		}
		mask |= 1 << (bit & 63)
	}

	arr[currWord] |= mask
}

func (b *Bitfield) Get(bit int) uint64 {
	word := bit >> 6
	return (b[word] >> (bit & 63)) & 1
}

func (b *Bitfield) ToIndices(out []uint16) int {
	filled := 0
	for wi, w := range b {
		for w != 0 {
			tz := bits.TrailingZeros64(w)
			bit := uint64(wi*64 + tz)
			out[filled] = uint16(bit)
			filled += 1
			w &= w - 1 // clear lowest set bit
		}
	}
	return filled
}

func (b *Bitfield) Any() bool {
	for _, w := range b {
		if w != 0 {
			return true
		}
	}
	return false
}

func (b *Bitfield) Count() int {
	c := 0
	for i := 0; i < 64; i += 4 {
		c += bits.OnesCount64(b[i+0])
		c += bits.OnesCount64(b[i+1])
		c += bits.OnesCount64(b[i+2])
		c += bits.OnesCount64(b[i+3])
	}
	return c
}

func MergeOR(a, b Bitfield) (out Bitfield) {
	for i := range a {
		out[i] = a[i] | b[i]
	}
	return
}

func MergeAND(a, b Bitfield) (out Bitfield) {
	for i := range a {
		out[i] = a[i] & b[i]
	}
	return
}
