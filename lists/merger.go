package lists

import (
	"github.com/dot5enko/simple-column-db/bits"
)

var (
	BitsetFull  = bits.NewFullBitfield()
	BitsetEmpty = bits.Bitfield{}
)

type IndiceUnmerged struct {
	initialized bool

	merges int

	ResultBitset bits.Bitfield

	fullSkip bool
}

func (i *IndiceUnmerged) Reset() {

	i.merges = 0
	i.fullSkip = false

	if i.initialized {
		for j := range i.ResultBitset {
			i.ResultBitset[j] = 0
		}
	}

	i.initialized = false
}

func (i *IndiceUnmerged) SetFullSkip() {
	i.fullSkip = true
}

func (i *IndiceUnmerged) FullSkip() bool {
	return i.fullSkip
}

func (i *IndiceUnmerged) Merges() int {
	return i.merges
}

// func (i *IndiceUnmerged) WithOtherBitset(other bits.Bitfield) {

// 	if !i.initialized {
// 		i.initialized = true

// 		i.ResultBitset = other
// 		return
// 	}

// 	i.ResultBitset = bits.MergeAND(i.ResultBitset, other)

// }

func (i *IndiceUnmerged) With(input []uint16, isEmpty, isFull bool) {

	i.merges += 1

	if isFull {
		i.withFull()
		return
	}

	if isEmpty {
		i.withEmpty()
		return
	}

	if !i.initialized {
		i.ResultBitset.FromSorted(input)
		i.initialized = true
		return
	}

	var bitset bits.Bitfield
	bitset.FromSorted(input)

	i.ResultBitset = bits.MergeAND(i.ResultBitset, bitset)
}

func (i *IndiceUnmerged) withFull() {

	if !i.initialized {
		i.ResultBitset = BitsetFull
		i.initialized = true
		return
	}

	i.ResultBitset = bits.MergeAND(i.ResultBitset, BitsetFull)

}

func (i *IndiceUnmerged) withEmpty() {

	if !i.initialized {
		i.ResultBitset = BitsetEmpty
		i.initialized = true
		return
	}

	i.ResultBitset = bits.MergeAND(i.ResultBitset, BitsetEmpty)
}

func NewUnmerged() *IndiceUnmerged {

	return &IndiceUnmerged{
		initialized: false,
	}
}
