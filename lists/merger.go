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
	allOnes  bool
}

func (i *IndiceUnmerged) Reset() {

	i.merges = 0
	i.fullSkip = false
	i.allOnes = false

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

func (i *IndiceUnmerged) Count() int {

	if !i.initialized {
		return -1
	} else {

		res := i.ResultBitset.Count()
		return res
	}
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

func (i *IndiceUnmerged) WithBitset(input *bits.Bitfield, isEmpty, isFull bool) {

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
		i.ResultBitset.FromOther(input)
		i.initialized = true
		return
	}

	i.allOnes = false
	i.ResultBitset.And(input)
}

func (i *IndiceUnmerged) withFull() {

	if !i.initialized {
		i.ResultBitset = BitsetFull
		i.allOnes = true
		i.initialized = true
		return
	}

	// do nothing as all the bits set in result would be the same afterwards
	// i.ResultBitset = bits.MergeAND(i.ResultBitset, BitsetFull)
}

func (i *IndiceUnmerged) withEmpty() {

	if !i.initialized {
		i.ResultBitset = BitsetEmpty
		i.initialized = true
		return
	}

	// simply clean all words
	i.ResultBitset = bits.MergeAND(i.ResultBitset, BitsetEmpty)
}

func NewUnmerged() *IndiceUnmerged {

	return &IndiceUnmerged{
		initialized: false,
	}
}
