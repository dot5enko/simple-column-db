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

	ResultBitset bits.Bitfield
}

func (i *IndiceUnmerged) WithOtherBitset(other bits.Bitfield) {

	if !i.initialized {
		i.initialized = true

		i.ResultBitset = other
		return
	}

	i.ResultBitset = bits.MergeAND(i.ResultBitset, other)

}

func (i *IndiceUnmerged) With(input []uint16, isEmpty, isFull bool) {

	if isFull {
		i.WithFull()
		return
	}

	if isEmpty {
		i.WithEmpty()
		return
	}

	var bitset bits.Bitfield
	bitset.FromSorted(input)

	if !i.initialized {
		i.ResultBitset = bitset
		i.initialized = true
		return
	}

	i.ResultBitset = bits.MergeAND(i.ResultBitset, bitset)
}

func (i *IndiceUnmerged) WithFull() {

	if !i.initialized {
		i.ResultBitset = BitsetFull
		i.initialized = true
		return
	}

	i.ResultBitset = bits.MergeAND(i.ResultBitset, BitsetFull)

}

func (i *IndiceUnmerged) WithEmpty() {

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
