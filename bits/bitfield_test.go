package bits

import (
	"fmt"
	"testing"

	"github.com/dot5enko/simple-column-db/manager/rtconfig"
)

func BenchmarkAll(t *testing.B) {

	fillPercentages := []float64{20}
	out := [rtconfig.ROWS_PER_BLOCK]BlockIndiceType{}

	// for each fill type and fill percentage, generate the bitfield
	for curFillType := FullRange; curFillType <= HotSuffix; curFillType++ {
		for _, fillPercentage := range fillPercentages {
			bf := PrefillBits(fillPercentage, curFillType)
			testName := fmt.Sprintf("%.2f_%s", fillPercentage, curFillType.String())

			// var lastA, lastB int

			t.Run(testName+"_old", func(b *testing.B) {
				for b.Loop() {
					_ = bf.ToIndices(out[:])
				}
			})

			// t.Run(testName+"_new", func(b *testing.B) {
			// 	for b.Loop() {
			// 		lastB = bf.ToIndicesSort(out[:])
			// 	}
			// })

			// if lastA != lastB {
			// 	panic("different results")
			// }

		}
	}

}

func BenchmarkSparseIndices(b *testing.B) {

}

type FType uint8

const (
	FullRange FType = iota
	SpareRanges
	ConcentratedSingle

	Empty
	AllSet
	Alternating
	WordDense
	WordSparse
	RandomUniform
	HotPrefix
	HotSuffix
)

func (ft FType) String() string {

	switch ft {
	case FullRange:
		return "full-range"
	case SpareRanges:
		return "spare-ranges"
	case ConcentratedSingle:
		return "concentrated-single"
	case Empty:
		return "empty"
	case AllSet:
		return "all-set"
	case Alternating:
		return "alternating"
	case WordDense:
		return "word-dense"
	case WordSparse:
		return "word-sparse"
	case RandomUniform:
		return "random-uniform"
	case HotPrefix:
		return "hot-prefix"
	case HotSuffix:
		return "hot-suffix"
	default:
		return "<unknown fill type>"
	}

}

func PrefillBits(percent float64, fillType FType) Bitfield {
	const (
		words     = 512
		bitsWord  = 64
		totalBits = words * bitsWord
	)

	if percent <= 0 {
		return Bitfield{}
	}
	if percent > 100 {
		percent = 100
	}

	target := int(float64(totalBits) * percent / 100.0)
	if target <= 0 {
		return Bitfield{}
	}

	var bf Bitfield

	switch fillType {

	case Empty:
		return bf

	case AllSet:
		for i := 0; i < words; i++ {
			bf[i] = ^uint64(0)
		}
		return bf

	case Alternating:
		for i := 0; i < words; i++ {
			bf[i] = 0xAAAAAAAAAAAAAAAA
		}

	case WordDense:
		fullWords := target / bitsWord
		for i := 0; i < fullWords && i < words; i++ {
			bf[i] = ^uint64(0)
		}

	case WordSparse:
		for i := 0; i < target && i < words; i++ {
			bf[i] = 1 << uint(i&63)
		}

	case RandomUniform:
		// Deterministic LCG, no math/rand dependency
		var x uint64 = 0xdeadbeefcafebabe
		for filled := 0; filled < target; {
			x = x*6364136223846793005 + 1
			pos := int(x % totalBits)
			w := pos >> 6
			b := uint(pos & 63)
			if (bf[w]>>b)&1 == 0 {
				bf[w] |= 1 << b
				filled++
			}
		}

	case HotPrefix:
		for i := 0; i < target; i++ {
			w := i >> 6
			b := uint(i & 63)
			bf[w] |= 1 << b
		}

	case HotSuffix:
		start := totalBits - target
		for i := 0; i < target; i++ {
			pos := start + i
			w := pos >> 6
			b := uint(pos & 63)
			bf[w] |= 1 << b
		}

	case FullRange:
		step := totalBits / target
		if step == 0 {
			step = 1
		}
		for i, filled := 0, 0; filled < target && i < totalBits; i += step {
			w := i >> 6
			b := uint(i & 63)
			bf[w] |= 1 << b
			filled++
		}

	case SpareRanges:
		ranges := 8
		if target < ranges {
			ranges = 1
		}
		perRange := target / ranges
		pos := 0
		for r := 0; r < ranges && pos < totalBits; r++ {
			for i := 0; i < perRange && pos < totalBits; i++ {
				w := pos >> 6
				b := uint(pos & 63)
				bf[w] |= 1 << b
				pos++
			}
			pos += bitsWord * 2
		}

	case ConcentratedSingle:
		start := (totalBits - target) / 2
		for i := 0; i < target; i++ {
			pos := start + i
			w := pos >> 6
			b := uint(pos & 63)
			bf[w] |= 1 << b
		}
	}

	return bf
}
