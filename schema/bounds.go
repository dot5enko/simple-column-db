package schema

import (
	"fmt"
	"math"

	"github.com/dot5enko/simple-column-db/bits"
)

type NumericTypes interface {
	uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int | float64 | float32
}

type Bounds[T NumericTypes] struct {
	Min T
	Max T
}

const BoundsSize = 8 + 8

type BoundsFilterMatchResult uint8

const (
	NoIntersection BoundsFilterMatchResult = iota
	PartialIntersection
	FullIntersection
	UnknownIntersection
)

func (b BoundsFilterMatchResult) String() string {

	switch b {
	case NoIntersection:
		return "no intersection"
	case PartialIntersection:
		return "partial intersection"
	case FullIntersection:
		return "full intersection"
	default:
		return fmt.Sprintf("unknown bounds match result:%d", b)
	}
}

type BoundsFloat struct {
	initialized bool

	Min float64
	Max float64
}

func (b BoundsFloat) Intersects(other BoundsFloat) BoundsFilterMatchResult {

	if !other.initialized || !b.initialized {
		panic("bounds not initialized, can't calc intersection")
	} else if !(b.Min <= other.Max && b.Max >= other.Min) {
		return NoIntersection
	} else if b.Min >= other.Min && b.Max <= other.Max {
		return FullIntersection
	} else {
		return PartialIntersection
	}

}

func (b BoundsFloat) Contains(value float64) bool {
	return value >= b.Min && value <= b.Max
}

func NewBoundsFromValues(min, max float64) BoundsFloat {
	return BoundsFloat{
		initialized: true,
		Min:         min,
		Max:         max,
	}
}

func NewBounds() BoundsFloat {
	return BoundsFloat{
		initialized: true,
		Min:         math.MaxFloat64,
		Max:         -math.MaxFloat64,
	}
}

func (b *BoundsFloat) Morph(other BoundsFloat) bool {

	if !b.initialized {

		b.Min = other.Min
		b.Max = other.Max

		b.initialized = other.initialized

		return true
	}

	changes := 0

	if other.Min < b.Min {
		b.Min = other.Min
		changes += 1
	}
	if other.Max > b.Max {
		b.Max = other.Max
		changes += 1
	}

	return changes != 0
}

func GetMaxMinBoundsFloat[T NumericTypes](arr []T) BoundsFloat {

	resultBounds := Bounds[T]{
		Min: arr[0],
		Max: arr[0],
	}

	for _, v := range arr[1:] {

		if v < resultBounds.Min {
			resultBounds.Min = v
		}
		if v > resultBounds.Max {
			resultBounds.Max = v
		}
	}

	return BoundsFloat{
		Min:         float64(resultBounds.Min),
		Max:         float64(resultBounds.Max),
		initialized: true,
	}
}

func (header *BoundsFloat) FromBytes(reader *bits.BitsReader) (topErr error) {

	header.Max = reader.MustReadF64()
	header.Min = reader.MustReadF64()
	header.initialized = true

	return nil

}

func (header *BoundsFloat) WriteTo(bw *bits.BitWriter) (int, error) {

	bw.PutFloat64(header.Max)

	if !header.initialized {
		panic(fmt.Sprintf("write unitialized bounds %e <-> %e", header.Min, header.Max))
	}
	bw.PutFloat64(header.Min)

	return bw.Position(), nil

}
