package ops

type NumericTypes interface {
	uint64 | uint16 | uint8 | uint32 | int64 | int32 | int16 | int8 | int | float64 | float32
}

type Bounds[T NumericTypes] struct {
	Min T
	Max T
}

func (b *Bounds[T]) Morph(other Bounds[T]) {
	if other.Min < b.Min {
		b.Min = other.Min
	}
	if other.Max > b.Max {
		b.Max = other.Max
	}
}

func GetMaxMin[T NumericTypes](arr []T) Bounds[T] {

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
	return resultBounds
}
