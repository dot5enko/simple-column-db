package lists

import "log/slog"

type IndiceUnmerged struct {
	Merged []uint16
	Used   int

	MergedElementsCount int

	IsEmpty bool
	IsFull  bool

	LastSize int
}

func (i *IndiceUnmerged) With(input []uint16, isEmpty, isFull bool) {

	if i.IsEmpty {
		slog.Warn("skipped mergin on an empty incideMerger", "input_size", len(input))
		return
	}

	if isFull {
		// сopy last size again
		if i.MergedElementsCount > 1 {
			panic("unable to do this")
		} else {

			// slog.Info("merging full list", "last_items", i.LastSize, "elements", i.MergedElementsCount)

			copy(i.Merged[i.Used:], i.Merged[i.Used-i.LastSize:])
			i.Used += i.LastSize
			i.MergedElementsCount++
			i.IsFull = true
		}

		return
	} else if isEmpty {

		i.IsEmpty = true
		i.IsFull = false

		i.MergedElementsCount = 0

		return
	}

	copy(i.Merged[i.Used:], input)

	i.LastSize = len(input)
	i.Used += i.LastSize

	i.MergedElementsCount++

	if i.IsFull {
		slog.Info("meging", "length", len(input), "meged_elements_count", i.MergedElementsCount)
	}
}

func (i *IndiceUnmerged) Merge(cache []uint16, out []uint16) int {

	clear(cache)

	filled := 0

	targetSize := uint16(i.MergedElementsCount) - 1

	for _, v := range i.Merged[:i.Used] {
		old := cache[v]

		if old == targetSize {
			out[filled] = v
			filled++
		}

		cache[v] = old + 1
	}

	return filled
}

func NewUnmerged(buf []uint16) *IndiceUnmerged {
	return &IndiceUnmerged{
		Merged:              buf,
		MergedElementsCount: 0,
	}
}
