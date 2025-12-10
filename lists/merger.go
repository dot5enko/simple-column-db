package lists

type IndiceUnmerged struct {
	Merged []uint16
	Used   int

	MergedElementsCount int
}

func (i *IndiceUnmerged) With(input []uint16) {
	copy(i.Merged[i.Used:], input)
	i.Used += len(input)
	i.MergedElementsCount++
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
