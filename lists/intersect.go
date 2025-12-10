package lists

func Intersect[T uint64 | uint16](a, b, out []T, cache map[T]uint8) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	clear(cache)
	var other []T

	if len(a) < len(b) {

		other = b
		for _, v := range a {
			cache[v] = 0
		}
	} else {
		other = a
		for _, v := range b {
			cache[v] = 0
		}
	}

	filled := 0
	for _, v := range other {
		if _, ok := cache[v]; ok {
			out[filled] = v
			filled++
		}
	}

	return filled
}

func IntersectFast(a, b, cache, cache2, out []uint16) int {

	clear(cache2)

	cachePos := 0

	// log.Println("len", len(cache), "cachePos", cachePos)
	copy(cache[cachePos:], a)
	cachePos += len(a)

	// log.Println("len", len(cache), "cachePos", cachePos)

	copy(cache[cachePos:], b)
	cachePos += len(b)

	// log.Println("len", len(cache), "cachePos", cachePos)

	filled := 0

	for _, v := range cache[:cachePos] {
		old := cache2[v]

		if old == 1 {
			out[filled] = v
			filled++
		}

		cache2[v] = old + 1
	}

	// for _, v := range cache[:cachePos] {
	// 	if cache2[v] > 1 {

	// 	}
	// }

	return filled
}
