package main

import (
	"math/rand"
	"testing"

	"github.com/dot5enko/simple-column-db/lists"
)

func randomFillIndices(n int, fillPercent int) []uint16 {
	out := make([]uint16, 0, n*fillPercent/100)
	for i := 0; i < n; i++ {
		if rand.Intn(100) < fillPercent {
			out = append(out, uint16(i))
		}
	}
	return out
}

func BenchmarkIntersectFastRandSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 35)
	input2 := randomFillIndices(size, 30)

	out := make([]uint16, size*2)
	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectFast(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastRandFull(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 80)

	out := make([]uint16, size*2)
	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectFast(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastRandHalfSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 15)

	out := make([]uint16, size*2)
	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectFast(input, input2, cache, cache3, out)
	}

}

// func BenchmarkIntersectSimple(t *testing.B) {

// 	size := 2

// 	input := []uint16{1000, 500, 123, 79, 100, 51}
// 	input2 := []uint16{212, 12, 10, 50, 13, 1000, 500}

// 	for i := 0; i < size; i++ {
// 		val := uint16(rand.Int63n(randCeiling))
// 		input[i] = val

// 		val2 := uint16(rand.Int63n(randCeiling))
// 		input2[i] = val2
// 	}

// 	out := make([]uint16, size*2)
// 	cache := make([]uint16, size*2)
// 	cache3 := make([]uint16, size*2)

// 	for t.Loop() {
// 		lists.IntersectFast(input, input2, cache, cache3, out)
// 	}

// }

func BenchmarkIntersectSlowSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 35)
	input2 := randomFillIndices(size, 30)

	out := make([]uint16, size*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func BenchmarkIntersectSlowFull(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 70)

	out := make([]uint16, size*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func BenchmarkIntersectSlowHalfSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 15)

	out := make([]uint16, size*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func TestMergeIsCorrect(t *testing.T) {
	size := 4000
	testI := 20

	input := randomFillIndices(size, 35)
	input2 := randomFillIndices(size, 30)

	out := make([]uint16, size*2)
	cacheMap := map[uint16]uint8{}

	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for i := 0; i < testI; i++ {
		intersectSlowResult := lists.Intersect(input, input2, out, cacheMap)
		intersectFastResult := lists.IntersectFast(input, input2, cache, cache3, out)

		// t.Logf("inputs a = %d, b = %d", len(input), len(input2))

		if intersectFastResult != intersectSlowResult {
			t.Errorf("Expected [slow=%d] but got [fast = %d]", intersectSlowResult, intersectFastResult)
		}
	}

}
