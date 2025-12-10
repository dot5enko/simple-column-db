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

func BenchmarkIntersectFastRandHalfSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 15)

	out := make([]uint16, size*2)
	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastRandSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 35)
	input2 := randomFillIndices(size, 30)

	out := make([]uint16, size*2)
	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
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
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastestRandSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 35)
	input2 := randomFillIndices(size, 30)

	out := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}

}

func BenchmarkIntersectFastestRandFull(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 80)

	out := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}

}

func BenchmarkIntersectFastestRandHalfSparse(t *testing.B) {

	size := 4000

	input := randomFillIndices(size, 85)
	input2 := randomFillIndices(size, 15)

	out := make([]uint16, size*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}
}

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

	input := randomFillIndices(size, 95)
	input2 := randomFillIndices(size, 90)

	out := make([]uint16, size*2)
	cacheMap := map[uint16]uint8{}

	cache := make([]uint16, size*2)
	cache3 := make([]uint16, size*2)

	for i := 0; i < testI; i++ {
		intersectSlowResult := lists.Intersect(input, input2, out, cacheMap)
		intersectFastResult := lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
		intersectFastestResult := lists.IntersectIndicesFastTwoListBitset(input, input2, out)

		if intersectFastResult != intersectSlowResult && intersectFastResult != intersectFastestResult {
			t.Errorf("Expected [slow=%d] but got [fast = %d], [fastest = %d]", intersectSlowResult, intersectFastResult, intersectFastestResult)
		}

		t.Logf("[slow=%d] [fast = %d], [fastest = %d]", intersectSlowResult, intersectFastResult, intersectFastestResult)
	}

}
