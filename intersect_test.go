package main

import (
	"math/rand"
	"testing"

	"github.com/dot5enko/simple-column-db/lists"
)

const blocksize = 32 * 1024 // ~32k rows per block

func randomFillIndices(n int, fillPercent int) []uint16 {
	out := make([]uint16, 0, n*fillPercent/100)
	for i := 0; i < n; i++ {
		if rand.Intn(100) < fillPercent {
			out = append(out, uint16(i))
		}
	}
	return out
}

func randomFillIndicesWithBounds(n int, fillPercent int) (output []uint16, start, end uint16) {
	out := make([]uint16, 0, n*fillPercent/100)
	for i := 0; i < n; i++ {
		if rand.Intn(100) < fillPercent {
			out = append(out, uint16(i))
		}
	}
	return out, 0, 0
}

func BenchmarkIntersectFastRandHalfSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 15)

	out := make([]uint16, blocksize*2)
	cache := make([]uint16, blocksize*2)
	cache3 := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastRandSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 35)
	input2 := randomFillIndices(blocksize, 30)

	out := make([]uint16, blocksize*2)
	cache := make([]uint16, blocksize*2)
	cache3 := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastRandFull(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 80)

	out := make([]uint16, blocksize*2)
	cache := make([]uint16, blocksize*2)
	cache3 := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoList(input, input2, cache, cache3, out)
	}

}

func BenchmarkIntersectFastestRandSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 35)
	input2 := randomFillIndices(blocksize, 30)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}

}

func BenchmarkIntersectFastestRandFull(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 80)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}

}

func BenchmarkIntersectFastestRandHalfSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 15)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}
}

// fastest optimized

func BenchmarkIntersectFastestOptimizedRandSparse(t *testing.B) {

	input, i1start, i1end := randomFillIndicesWithBounds(blocksize, 35)
	input2, i2start, i2end := randomFillIndicesWithBounds(blocksize, 30)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitsetOptimized(input, input2, out, i1start, i1end, i2start, i2end)
	}

}

func BenchmarkIntersectFastesOptimizedRandFull(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 80)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}

}

func BenchmarkIntersectFastestOptimizedRandHalfSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 15)

	out := make([]uint16, blocksize*2)

	for t.Loop() {
		lists.IntersectIndicesFastTwoListBitset(input, input2, out)
	}
}

// optimized end

func BenchmarkIntersectSlowSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 35)
	input2 := randomFillIndices(blocksize, 30)

	out := make([]uint16, blocksize*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func BenchmarkIntersectSlowFull(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 70)

	out := make([]uint16, blocksize*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func BenchmarkIntersectSlowHalfSparse(t *testing.B) {

	input := randomFillIndices(blocksize, 85)
	input2 := randomFillIndices(blocksize, 15)

	out := make([]uint16, blocksize*2)
	cache := map[uint16]uint8{}

	for t.Loop() {
		lists.Intersect(input, input2, out, cache)
	}
}

func TestMergeIsCorrect(t *testing.T) {
	testI := 5

	input := randomFillIndices(blocksize, 95)
	input2 := randomFillIndices(blocksize, 90)

	out := make([]uint16, blocksize*2)
	cacheMap := map[uint16]uint8{}

	cache := make([]uint16, blocksize*2)
	cache3 := make([]uint16, blocksize*2)

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
