package compression

import (
	"testing"
)

//go:noescape
//go:nosplit
func processBlockOfSingleByteDiffsOptimized(buf []byte, bufIdx int, metaCount int, prev uint64, out []uint64, idx int) (int, int, uint64)

func BenchmarkProcessBlockOfSingleByteDiffsOptimized(t *testing.B) {
	// Sample input: buf with alternating even/odd bytes for positive/negative deltas
	buf := []byte{0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255} // Deltas: +0, -1, +1, -2, +2, -3, +127, -128
	out := make([]uint64, 32)

	startPrev := uint64(1000) // Arbitrary positive start
	metaCount := 32           // Limit to 5 writes (less than 64 and len(out))

	for t.Loop() {

		startBufIdx := 0
		startIdx := 0

		newBufIdx, newIdx, _ := processBlockOfSingleByteDiffsOptimized(
			buf,
			startBufIdx,
			metaCount,
			startPrev,
			out,
			startIdx,
		)

		if newBufIdx != 32 {
			t.FailNow()
		}

		if newIdx != 32 {
			t.FailNow()
		}

		if out[0] != 1000 {
			t.FailNow()
		}

	}
}

func BenchmarkProcessBlockOfSingleByteDiffs(t *testing.B) {
	// Sample input: buf with alternating even/odd bytes for positive/negative deltas
	buf := []byte{0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255, 0, 1, 2, 3, 4, 5, 254, 255} // Deltas: +0, -1, +1, -2, +2, -3, +127, -128
	out := make([]uint64, 32)

	startPrev := uint64(1000) // Arbitrary positive start
	metaCount := 32           // Limit to 5 writes (less than 64 and len(out))

	for t.Loop() {

		startBufIdx := 0
		startIdx := 0

		newBufIdx, newIdx, _ := processBlockOfSingleByteDiffs(
			buf,
			startBufIdx,
			metaCount,
			startPrev,
			out,
			startIdx,
		)

		if newBufIdx != 32 {
			t.FailNow()
		}

		if newIdx != 32 {
			t.FailNow()
		}

		if out[0] != 1000 {
			t.FailNow()
		}

	}
}

func BenchmarkProcessBlockOfSingleByteDiffsNative(t *testing.B) {
	// Sample input: buf with alternating even/odd bytes for positive/negative deltas
	buf := []byte{0, 1, 2, 3, 4, 5, 254, 255,
		0, 1, 2, 3, 4, 5, 254, 255,
		0, 1, 2, 3, 4, 5, 254, 255,
		0, 1, 2, 3, 4, 5, 254, 255,
	} // Deltas: +0, -1, +1, -2, +2, -3, +127, -128
	out := make([]uint64, 32)

	startPrev := uint64(1000) // Arbitrary positive start
	metaCount := 32           // Limit to 5 writes (less than 64 and len(out))

	for t.Loop() {

		startBufIdx := 0
		startIdx := 0

		prev := startPrev

		for j := 0; j < 64 && startIdx < metaCount; j++ {
			v := uint64(buf[startBufIdx])
			startBufIdx++
			prev += uint64(int64((v >> 1) ^ uint64(-(v & 1))))
			out[startIdx] = prev
			startIdx++
		}

		if startBufIdx != 32 {
			t.FailNow()
		}

		if startIdx != 32 {
			t.FailNow()
		}

		if out[0] != 1000 {
			t.FailNow()
		}

	}
}
