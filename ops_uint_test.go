package main

import (
	"math/rand"
	"testing"

	"github.com/dot5enko/simple-column-db/ops"
)

func TestRangeTail(t *testing.T) {
	size := 3

	input := []uint64{1050, 9000, 2000}

	var fromBounds uint64 = 1024
	var toBounds uint64 = 8192

	out := make([]uint16, size)

	resultSize := ops.CompareValuesAreInRange(input[:], fromBounds, toBounds, out)

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d", 1, resultSize)
	} else if out[1] != 2 {
		t.Errorf("result compare Expected %v but got %v", input[0], 0)
	}

}

func TestRangeBlockAndTail(t *testing.T) {
	size := 9

	input := []uint64{0, 0, 0, 1, 0, 0, 0, 7000, 1500}

	var fromBounds uint64 = 1024
	var toBounds uint64 = 8192

	out := make([]uint16, size)

	resultSize := ops.CompareValuesAreInRange(input[:], fromBounds, toBounds, out)

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d", 1, resultSize)
	} else if out[1] != 8 {
		t.Errorf("result compare Expected %v but got %v", out[1], 0)
	}

}

func BenchmarkRange(b *testing.B) {

	size := 40000

	input := make([]uint64, size)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))
		input[i] = val
	}

	var fromBounds uint64 = 4096
	var toBounds uint64 = 8192

	out := make([]uint16, size)

	for b.Loop() {
		ops.CompareValuesAreInRange(input[:], fromBounds, toBounds, out)
	}

}
