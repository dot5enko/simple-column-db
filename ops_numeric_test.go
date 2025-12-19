package main

import (
	"log"
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

	resultSize := ops.CompareValuesAreInRangeUnsignedInts(input[:], fromBounds, toBounds, out)

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d", 1, resultSize)
	} else if out[1] != 2 {
		t.Errorf("result compare Expected %v but got %v", input[0], 0)
	}

}

func TestRangeTailFloat(t *testing.T) {
	size := 3

	input := []float64{1050, 9000, 2000}

	var fromBounds float64 = 1024
	var toBounds float64 = 8192

	out := make([]uint16, size)

	resultSize := ops.CompareValuesAreInRangeFloats(input[:], fromBounds, toBounds, out)

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d", 1, resultSize)
	} else if out[1] != 2 {
		t.Errorf("result compare Expected %v but got %v", input[0], 0)
	}

}

func TestRangeBlockAndTailFloat(t *testing.T) {
	size := 9

	input := []float64{0, 0, 0, 1, 0, 0, 0, 7000, 1500}

	var fromBounds float64 = 1024.0
	var toBounds float64 = 8192

	out := make([]uint16, size)

	resultSize := ops.CompareValuesAreInRangeFloats(input[:], fromBounds, toBounds, out)

	valuesFiltered := []float64{}
	for _, v := range out {
		valuesFiltered = append(valuesFiltered, input[v])
	}

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d. filtered : %v", 2, resultSize, valuesFiltered)
	} else if out[1] != 8 {
		t.Errorf("result compare Expected %v but got %v", out[1], 0)
	}

}

func TestRangeBlockAndTail(t *testing.T) {
	size := 9

	input := []uint64{0, 0, 0, 1, 0, 0, 0, 7000, 1500}

	var fromBounds uint64 = 1024
	var toBounds uint64 = 8192

	out := make([]uint16, size)

	resultSize := ops.CompareValuesAreInRangeUnsignedInts(input[:], fromBounds, toBounds, out)

	if resultSize != 2 {
		t.Errorf("Expected %d but got %d", 1, resultSize)
	} else if out[1] != 8 {
		t.Errorf("result compare Expected %v but got %v", out[1], 0)
	}

}

func BenchmarkRangeUnsigned(b *testing.B) {

	size := 40000

	var fromBounds uint64 = 4096
	var toBounds uint64 = 8192

	totalCount := 0
	totalSum := 0

	input := make([]uint64, size)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))
		input[i] = val

		if val >= fromBounds && val <= toBounds {
			totalCount++
			totalSum += int(val)
		}

	}

	out := make([]uint16, size)

	log.Printf("amount %d", totalCount)

	for b.Loop() {
		totalBenchCount := ops.CompareValuesAreInRangeUnsignedInts(input[:], fromBounds, toBounds, out)
		if totalCount != totalBenchCount {
			b.Fatalf("Benchmark failed: expected %d but got %d", totalCount, totalBenchCount)
		}
	}

}

func BenchmarkRangeFloats(b *testing.B) {

	size := 40000

	var fromBounds float64 = 4096
	var toBounds float64 = 8192

	totalCount := 0
	totalSum := 0

	input := make([]float64, size)

	for i := 0; i < size; i++ {
		val := float64(rand.Int63n(50000))
		input[i] = val

		if val >= fromBounds && val <= toBounds {
			totalCount++
			totalSum += int(val)
		}

	}

	out := make([]uint16, size)

	log.Printf("amount %d", totalCount)

	for b.Loop() {
		totalBenchCount := ops.CompareValuesAreInRangeFloats(input[:], fromBounds, toBounds, out)
		if totalCount != totalBenchCount {
			b.Fatalf("Benchmark failed: expected %d but got %d", totalCount, totalBenchCount)
		}
	}

}
