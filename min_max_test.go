package main

import (
	"math/rand"
	"testing"

	"github.com/dot5enko/simple-column-db/schema"
)

func BenchmarkMinMaxRand(b *testing.B) {

	size := 40000

	input := make([]uint64, size)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))
		input[i] = val
	}

	var result schema.BoundsFloat

	for b.Loop() {
		result = schema.GetMaxMinBoundsFloat(input)
	}

	b.Logf("min : %d, max : %d", result.Min, result.Max)
}

func TestMinMax(b *testing.T) {

	minVal := float64(0)
	maxVal := float64(7000)

	input := []float64{minVal, maxVal, 1, 2, 3, 4, 5, 6, 0}

	result := schema.GetMaxMinBoundsFloat(input[:])

	if result.Max != maxVal {
		b.Errorf("Expected %d but got %d", maxVal, result.Max)
	}

	if result.Min != minVal {
		b.Errorf("Expected %d but got %d", minVal, result.Min)
	}

}

func TestMinMaxFloat(b *testing.T) {

	minVal := -10.0
	maxVal := 7000.0

	input := []float64{minVal, maxVal, 1, 2, 3, 4, 5, 6, 0.0, 1000}

	result := schema.GetMaxMinBoundsFloat(input[:])

	if result.Max != maxVal {
		b.Errorf("Expected %.2f but got %.2f", maxVal, result.Max)
	}

	if result.Min != minVal {
		b.Errorf("Expected %.2f but got %.2f", minVal, result.Min)
	}

}
