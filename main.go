package main

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/io"
)

func testCycles(n int, label string, testSize int, cb func()) {

	before := time.Now()

	for i := 0; i < n; i++ {
		cb()
	}

	after := time.Since(before)

	perCycle := after.Nanoseconds() / int64(testSize)
	log.Printf(" %s per cycle : %d/ns", label, perCycle)
}

func main() {

	isDump := false

	const size = 4000
	testSize := 10
	fileName := "test.bin"

	if isDump {
		data := make([]uint64, size)

		for i := 0; i < size; i++ {
			val := uint64(rand.Int63n(50000))
			data[i] = val
			if i < testSize {
				log.Printf("%d %v", i, val)
			}
		}

		dumpErr := io.DumpNumbersArrayBlock(fileName, data)

		if dumpErr != nil {
			panic(dumpErr)
		}
	}

	readBackData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		panic(readErr)
	}

	// log.Printf("read %d bytes of input", len(readBackData))
	arrResult := bits.MapBytesToArray[[size]uint64](readBackData, size)
	// log.Printf("result pointer : %d", len(arrResult))

	for i := 0; i < testSize; i++ {
		val := arrResult[i]
		if i < 10 {
			log.Printf(" >> test %d %v", i, val)
		}
	}
}

func intersectSorted(a, b []uint64, out []uint64, cache map[uint64]uint8) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	// Use the smaller slice to build the map
	clear(cache)

	var other []uint64

	clear(cache)

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
