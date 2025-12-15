package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/manager"
	"github.com/dot5enko/simple-column-db/schema"
)

func BenchmarkTransponeSlow(b *testing.B) {

	const size = 40000

	input := make([]uint64, size)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))
		input[i] = val
	}

	inputRows := []any{}

	for idx, it := range input {
		inputRows = append(inputRows, []any{uint16(idx), it})
	}

	var outputInts [size]uint64

	for b.Loop() {
		manager.CollectTypedDataToArray(inputRows, outputInts[:], schema.Uint64FieldType, 1)
		manager.CollectTypedDataToArray(inputRows, outputInts[:], schema.Uint16FieldType, 0)
	}

}

func BenchmarkTransponeFast(b *testing.B) {

	const size = 40000

	singleRowSize := 8 + 2
	bigBuffer := make([]byte, size*singleRowSize)

	writer := bits.NewEncodeBuffer(bigBuffer, binary.LittleEndian)

	totalTestSum := 0

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))

		totalTestSum += int(val)

		writer.PutUint64(val)
		writer.PutUint16(uint16(i))
	}

	// xpos := writer.Position()
	// log.Printf("last pos : %d", xpos)

	var outputInts [size]uint64

	readBuffer := bytes.NewReader(bigBuffer)
	binWriter := bits.NewReader(readBuffer, binary.LittleEndian)

	testOutput := func(inp []uint64, t *testing.B) {

		totalSum := 0
		for _, v := range inp {
			totalSum += int(v)
		}

		if totalSum != totalTestSum {
			t.Errorf("result data mismatch, got %d, expected : %d", totalSum, totalTestSum)
			t.FailNow()
		}

	}

	// log.Printf("totalSum : %d", totalTestSum)

	for b.Loop() {

		binWriter.Reset()

		manager.CollectTypedDataToArrayFromBinaryBuffer(binWriter,
			outputInts[:], schema.Uint64FieldType,
			0, singleRowSize, size,
		)

		testOutput(outputInts[:], b)
	}

}

func BenchmarkTransponeFastest(b *testing.B) {

	const size = 40000

	singleRowSize := 8 + 2
	bigBuffer := make([]byte, size*singleRowSize)

	writer := bits.NewEncodeBuffer(bigBuffer, binary.LittleEndian)

	totalTestSum := 0

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))

		totalTestSum += int(val)

		writer.PutUint64(val)
		writer.PutUint16(uint16(i))
	}

	var outputInts [size]uint64

	testOutput := func(inp []uint64, t *testing.B) {

		totalSum := 0
		for _, v := range inp {
			totalSum += int(v)
		}

		if totalSum != totalTestSum {
			t.Errorf("result data mismatch, got %d, expected : %d", totalSum, totalTestSum)
			t.FailNow()
		}
	}

	outBuffer := make([]byte, size*8)

	for b.Loop() {

		manager.CollectTypedDataToArrayFromBinaryBufferFast(bigBuffer,
			outputInts[:], schema.Uint64FieldType,
			0, singleRowSize, size,
			outBuffer[:],
		)

		testOutput(outputInts[:], b)
	}

}

func TestTranspone(t *testing.T) {
	const size = 40000

	singleRowSize := 8 + 2
	bigBuffer := make([]byte, size*singleRowSize)

	writer := bits.NewEncodeBuffer(bigBuffer, binary.LittleEndian)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))

		writer.PutUint64(val)
		writer.PutUint16(uint16(i))
	}

	// xpos := writer.Position()
	// log.Printf("last pos : %d", xpos)

	var outputInts [size]uint64

	readBuffer := bytes.NewReader(bigBuffer)
	binWriter := bits.NewReader(readBuffer, binary.LittleEndian)

	// pos, _ := readBuffer.Seek(0, io.SeekCurrent)

	manager.CollectTypedDataToArrayFromBinaryBuffer(binWriter,
		outputInts[:], schema.Uint64FieldType,
		0, singleRowSize, size,
	)
}
