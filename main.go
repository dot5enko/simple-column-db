package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/manager"
	"github.com/dot5enko/simple-column-db/schema"
)

func testCycles(n int, label string, testSize int, cb func()) {

	before := time.Now()

	for range n {
		cb()
	}

	after := time.Since(before)

	perCycle := after.Nanoseconds() / int64(testSize)
	log.Printf(" %s per cycle : %d/ns", label, perCycle)
}

func gen_fake_data[T uint64 | float64](size int, fileName string) {

	data := make([]T, size)

	for i := 0; i < size; i++ {
		val := T(rand.Int63n(50000))
		data[i] = val
	}

	for i := 0; i < 10; i++ {
		log.Printf("%d : %v", i, (data)[i])
	}

	log.Printf("generated %d items ", len(data))

	fw, ferr := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if ferr != nil {
		panic(ferr)
	}

	_, dumpErr := io.DumpNumbersArrayBlock(fw, data)

	if dumpErr != nil {
		panic(dumpErr)
	}
}

func read_array_data[T any](fileName string, size int, typ schema.FieldType) (data []T) {

	reader := io.NewFileReader(fileName)
	reader.Open(true)

	elementSize := typ.Size()
	blockSize := elementSize * size

	log.Printf(" << about to read %v bytes from file >> ", blockSize)

	buffer := make([]byte, blockSize)

	readErr := reader.ReadAt(buffer, 0, blockSize)
	if readErr != nil {
		panic(readErr)
	}

	return bits.MapBytesToArray[T](buffer, size)
}

func main() {

	m := manager.New(manager.ManagerConfig{
		PathToStorage: "./storage",
		CacheMaxBytes: 0,
	})

	testSchemaName := "health_cheks_"
	//+ uuid.NewString()[:5]

	shemaCreatedErr := m.CreateSchemaIfNotExists(schema.Schema{
		Name: testSchemaName,
		Columns: []schema.SchemaColumn{
			{Name: "created_at", Type: schema.Uint64FieldType},
			{Name: "value", Type: schema.Float32FieldType},
		},
	})

	if shemaCreatedErr != nil {
		panic(shemaCreatedErr)
	}

	ingest_data_into_simple_metric_value(m, testSchemaName, 1_000_000)

	beforeIndex := time.Hour * 24 * 30 * 12 * 4

	before := time.Now()
	result, qerr := m.Get(testSchemaName, manager.Query{
		Filter: []manager.FilterCondition{
			{
				Field:     "created_at",
				Operand:   manager.RANGE,
				Arguments: []any{uint64(time.Now().Add(-beforeIndex).Unix()), uint64(time.Now().Unix())},
			},
			{
				Field:     "value",
				Operand:   manager.GT,
				Arguments: []any{float32(0.7)},
			},
		},
		Select: []manager.Selector{
			{
				Arguments: []any{"avg", "value"},
				Alias:     "avg_value",
			},
			{
				Arguments: []any{"count"},
				Alias:     "total_count",
			},
		},
	})

	end := time.Since(before)
	log.Printf("query took %.2fms", end.Seconds()*1000)

	if qerr != nil {
		panic(fmt.Sprintf("unable to get data out of schema: %s", qerr.Error()))
	} else {
		log.Printf("query result : %v", result)
	}

}

func ingest_data_into_simple_metric_value(m *manager.Manager, testSchemaName string, dataSize int) {

	fields := []string{"created_at", "value"}
	testRows := dataSize

	binWriter := bits.NewEncodeBuffer([]byte{}, binary.LittleEndian)
	binWriter.EnableGrowing()

	frameStart := time.Hour * 24 * 30 * 12 * 5
	startTime := time.Now().Add(-frameStart).Unix()

	for i := 0; i < testRows; i++ {

		timeOffset := uint64(i * 60)
		timeVal := uint64(startTime) + timeOffset
		randVal := 0.5 + rand.Float32()*(0.8-0.5)

		binWriter.PutUint64(timeVal)
		binWriter.PutFloat32(randVal)

	}

	before := time.Now()
	ingestErr := m.Ingest(testSchemaName, manager.IngestBufferFromBinary(binWriter.Bytes(), fields))
	after := time.Since(before)

	log.Printf("ingested %d rows in %.2f ms", testRows, after.Seconds()*1000)

	if ingestErr != nil {
		panic(ingestErr)
	}

}
