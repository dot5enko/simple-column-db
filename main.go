package main

import (
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

	dumpErr := io.DumpNumbersArrayBlock(fw, data)

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

	shemaCreatedErr := m.CreateSchema(schema.Schema{
		Name: "health_cheks",
		Columns: []schema.SchemaColumn{
			{Name: "created_at", Type: schema.Uint64FieldType},
			{Name: "value", Type: schema.Uint64FieldType},
		},
	})

	if shemaCreatedErr != nil {
		panic(shemaCreatedErr)
	}

}
