package main

import (
	"log"
	"math/rand"
	"os"
	"time"

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

func gen_fake_data(size int, fileName string) {

	data := make([]uint64, size)

	for i := 0; i < size; i++ {
		val := uint64(rand.Int63n(50000))
		data[i] = val
	}

	fw, ferr := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if ferr != nil {
		panic(ferr)
	}

	dumpErr := io.DumpNumbersArrayBlock(fw, data)

	if dumpErr != nil {
		panic(dumpErr)
	}
}

func main() {

	gen_fake_data(1000, "test1.bin")

	return

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
