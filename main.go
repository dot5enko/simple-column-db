package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/manager"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"

	"net/http"
	_ "net/http/pprof"
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

	pprofEnabled := flag.Bool("pprof", false, "enable pprof server")
	testIterations := flag.Int("test_iterations", 1, "number of iterations")
	workerThreads := flag.Int("worker_threads", 1, "number of worker threads")

	flag.Parse()

	waiter := sync.WaitGroup{}
	waiter.Add(1)

	if *pprofEnabled {
		go func() {
			defer func() {
				waiter.Done()
				log.Printf(" >> done pprof server")
			}()
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

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
			{Name: "monitor_id", Type: schema.Uint64FieldType},
		},
	})

	if shemaCreatedErr != nil {
		panic(shemaCreatedErr)
	}

	// ingest_data_into_simple_metric_value(m, testSchemaName, 10_000_000, 8)

	beforeIndex := time.Hour * 24 * 30 * 12 * 4

	testN := *testIterations

	if *pprofEnabled {
		time.Sleep(time.Second * 5)
	}

	workersCtx, cancelWorkers := context.WithCancel(context.Background())

	m.StartWorkers(*workerThreads, workersCtx)

	for i := 0; i < testN; i++ {

		result, qerr := m.Query(testSchemaName, query.Query{
			Filter: []query.FilterCondition{
				{
					Field:     "created_at",
					Operand:   query.RANGE,
					Arguments: []any{uint64(time.Now().Add(-beforeIndex).Unix()), uint64(time.Now().Unix())},
				},
				{
					Field:     "monitor_id",
					Operand:   query.RANGE,
					Arguments: []any{uint64(4), uint64(6)},
				},
				{
					Field:     "value",
					Operand:   query.GT,
					Arguments: []any{float32(0.7999)},
				},
			},
			Select: []query.Selector{
				{
					Arguments: []any{"avg", "value"},
					Alias:     "avg_value",
				},
				{
					Arguments: []any{"count"},
					Alias:     "total_count",
				},
			},
		}, context.Background())

		if qerr != nil {
			panic(fmt.Sprintf("unable to get data out of schema: %s", qerr.Error()))
		} else {
			_ = result
		}
	}

	if *pprofEnabled {
		waiter.Wait()

	}

	cancelWorkers()

}

func ingest_data_into_simple_metric_value(m *manager.Manager, testSchemaName string, dataSize int, monitors int) {

	fields := []string{"created_at", "value", "monitor_id"}
	testRows := dataSize

	binWriter := bits.NewEncodeBuffer([]byte{}, binary.LittleEndian)
	binWriter.EnableGrowing()

	frameStart := time.Hour * 24 * 30 * 12 * 5
	startTime := time.Now().Add(-frameStart).Unix()

	for i := 0; i < testRows; i++ {

		monitorId := rand.Int63n(int64(monitors))

		timeOffset := uint64(i * 60)
		timeVal := uint64(startTime) + timeOffset
		randVal := 0.5 + rand.Float32()*(0.8-0.5)

		binWriter.PutUint64(timeVal)
		binWriter.PutFloat32(randVal)
		binWriter.PutUint64(uint64(monitorId))

	}

	before := time.Now()
	ingestErr := m.Ingest(testSchemaName, manager.IngestBufferFromBinary(binWriter.Bytes(), fields))
	after := time.Since(before)

	log.Printf("ingested %d rows in %.2f ms", testRows, after.Seconds()*1000)

	if ingestErr != nil {
		panic(ingestErr)
	}

}
