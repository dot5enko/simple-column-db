package executor

import (
	"sync"
	"sync/atomic"

	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

type TaskStatus struct {
	ChunksTotal     int
	ChunksProcessed atomic.Int32

	Err       atomic.Bool
	ErrObject error

	ChunkResult ChunkFilterProcessResult

	Waiter sync.WaitGroup
	Lock   sync.Mutex
}

type ChunkProcessingTask struct {
	Bchunk *query.BlockChunk
	Slabs  *meta.SlabManager
	Plan   *query.QueryPlan

	ChunkIdx int

	Status *TaskStatus
}
