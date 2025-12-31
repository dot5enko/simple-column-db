package executor

import (
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/lightsync"
	executortypes "github.com/dot5enko/simple-column-db/manager/executor/executor_types"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

type TaskStatus struct {
	StartTime time.Time

	ChunksTotal     int
	ChunksProcessed atomic.Int32

	Err       atomic.Bool
	ErrObject error

	ChunkResult executortypes.ChunkFilterProcessResult

	Waiter lightsync.Waiter
}

type ChunkProcessingTask struct {
	Bchunk *query.BlockChunk
	Slabs  *meta.SlabManager
	Plan   *query.QueryPlan

	ChunkIdx int

	Status *TaskStatus
}
