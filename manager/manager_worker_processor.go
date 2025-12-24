package manager

import (
	"context"
	"log/slog"
	"sync"

	"github.com/dot5enko/simple-column-db/manager/executor"
)

// todo handle context
func (sm *Manager) StartWorkers(routines int, ctx context.Context) *sync.WaitGroup {

	slog.Info("starting workers", "max_executors", routines)

	return StartWorkerThreads(routines, func(threadId int) {
		executor.ChunkSingleThreadProcessor(threadId, sm.Slabs, sm.chunksQueue)
	})
}
