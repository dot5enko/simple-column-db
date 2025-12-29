package manager

import (
	"context"
	"log/slog"
	"sync"

	"github.com/dot5enko/simple-column-db/lightsync"
	"github.com/dot5enko/simple-column-db/manager/executor"
)

// todo handle context
func (sm *Manager) StartWorkers(ctx context.Context) *sync.WaitGroup {

	workersCount := sm.config.ExecutorsMaxConcurentThreads

	slog.Info("starting workers", "workers_count", workersCount)

	sm.workerQueues = make([]*lightsync.RingQueue[executor.ChunkProcessingTask], workersCount)
	for threadId := range sm.workerQueues {
		// todo move size into config
		sm.workerQueues[threadId] = lightsync.NewRingQueue[executor.ChunkProcessingTask](1024)
	}

	return StartWorkerThreads(workersCount, func(workerId int) {
		executor.ChunkSingleThreadProcessor(workerId, sm.Slabs, sm.workerQueues[workerId])
	})
}
