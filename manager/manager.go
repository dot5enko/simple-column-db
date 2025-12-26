package manager

import (
	"runtime"

	"github.com/dot5enko/simple-column-db/manager/executor"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
)

type ManagerConfig struct {
	PathToStorage string

	CacheMaxBytes uint64

	ExecutorsMaxConcurentThreads int
}

type Manager struct {
	config ManagerConfig

	Slabs   *meta.SlabManager
	Planner *QueryPlanner
	Meta    *meta.MetaManager

	queryOptions query.QueryOptions

	chunksQueue chan *executor.ChunkProcessingTask
}

func (m *Manager) SetQueryOptions(qopts query.QueryOptions) {
	m.queryOptions = qopts
}

func New(config ManagerConfig) *Manager {

	man := &Manager{
		Planner:     NewQueryPlanner(),
		Meta:        meta.NewMetaManager(config.PathToStorage),
		chunksQueue: make(chan *executor.ChunkProcessingTask, 100),
	}

	man.Slabs = meta.NewSlabManager(config.PathToStorage, man.Meta)

	{ // executor cache setup
		maxThreadsCache := config.ExecutorsMaxConcurentThreads
		if maxThreadsCache == 0 {
			maxThreadsCache = runtime.NumCPU()
		}

		// set default value if not specified
		config.ExecutorsMaxConcurentThreads = maxThreadsCache
	}

	man.config = config

	loadErr := man.Meta.LoadSchemesFromDisk()
	if loadErr != nil {
		panic(loadErr) // todo return error
	}

	return man

}
