package manager

import (
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type ManagerConfig struct {
	PathToStorage string

	CacheMaxBytes uint64
}

type Manager struct {
	config ManagerConfig

	Slabs   *meta.SlabManager
	Planner *query.QueryPlanner
	Meta    *meta.MetaManager

	BlockBuffer [schema.TotalHeaderSize]byte
}

func New(config ManagerConfig) *Manager {

	// var unmergedPool = sync.Pool{
	// 	New: func() any {
	// 		return lists.NewUnmerged() // allocates zeroed object
	// 	},
	// }

	man := &Manager{
		Planner: query.NewQueryPlanner(),
		config:  config,
		Meta:    meta.NewMetaManager(config.PathToStorage),

		// indiceMergerPool: &unmergedPool,
	}

	man.Slabs = meta.NewSlabManager(config.PathToStorage, man.Meta)

	loadErr := man.Meta.LoadSchemesFromDisk()
	if loadErr != nil {
		panic(loadErr) // todo return error
	}

	return man

}
