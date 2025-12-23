package manager

import (
	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/manager/meta"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type BlockRuntimeInfo struct {
	Val *schema.RuntimeBlockData

	BlockHeader *schema.DiskHeader
	SlabHeader  *schema.DiskSlabHeader

	// 32 filters max ?
	HeaderFilterMatchResult [16]schema.BoundsFilterMatchResult
}

type ManagerConfig struct {
	PathToStorage string

	CacheMaxBytes uint64
}

type Manager struct {
	config ManagerConfig

	Slabs   SlabManager
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
		Slabs: SlabManager{
			storagePath: config.PathToStorage,
			// caches
			cache:         map[[32]byte]BlockCacheItem{},
			slabCacheItem: map[uuid.UUID]*cache.SlabCacheItem{},
			cacheManager:  cache.NewSlabCacheManager(),
		},
		// indiceMergerPool: &unmergedPool,
	}

	man.Slabs.cacheManager.Prefill(32)

	loadErr := man.Meta.LoadSchemesFromDisk()
	if loadErr != nil {
		panic(loadErr) // todo return error
	}

	return man

}
