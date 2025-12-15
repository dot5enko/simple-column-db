package manager

import (
	"github.com/dot5enko/simple-column-db/schema"
)

type BlockRuntimeInfo struct {
	Val          any
	Synchronized bool
	Header       schema.DiskHeader
}

type ManagerConfig struct {
	PathToStorage string

	CacheMaxBytes uint64
}

type Manager struct {
	schemas map[string]schema.Schema
	blocks  map[schema.BlockUniqueId]BlockRuntimeInfo

	config ManagerConfig
}

func New(config ManagerConfig) *Manager {
	return &Manager{
		schemas: make(map[string]schema.Schema),
		blocks:  make(map[schema.BlockUniqueId]BlockRuntimeInfo),
		config:  config,
	}
}
