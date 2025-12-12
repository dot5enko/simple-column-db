package schemamanager

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

type SchemaManager struct {
	schemas map[string]schema.Schema
	blocks  map[schema.BlockUniqueId]BlockRuntimeInfo

	config ManagerConfig
}

func New(config ManagerConfig) (*SchemaManager, error) {
	return &SchemaManager{
		schemas: make(map[string]schema.Schema),
		blocks:  make(map[schema.BlockUniqueId]BlockRuntimeInfo),
		config:  config,
	}, nil
}

const BlockRowsSize = 32 * 1024 // 32k rows per block
