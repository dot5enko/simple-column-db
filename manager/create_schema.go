package manager

import (
	"github.com/dot5enko/simple-column-db/schema"
)

func (sm *Manager) CreateSchemaIfNotExists(schemaConfig schema.Schema) error {
	return sm.Slabs.CreateSchema(schemaConfig)
}
