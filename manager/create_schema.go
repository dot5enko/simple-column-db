package manager

import (
	"fmt"
	"os"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (sm *Manager) CreateSchemaIfNotExists(schemaConfig schema.Schema) error {

	storagePath := sm.Slabs.getAbsStoragePath(schemaConfig.Name)

	_, err := os.Stat(storagePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("unable to check schema folder existence : %s", err.Error())
		} // path does not exist
	} else {
		return nil
	}

	_, err = sm.Slabs.createStoragePathIfNotExists(schemaConfig.Name)

	if err != nil {
		return fmt.Errorf("unable to create schema folder: `%s`", err.Error())
	}

	// for each column create slab on disk
	for colIdx := range schemaConfig.Columns {

		newSlab, slabCreationErr := sm.Slabs.NewSlabForColumn(schemaConfig, schemaConfig.Columns[colIdx], 0)
		if slabCreationErr != nil {
			return slabCreationErr
		}

		{
			col := &schemaConfig.Columns[colIdx]

			if col.Slabs == nil {
				col.Slabs = []uuid.UUID{}
			}

			col.Slabs = append(col.Slabs, newSlab.Uid)
			col.ActiveSlab = newSlab.Uid
		}

	}

	// TODO: should be one api
	// store once per all columns/slabs
	storeErr := sm.Meta.StoreSchemeToDisk(schemaConfig)
	if storeErr != nil {
		return fmt.Errorf("unable to save schema config to disk : %s", storeErr.Error())
	}
	sm.Meta.AddSchema(&schemaConfig)

	return nil
}
