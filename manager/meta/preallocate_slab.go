package meta

import (
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (sm *SlabManager) preallocateSlab(s schema.Schema, uid uuid.UUID) error {

	fileManager, err := sm.GetSlabFile(s, uid, true)

	if err != nil {
		return err
	}

	defer fileManager.Close()

	// hard guess that block headers are no more than 20% of slab size
	return fileManager.FillZeroes(0, int(float64(schema.SlabDiskContentsUncompressed)*1.2))
}
