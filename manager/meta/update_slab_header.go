package meta

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/schema"
)

func (sm *SlabManager) UpdateSlabHeaderOnDisk(s schema.Schema, slab *schema.DiskSlabHeader) error {

	serializedBytes, headerBytesErr := slab.WriteTo(sm.SlabHeaderReaderBuffer[:])
	if headerBytesErr != nil {
		return fmt.Errorf("unable to finalize block, slab header won't serialize : %s", headerBytesErr.Error())
	} else {

		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to update slab header : %s", slabErr.Error())
		}

		defer fileManager.Close()
		return fileManager.WriteAt(sm.SlabHeaderReaderBuffer[:], 0, serializedBytes)
	}
}
