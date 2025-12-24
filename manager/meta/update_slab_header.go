package meta

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/schema"
)

// todo check thread safety

func (sm *SlabManager) UpdateSlabHeaderOnDisk(s schema.Schema, slab *schema.DiskSlabHeader) error {

	headerReadBuffer, headerBufferIdx := sm.headerReaderBufferRing.Get()

	defer func() {
		sm.headerReaderBufferRing.Return(headerBufferIdx)
	}()

	serializedBytes, headerBytesErr := slab.WriteTo(headerReadBuffer)
	if headerBytesErr != nil {
		return fmt.Errorf("unable to finalize block, slab header won't serialize : %s", headerBytesErr.Error())
	} else {

		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to update slab header : %s", slabErr.Error())
		}

		defer fileManager.Close()
		return fileManager.WriteAt(headerReadBuffer, 0, serializedBytes)
	}
}
