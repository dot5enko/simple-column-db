package manager

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (m *SlabManager) IngestIntoBlock(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
	columnDataArray any,
	dataArrayStartOffset int,
) (int, bool, error) {

	data, err := m.LoadBlockToRuntimeBlockData(schemaObject, slab, block)

	if err != nil {
		return 0, false, fmt.Errorf("unable to load block into runtime: %s", err.Error())
	} else {
		written, writeErr, bounds := data.Write(columnDataArray, dataArrayStartOffset, slab.Type)
		if writeErr != nil {
			return written, false, writeErr
		} else {

			slabHeaderChanged := slab.Bounds.Morph(bounds)

			data.Header.Bounds.Morph(bounds)

			blockFinished := false

			if data.Items == data.Cap {
				// finalize block

				slab.BlocksFinalized += 1
				// write updated slab header content to disk

				slabHeaderChanged = true
				blockFinished = true
			}

			if slabHeaderChanged {
				updateSlabHeaderErr := m.UpdateSlabHeaderOnDisk(schemaObject, slab)
				if updateSlabHeaderErr != nil {
					return written, blockFinished, fmt.Errorf("unable to update slab info: %s", updateSlabHeaderErr.Error())
				}
			}

			// write block header and data to disk
			diskBlockUpdateErr := m.UpdateBlockHeaderAndDataOnDisk(schemaObject, slab, data)

			return written, blockFinished, diskBlockUpdateErr
		}

	}

}
