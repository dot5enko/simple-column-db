package manager

import (
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (m *SlabManager) IngestIntoBlock(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
	tm *Manager,
	columnDataArray any,
	dataArrayStartOffset int,
) (int, bool, error) {

	data, err := m.LoadBlockToRuntimeBlockData(schemaObject, slab, block, tm)

	if err != nil {
		return 0, false, fmt.Errorf("unable to load block into runtime: %s", err.Error())
	} else {
		written, writeErr, bounds := data.Write(columnDataArray, dataArrayStartOffset, slab.Type)
		if writeErr != nil {
			return written, false, writeErr
		} else {

			slabHeaderChanged := slab.Bounds.Morph(bounds)

			data.Header.Bounds.Morph(bounds)

			// update block header
			log.Printf(" block %s header not updated ", block.String())
			// recalc max/min values

			blockFinished := false

			if data.Items == data.Cap {
				// finalize block

				slab.BlocksFinalized += 1
				// write updated slab header content to disk

				slabHeaderChanged = true
				blockFinished = true
			}

			if slabHeaderChanged {
				updateSlabHeaderErr := tm.UpdateSlabHeaderOnDisk(schemaObject, slab)
				if updateSlabHeaderErr != nil {
					return written, blockFinished, fmt.Errorf("unable to update slab info: %s", updateSlabHeaderErr.Error())
				}
			}

			// write block header and data to disk
			diskBlockUpdateErr := tm.UpdateBlockHeaderAndDataOnDisk(schemaObject, slab, data)

			return written, blockFinished, diskBlockUpdateErr
		}

	}

}
