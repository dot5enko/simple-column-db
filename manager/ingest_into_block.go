package manager

import (
	"fmt"
	"time"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

type IngestStats struct {
	Written       int
	BlockFinished bool

	IoTime  time.Duration
	IoCalls int
}

func (m *SlabManager) IngestIntoBlock(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
	columnDataArray any,
	dataArrayStartOffset int,
) (IngestStats, error) {

	stats := IngestStats{}

	color.Yellow(" ++ ingesting into block %s, slab %s", block.String(), slab.Uid.String())

	data, err := m.LoadBlockToRuntimeBlockData(schemaObject, slab, block)

	if err != nil {
		return stats, fmt.Errorf("unable to load block into runtime: %s", err.Error())
	} else {
		written, writeErr, bounds := data.Write(columnDataArray, dataArrayStartOffset, slab.Type)
		if writeErr != nil {
			stats.Written = written
			return stats, writeErr
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

			stats.BlockFinished = blockFinished

			if slabHeaderChanged {

				ioStart := time.Now()
				updateSlabHeaderErr := m.UpdateSlabHeaderOnDisk(schemaObject, slab)
				ioTook := time.Since(ioStart)

				stats.IoTime += ioTook

				if updateSlabHeaderErr != nil {

					stats.Written = written

					return stats, fmt.Errorf("unable to update slab info: %s", updateSlabHeaderErr.Error())
				}
			}

			// write block header and data to disk
			ioStart := time.Now()
			diskBlockUpdateErr := m.UpdateBlockHeaderAndDataOnDisk(schemaObject, slab, data)
			ioTook := time.Since(ioStart)

			stats.Written = written
			stats.IoTime += ioTook
			stats.IoCalls = 2

			return stats, diskBlockUpdateErr
		}

	}

}
