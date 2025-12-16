package manager

import (
	"bytes"
	"fmt"
	"log"

	"github.com/dot5enko/simple-column-db/io"
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
) (int, error) {

	data, err := m.LoadBlockToRuntimeBlockData(schemaObject, slab, block, tm)

	if err != nil {
		return 0, err
	} else {
		written, writeErr, bounds := data.Write(columnDataArray, dataArrayStartOffset, slab.Type)
		if writeErr != nil {
			return written, writeErr
		} else {

			slabHeaderChanged := slab.Bounds.Morph(bounds)

			data.Header.Bounds.Morph(bounds)

			// update block header
			log.Printf(" block %s header not updated ", block.String())
			// recalc max/min values

			if data.Items == data.Cap {
				// finalize block

				slab.BlocksFinalized += 1
				// write updated slab header content to disk

				slabHeaderChanged = true
			}

			if slabHeaderChanged {
				updateSlabHeaderErr := tm.UpdateSlabHeaderOnDisk(schemaObject, slab)
				if updateSlabHeaderErr != nil {
					return written, fmt.Errorf("unable to update slab info: %s", updateSlabHeaderErr.Error())
				}
			}

			// write update block content to disk

			writeBuf := bytes.NewBuffer(m.BufferForCompressedData10Mb[:0])
			writeErr := io.DumpNumbersArrayBlockAny(writeBuf, data.DataTypedArray)
			if writeErr != nil {
				return written, fmt.Errorf("unable to finalize block : %s", writeErr.Error())
			}

			// m.WriteBlockHeaderToDisk(slab, block, data)
			// m.WriteSlabDataToDisk(slab, block, data, writeBuf.Bytes())
			// m.WriteSlabHeader(slab)

			return written, nil
		}

	}

}
