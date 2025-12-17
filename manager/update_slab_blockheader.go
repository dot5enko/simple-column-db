package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
)

func (sm *SlabManager) UpdateBlockHeaderAndDataOnDisk(
	s schema.Schema,
	slab *schema.DiskSlabHeader,
	block *schema.RuntimeBlockData,
) error {

	foundIdx := -1
	for idx, it := range slab.BlockHeaders {
		if it.Uid == block.Header.Uid {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		return fmt.Errorf("block with uid `%s` doesn't exist in slab", block.Header.Uid.String())
	}

	buf := bits.NewEncodeBuffer(sm.SlabBlockHeadersReadBuffer[:], binary.LittleEndian)
	serializedBytes, headerBytesErr := block.Header.WriteTo(&buf)

	if headerBytesErr != nil {
		return fmt.Errorf("unable to serialize block header, header won't serialize : %s", headerBytesErr.Error())
	} else {

		singleBlockUncompressedSize := slab.Type.Size() * schema.BlockRowsSize
		blockDataOffset := singleBlockUncompressedSize * foundIdx

		headersHeaderOffset := schema.TotalHeaderSize * uint64(foundIdx)
		slabHeaderAbsOffset := schema.SlabHeaderFixedSize + headersHeaderOffset
		headersSize := schema.TotalHeaderSize * uint64(slab.BlocksTotal)

		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to get slab file : %s", slabErr.Error())
		}

		defer fileManager.Close()

		headerBlockUpdateErr := fileManager.WriteAt(sm.SlabBlockHeadersReadBuffer[:], int(slabHeaderAbsOffset), serializedBytes)
		if headerBlockUpdateErr != nil {
			return fmt.Errorf("unable to update block header : %s", headerBlockUpdateErr.Error())
		}

		writeBuf := bytes.NewBuffer(sm.BufferForCompressedData10Mb[:0])
		writeErr := io.DumpNumbersArrayBlockAny(writeBuf, block.DataTypedArray)
		if writeErr != nil {
			return fmt.Errorf("unable to finalize block : %s", writeErr.Error())
		}

		// update block content

		// writeBuf.Bytes()
		slab := sm.GetSlabFromCache(slab.Uid)

		copy(slab.data[blockDataOffset:], writeBuf.Bytes())

		return fileManager.WriteAt(slab.data[:], int(schema.SlabHeaderFixedSize+headersSize), schema.SlabDiskContentsUncompressed)
	}

	// return nil
}
