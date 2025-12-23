package meta

import (
	"encoding/binary"
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

func (m *SlabManager) NewSlabForColumn(schemaConfig schema.Schema, col schema.SchemaColumn, slabOffsetBlocks uint64) (*schema.DiskSlabHeader, error) {

	slabHeader, slabError := schema.NewDiskSlab(schemaConfig, col.Name, slabOffsetBlocks)
	if slabError != nil {
		return nil, slabError
	}

	preallocateErr := m.preallocateSlab(schemaConfig, slabHeader.Uid)
	if preallocateErr != nil {
		return nil, fmt.Errorf("unable to preallocate slab : %s", preallocateErr.Error())
	}

	slabHeaderWriteErr := m.UpdateSlabHeaderOnDisk(schemaConfig, slabHeader)
	if slabHeaderWriteErr != nil {
		return nil, slabHeaderWriteErr
	}

	f, slabFileErr := m.GetSlabFile(schemaConfig, slabHeader.Uid, true)

	if slabFileErr != nil {
		return nil, fmt.Errorf("unable to open slab file : %s", slabFileErr.Error())
	}

	// crete first block
	firstBlock := schema.NewBlockHeader(col.Type)
	headerWriter := bits.NewEncodeBuffer(m.SlabBlockHeadersReadBuffer[:], binary.LittleEndian)
	writtenBytes, writeErr := firstBlock.WriteTo(&headerWriter)
	if writeErr != nil {
		return nil, fmt.Errorf("unable to encode block header : %s", writeErr.Error())
	}

	writeToDiskErr := f.WriteAt(m.SlabBlockHeadersReadBuffer[:writtenBytes], schema.SlabHeaderFixedSize, writtenBytes)
	if writeToDiskErr != nil {
		return nil, fmt.Errorf("unable to write block header into slab : %s", writeToDiskErr.Error())
	}

	// headers for blocks inside
	headersReservedSpace := int(slabHeader.BlocksTotal-1) * int(schema.TotalHeaderSize)
	reservedSize := int(slabHeader.SingleBlockRowsSize) * int(slabHeader.BlocksTotal) * slabHeader.Type.Size()

	totalZeroSize := headersReservedSpace + reservedSize

	zeroesFilledErr := f.FillZeroes(schema.SlabHeaderFixedSize+schema.TotalHeaderSize, totalZeroSize)

	color.Green(" +++ created new slab with id %v, size %d bytes, type = %s, field = %s", slabHeader.Uid.String(), slabHeader.CompressedSlabContentSize, slabHeader.Type.String(), schemaConfig.Columns[slabHeader.SchemaFieldId-1].Name)

	if zeroesFilledErr != nil {
		return nil, zeroesFilledErr
	}

	return slabHeader, nil

}
