package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/google/uuid"
)

const SlabBlocks = 32

// slab of blocks on disk

// *--------------------------------*
// | version        				|
// *--------------------------------*
// | slab meta						|
// *--------------------------------*
// | unfinished block header		|
// | unfinished block data			|
// *--------------------------------*
// | block headers 2 ... n 			|
// *--------------------------------*
// | compressed block data 2... n	|
// *--------------------------------*

const CurrentSlabVersion = 1

type DiskSlabHeader struct {
	Version uint16

	Uid uuid.UUID

	BlocksTotal     uint16
	BlocksFinalized uint16

	SingleBlockRowsSize uint16

	SchemaFieldId uint8
	Type          FieldType

	CompressionType uint8

	UnfinishedBlockHeader  DiskHeader
	CompressedBlockHeaders [SlabBlocks]DiskHeader

	// end of predictable layout

	// UnfinishedBlockData  []byte
	// BlocksCompressedData []byte
}

func NewDiskSlab(schemaObject Schema, fieldName string) *DiskSlabHeader {

	columnDef := schemaObject.Columns[fieldName]

	return &DiskSlabHeader{
		Version:             CurrentSlabVersion,
		Uid:                 uuid.New(),
		BlocksTotal:         SlabBlocks,
		SingleBlockRowsSize: BlockRowsSize,
		SchemaFieldId:       columnDef.Id,
		Type:                columnDef.Type,

		//  block is new, so it's empty
		BlocksFinalized: 0,
		CompressionType: 0,
	}
}

func (header *DiskSlabHeader) FromBytes(input []byte, cache []byte) (topErr error) {

	reader := bits.NewReader(bytes.NewBuffer(input), binary.LittleEndian)

	header.Version = reader.MustReadU16()

	if header.Version != CurrentSlabVersion {
		return fmt.Errorf("invalid version. Supported versions: %d ", CurrentSlabVersion)
	}

	var uuidErr error
	header.Uid, uuidErr = reader.ReadUUID()
	if topErr != nil {
		return uuidErr
	}

	header.BlocksTotal = reader.MustReadU16()
	header.BlocksFinalized = reader.MustReadU16()
	header.SingleBlockRowsSize = reader.MustReadU16()

	header.SchemaFieldId = reader.MustReadU8()
	header.Type = FieldType(reader.MustReadU8())

	header.CompressionType = reader.MustReadU8()

	reader.ReadBytes(int(TotalHeaderSize), cache)
	header.UnfinishedBlockHeader.FromBytes(cache[:TotalHeaderSize], nil)

	for i := 0; i < int(header.BlocksFinalized); i++ {
		reader.ReadBytes(int(TotalHeaderSize), cache)
		header.CompressedBlockHeaders[i].FromBytes(cache[:TotalHeaderSize], nil)
	}

	// uncompressedBlockEntriesSize := int(header.SingleBlockRowsSize) * header.Type.Size()
	// allocate here ?

	// uncompressedBlockValues := make([]byte, uncompressedBlockEntriesSize)

	return nil

}

func (header *DiskSlabHeader) WriteTo(buffer []byte) error {
	bw := bits.NewEncodeBuffer(1024, binary.LittleEndian)

	// Write basic fields
	bw.PutUint16(header.Version)
	bw.Write(header.Uid[:]) // UUID assumed to be [16]byte
	bw.PutUint16(header.BlocksTotal)
	bw.PutUint16(header.BlocksFinalized)
	bw.PutUint16(header.SingleBlockRowsSize)
	bw.WriteByte(header.SchemaFieldId)
	bw.WriteByte(uint8(header.Type))
	bw.WriteByte(header.CompressionType)

	// // Write unfinished block header
	// cache := make([]byte, TotalHeaderSize)
	// if err := header.UnfinishedBlockHeader.WriteTo(cache); err != nil {
	// 	return err
	// }
	// bw.Write(cache)

	// // Write finalized block headers
	// for i := 0; i < int(header.BlocksFinalized); i++ {
	// 	if err := header.CompressedBlockHeaders[i].WriteTo(cache); err != nil {
	// 		return err
	// 	}
	// 	bw.Write(cache)
	// }

	// // Flush to the writer
	// _, err := writer.Write(bw.Bytes())
	return nil
}
