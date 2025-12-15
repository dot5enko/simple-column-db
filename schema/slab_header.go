package schema

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/google/uuid"
)

const CurrentSlabVersion = 1

const SlabHeaderFixedSize = 2 + 16 + 2 + 2 + 2 + 1 + 1 + 1 + 8 + 8 + TotalHeaderSize

type DiskSlabHeader struct {
	Version uint16

	Uid uuid.UUID

	BlocksTotal     uint16
	BlocksFinalized uint16

	SingleBlockRowsSize uint16

	SchemaFieldId uint8
	Type          FieldType

	CompressionType             uint8
	UncompressedSlabContentSize uint64
	CompressedSlabContentSize   uint64

	UnfinishedBlockHeader DiskHeader

	// up to this point we have a predictable layout

	CompressedBlockHeaders []DiskHeader

	// UnfinishedBlockData  []byte
	// BlocksCompressedData []byte
}

func NewDiskSlab(schemaObject Schema, fieldName string) (*DiskSlabHeader, error) {

	var columnDef SchemaColumn
	selectedIdx := -1

	for idx, it := range schemaObject.Columns {
		if it.Name == fieldName {
			columnDef = it
			selectedIdx = idx
			break
		}
	}

	if selectedIdx == -1 {
		return nil, fmt.Errorf("column '%s' does not exist", fieldName)
	}

	// calc number of blocks so the slab size would be 2-6 MB when compressed with lz4
	uncompressedBlockSize := BlockRowsSize * columnDef.Type.Size()
	slabBlocks := 10 * 1024 * 1024 / uncompressedBlockSize

	if slabBlocks > 65000 {
		slabBlocks = 65000
	}

	log.Printf(" slab for %s will contain %d blocks", columnDef.Name, slabBlocks)

	return &DiskSlabHeader{
		Version:             CurrentSlabVersion,
		Uid:                 uuid.New(),
		BlocksTotal:         uint16(slabBlocks),
		SingleBlockRowsSize: BlockRowsSize,
		SchemaFieldId:       uint8(selectedIdx) + 1,
		Type:                columnDef.Type,

		//  block is new, so it's empty
		BlocksFinalized: 0,
		CompressionType: 0,
	}, nil
}

func (header *DiskSlabHeader) FromBytes(input io.Reader) (topErr error) {

	reader := bits.NewReader(input, binary.LittleEndian)

	header.Version = reader.MustReadU16()

	if header.Version != CurrentSlabVersion {
		return fmt.Errorf("invalid version. Supported versions: %d ", CurrentSlabVersion)
	}

	var uuidErr error
	header.Uid, uuidErr = reader.ReadUUID()
	if uuidErr != nil {
		return uuidErr
	}

	header.BlocksTotal = reader.MustReadU16()
	header.BlocksFinalized = reader.MustReadU16()
	header.SingleBlockRowsSize = reader.MustReadU16()

	header.SchemaFieldId = reader.MustReadU8()
	header.Type = FieldType(reader.MustReadU8())

	header.CompressionType = reader.MustReadU8()
	header.UncompressedSlabContentSize = reader.MustReadU64()
	header.CompressedSlabContentSize = reader.MustReadU64()

	header.UnfinishedBlockHeader.FromBytes(reader.Buffer())

	// for i := 0; i < int(header.BlocksFinalized); i++ {
	// 	reader.ReadBytes(int(TotalHeaderSize), cache)
	// 	header.CompressedBlockHeaders[i].FromBytes(cache[:TotalHeaderSize], nil)
	// }

	// uncompressedBlockEntriesSize := int(header.SingleBlockRowsSize) * header.Type.Size()
	// allocate here ?

	// uncompressedBlockValues := make([]byte, uncompressedBlockEntriesSize)

	return nil

}

func (header *DiskSlabHeader) WriteTo(buffer []byte) (int, error) {
	bw := bits.NewEncodeBuffer(buffer, binary.LittleEndian)

	// Write basic fields
	bw.PutUint16(header.Version)

	uuidLength := 16
	n, _ := bw.Write(header.Uid[:])
	if n != uuidLength {
		return 0, fmt.Errorf("failed to write UUID")
	}

	bw.PutUint16(header.BlocksTotal)
	bw.PutUint16(header.BlocksFinalized)
	bw.PutUint16(header.SingleBlockRowsSize)
	bw.WriteByte(header.SchemaFieldId)
	bw.WriteByte(uint8(header.Type))
	bw.WriteByte(header.CompressionType)

	// size the content of the slab before compression
	// preallocated on disk upon slab creation
	bw.PutUint64(header.UncompressedSlabContentSize)
	bw.PutUint64(header.CompressedSlabContentSize)

	// headersReservedSpace := (int(header.BlocksTotal) + 1) * int(TotalHeaderSize)
	// bw.EmptyBytes(headersReservedSpace)

	// // reserve space for block entries
	// bw.EmptyBytes(int(header.UncompressedSlabContentSize))

	return bw.Position(), nil

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
	return 0, nil
}
