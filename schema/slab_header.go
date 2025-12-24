package schema

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

const CurrentSlabVersion = 1
const SlabHeaderFixedSize = 2 + 8 + 16 + 2 + 2 + 2 + 1 + 1 + 1 + 8 + BoundsSize
const SlabDiskContentsUncompressed = 10 * 1024 * 1024

type DiskSlabHeader struct {
	Bounds BoundsFloat
	Uid    uuid.UUID

	CompressedSlabContentSize uint64
	SlabOffsetBlocks          uint64

	BlocksTotal         uint16
	BlocksFinalized     uint16
	SingleBlockRowsSize uint16
	Version             uint16

	SchemaFieldId   uint8
	CompressionType uint8
	Type            FieldType

	// up to this point we have a predictable layout
	BlockHeaders []DiskHeader
}

func NewDiskSlab(
	schemaObject Schema,
	fieldName string,
	slabOffsetBlocks uint64,
) (*DiskSlabHeader, error) {

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
	slabBlocks := columnDef.Type.BlocksPerSlab()
	uncompressedSize := int(slabBlocks) * columnDef.Type.BlockSize()

	color.Red(" --- new slab creation with offset blocks : %d", slabOffsetBlocks)

	uid, _ := uuid.NewV7()

	return &DiskSlabHeader{
		Version:             CurrentSlabVersion,
		SlabOffsetBlocks:    slabOffsetBlocks,
		Uid:                 uid,
		BlocksTotal:         uint16(slabBlocks),
		SingleBlockRowsSize: BlockRowsSize,
		SchemaFieldId:       uint8(selectedIdx) + 1,
		Type:                columnDef.Type,
		// block is new, so it's empt	y
		BlocksFinalized:           0,
		CompressionType:           0,
		CompressedSlabContentSize: uint64(uncompressedSize),
		Bounds:                    NewBounds(),
	}, nil
}

func (header *DiskSlabHeader) FromBytes(input io.ReadSeeker) (topErr error) {

	reader := bits.NewReader(input, binary.LittleEndian)

	header.Version = reader.MustReadU16()
	header.SlabOffsetBlocks = reader.MustReadU64()

	if header.Version != CurrentSlabVersion {
		return fmt.Errorf("invalid version (%d). Supported versions: %d ", header.Version, CurrentSlabVersion)
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
	// header.UncompressedSlabContentSize = reader.MustReadU64()
	header.CompressedSlabContentSize = reader.MustReadU64()

	header.Bounds.FromBytes(reader)

	return nil

}

func (header *DiskSlabHeader) WriteTo(buffer []byte) (int, error) {

	bw := bits.NewEncodeBuffer(buffer, binary.LittleEndian)

	// defer fmt.Printf(" >> wsh writing slab header %s : \n >> wsh %v\n", header.Uid.String(), buffer[:SlabHeaderFixedSize])

	// Write basic fields
	bw.PutUint16(header.Version)
	bw.PutUint64(header.SlabOffsetBlocks)

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
	// bw.PutUint64(header.UncompressedSlabContentSize)
	bw.PutUint64(header.CompressedSlabContentSize)

	header.Bounds.WriteTo(&bw)

	return bw.Position(), nil

}
