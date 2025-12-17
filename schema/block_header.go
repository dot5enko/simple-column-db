package schema

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/google/uuid"
)

const BlockRowsSize = 32 * 1024 // 32k rows per block

const TotalHeaderSize = 128

const HeaderSizeUsed uint64 = 16 + 2 + 8 + 8 + 1 + 16 // guid + start offset + compressed size + datatype + [max value + min value] bounds : 16
const ReservedSize uint64 = TotalHeaderSize - HeaderSizeUsed

type DiskHeader struct {
	Uid uuid.UUID

	Items uint16

	StartOffset    uint64
	CompressedSize uint64

	DataType FieldType
	Bounds   BoundsFloat

	// reserved for future use
	Reserved [ReservedSize]uint8
}

func NewBlockHeader(typ FieldType) *DiskHeader {
	return &DiskHeader{
		Uid:      uuid.New(),
		DataType: typ,
		Items:    0,
	}
}

func (header *DiskHeader) FromBytes(input io.ReadSeeker) (topErr error) {

	reader := bits.NewReader(input, binary.LittleEndian)

	header.Uid, topErr = reader.ReadUUID()
	if topErr != nil {
		return fmt.Errorf("unable to decode block header guid: %s", topErr.Error())
	}
	header.Items = reader.MustReadU16()

	header.StartOffset, topErr = reader.ReadU64()
	if topErr != nil {
		return fmt.Errorf("unable to decode block header start offset: %s", topErr.Error())
	}
	header.CompressedSize, topErr = reader.ReadU64()
	if topErr != nil {
		return fmt.Errorf("unable to decode block header compressed size: %s", topErr.Error())
	}

	columnTypeRaw, topErr := reader.ReadU8()
	if topErr != nil {
		return fmt.Errorf("unable to decode block header column type: %s", topErr.Error())
	}

	columnType := FieldType(columnTypeRaw)
	header.DataType = columnType

	// read max/min values
	header.Bounds.FromBytes(reader)

	return nil

}

func (header *DiskHeader) WriteTo(bw *bits.BitWriter) (int, error) {

	// UUID
	n, _ := bw.Write(header.Uid[:])
	if n != 16 {
		return 0, fmt.Errorf("failed to write GroupUid")
	}

	// Items
	bw.PutUint16(header.Items)

	// Offsets and sizes
	bw.PutUint64(header.StartOffset)
	bw.PutUint64(header.CompressedSize)

	// Column type
	bw.WriteByte(uint8(header.DataType))

	// bounds
	header.Bounds.WriteTo(bw)

	bw.EmptyBytes(int(ReservedSize))

	return bw.Position(), nil
}
