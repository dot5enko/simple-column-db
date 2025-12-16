package schema

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/google/uuid"
)

const BlockRowsSize = 32 * 1024 // 32k rows per block

const TotalHeaderSize uint64 = 128
const HeaderSizeUsed uint64 = 16 + 2 + 8 + 8 + 8 + 8 + 1 // guid + start offset + compressed size + datatype + [max value + min value] (2xi64)
const ReservedSize uint64 = TotalHeaderSize - HeaderSizeUsed

type DiskHeader struct {
	Uid uuid.UUID

	Items uint16

	StartOffset    uint64
	CompressedSize uint64

	DataType FieldType

	// uinon of MaxIValue and MinIValue or MaxFValue and MinFValue
	MaxIValue int64
	MinIValue int64

	MaxFValue float64
	MinFValue float64

	// reserved for future use
	Reserved [ReservedSize]uint8
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

	// read max/min values
	switch columnType {
	case Int8FieldType, Int16FieldType, Int32FieldType, Int64FieldType:
		header.MaxIValue = reader.MustReadI64()
		header.MinIValue = reader.MustReadI64()
	case Uint8FieldType, Uint16FieldType, Uint32FieldType, Uint64FieldType:
		header.MaxIValue = int64(reader.MustReadU64())
		header.MinIValue = int64(reader.MustReadU64())
	case Float32FieldType, Float64FieldType:
		header.MaxFValue = reader.MustReadF64()
		header.MinFValue = reader.MustReadF64()
	default:
		panic(fmt.Sprintf("unsupported field type: %s", columnType.String()))
	}

	return nil

}

func (header *DiskHeader) WriteTo(buffer []byte) (int, error) {
	bw := bits.NewEncodeBuffer(buffer, binary.LittleEndian)

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

	// Min / Max values depending on type
	switch header.DataType {
	case Int8FieldType, Int16FieldType, Int32FieldType, Int64FieldType:
		bw.PutInt64(header.MaxIValue)
		bw.PutInt64(header.MinIValue)

	case Uint8FieldType, Uint16FieldType, Uint32FieldType, Uint64FieldType:
		bw.PutUint64(uint64(header.MaxIValue))
		bw.PutUint64(uint64(header.MinIValue))

	case Float32FieldType, Float64FieldType:
		bw.PutFloat64(header.MaxFValue)
		bw.PutFloat64(header.MinFValue)

	default:
		return 0, fmt.Errorf("unsupported field type: %s", header.DataType.String())
	}

	bw.EmptyBytes(int(ReservedSize))

	return bw.Position(), nil
}
