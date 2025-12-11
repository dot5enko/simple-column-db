package block

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

const TotalHeaderSize uint64 = 128
const HeaderSizeUsed uint64 = 16 + 8 + 8 + 8 + 8 + 1 // guid + start offset + compressed size + datatype + [max value + min value] (2xi64)
const ReservedSize uint64 = TotalHeaderSize - HeaderSizeUsed

type DiskHeader struct {
	GroupUid uuid.UUID

	StartOffset    uint64
	CompressedSize uint64

	DataType schema.FieldType

	// uinon of MaxIValue and MinIValue or MaxFValue and MinFValue
	MaxIValue int64
	MinIValue int64

	MaxFValue float64
	MinFValue float64

	// reserved for future use
	Reserved [ReservedSize]uint8
}

func (header *DiskHeader) FromBytes(input []byte) (topErr error) {

	reader := bits.NewReader(bytes.NewBuffer(input), binary.LittleEndian)

	header.GroupUid, topErr = reader.ReadUUID()
	if topErr != nil {
		return fmt.Errorf("unable to decode block header guid: %s", topErr.Error())
	}

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

	columnType := schema.FieldType(columnTypeRaw)

	// read max/min values
	switch columnType {
	case schema.Int8FieldType, schema.Int16FieldType, schema.Int32FieldType, schema.Int64FieldType:
		header.MaxIValue = reader.MustReadI64()
		header.MinIValue = reader.MustReadI64()
	case schema.Uint8FieldType, schema.Uint16FieldType, schema.Uint32FieldType, schema.Uint64FieldType:
		header.MaxIValue = int64(reader.MustReadU64())
		header.MinIValue = int64(reader.MustReadU64())
	case schema.Float32FieldType, schema.Float64FieldType:
		header.MaxFValue = reader.MustReadF64()
		header.MinFValue = reader.MustReadF64()
	default:
		panic(fmt.Sprintf("unsupported field type: %d", columnType.String()))
	}

	return nil

}
