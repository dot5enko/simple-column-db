package block

import (
	"github.com/google/uuid"
)

const TotalHeaderSize uint64 = 128
const HeaderSizeUsed uint64 = 16 + 2 + 2 + 1 + 8 + 8 + 1
const ReservedSize uint64 = TotalHeaderSize - HeaderSizeUsed

type DiskHeader struct {

	// same schema columns share group uid
	GroupUid      uuid.UUID
	SchemaFieldId uint8

	Type uint16

	Values    uint16
	ValueSize uint8

	// uinon of MaxIValue and MinIValue or MaxFValue and MinFValue
	MaxIValue int64
	MinIValue int64

	MaxFValue float64
	MinFValue float64

	CompressionType uint8
	CompressedSize  uint64

	// uinion end

	// reserved for future use
	Reserved [ReservedSize]uint8
}
