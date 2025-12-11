package block

import (
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
