package schema

type FieldType uint8

const (
	Int8FieldType FieldType = iota
	Int16FieldType
	Int32FieldType
	Int64FieldType

	Float64FieldType
	Float32FieldType

	Uint64FieldType
	Uint8FieldType
	Uint32FieldType
	Uint16FieldType
)

func (f FieldType) String() string {
	switch f {
	case Int8FieldType:
		return "Int8"
	case Int16FieldType:
		return "Int16"
	case Int32FieldType:
		return "Int32"
	case Int64FieldType:
		return "Int64"
	case Float64FieldType:
		return "Float64"
	case Float32FieldType:
		return "Float32"
	case Uint64FieldType:
		return "Uint64"
	case Uint8FieldType:
		return "Uint8"
	case Uint32FieldType:
		return "Uint32"
	case Uint16FieldType:
		return "Uint16"
	default:
		return ""

	}
}

func (f FieldType) Size() int {
	switch f {

	case Int8FieldType, Uint8FieldType:
		return 1
	case Int16FieldType, Uint16FieldType:
		return 2
	case Int32FieldType, Float32FieldType, Uint32FieldType:
		return 4
	case Int64FieldType, Float64FieldType, Uint64FieldType:
		return 8

	default:
		panic("unknown field type " + f.String())
	}
}

func (f FieldType) BlockSize() int {
	elementSize := f.Size()
	return elementSize * BlockRowsSize
}

func (f FieldType) BlocksPerSlab() int16 {
	blockSize := f.BlockSize()
	result := SlabDiskContentsUncompressed / blockSize
	if result > 32000 {
		return int16(32000)
	}
	return int16(result)
}
