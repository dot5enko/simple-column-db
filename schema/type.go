package schema

type FieldType = uint8

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
