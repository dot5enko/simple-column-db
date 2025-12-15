package schema

import "github.com/google/uuid"

type SchemaColumn struct {
	Name string
	Type FieldType

	// runtime
	ActiveSlab uuid.UUID
	Slabs      []uuid.UUID
}
