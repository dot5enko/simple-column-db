package schema

import "github.com/google/uuid"

type SchemaColumn struct {
	Name string    `json:"name"`
	Type FieldType `json:"type"`

	// runtime
	ActiveSlab uuid.UUID   `json:"active_slab"`
	Slabs      []uuid.UUID `json:"slabs"`
}
