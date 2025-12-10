package schema

import "github.com/google/uuid"

type Schema struct {
	Name string `json:"name"`
	Uid  string `json:"uuid"`

	Columns map[string]SchemaColumn

	Blocks []uuid.UUID
}
