package schema

type Schema struct {
	Name    string         `json:"name"`
	Columns []SchemaColumn `json:"columns"`
}
