package schema

type Schema struct {
	Name string `json:"name"`
	Uid  string `json:"uuid"`

	// runtime
	Rows    int
	Columns []SchemaColumn
}
