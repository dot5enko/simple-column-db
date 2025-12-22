package query

type SelectorType byte

const (
	SelectFunction SelectorType = iota
)

type Selector struct {
	Type      SelectorType
	Arguments []any

	Alias string
}
