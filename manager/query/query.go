package query

type Query struct {
	Filter []FilterCondition
	Select []Selector
}
