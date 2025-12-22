package query

import "fmt"

type FilterCondition struct {
	Field     string
	Operand   CondOperand
	Arguments []any
}

func (fc FilterCondition) ArgumentFloatValue(idx int) float64 {

	arg := fc.Arguments[idx]

	switch v := arg.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case uint64:
		return float64(v)
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case float32:
		return float64(v)
	default:
		panic(fmt.Sprintf("filter cond argument is not numeric: %T", arg))
	}
}
