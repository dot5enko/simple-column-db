package query

import "fmt"

type CondOperand byte

const (
	EQ CondOperand = iota
	GT
	LT
	RANGE
)

func (c CondOperand) String() string {
	switch c {
	case EQ:
		return "EQ"
	case GT:
		return "GT"
	case LT:
		return "LT"
	case RANGE:
		return "RANGE"
	default:
		panic(fmt.Sprintf("unknown operand %v", c))
	}
}
