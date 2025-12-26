package filters

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/ops"
	"github.com/dot5enko/simple-column-db/schema"
)

func ProcessFilterOnBounds[T ops.NumericTypes](
	filter query.FilterCondition,
	bounds *schema.BoundsFloat,
) (matchResult schema.BoundsFilterMatchResult, err error) {

	switch filter.Operand {
	case query.RANGE:

		operandFrom := float64(filter.Arguments[0].(T))
		operandTo := float64(filter.Arguments[1].(T))

		if operandFrom > operandTo {
			temp := operandTo
			operandTo = operandFrom
			operandFrom = temp
		}

		matchResult = bounds.Intersects(schema.NewBoundsFromValues(operandFrom, operandTo))
		return matchResult, nil

	case query.EQ:

		operand := float64(filter.Arguments[0].(T))
		contains := bounds.Contains(operand)

		if !contains {
			return schema.NoIntersection, nil
		} else if contains {
			return schema.PartialIntersection, nil
		}

	case query.GT:

		operand := float64(filter.Arguments[0].(T))

		if operand > bounds.Max {
			return schema.NoIntersection, nil
		}

		if operand <= bounds.Min {
			return schema.FullIntersection, nil
		}

		return schema.PartialIntersection, nil

	case query.LT:

		operand := float64(filter.Arguments[0].(T))

		if operand < bounds.Min {
			return schema.NoIntersection, nil
		}

		if operand >= bounds.Max {
			return schema.FullIntersection, nil
		}

		return schema.PartialIntersection, nil

	default:
		return schema.UnknownIntersection, fmt.Errorf("unsupported operand type=%v while ProcessFilterOnBounds", filter.Operand)
	}

	return schema.UnknownIntersection, nil

}
