package filters

import (
	"testing"

	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

func TestHeaderFullIntersectFilter(t *testing.T) {

	bounds := schema.NewBoundsFromValues(0.5, 0.8)

	filter := query.FilterCondition{
		Field:     "value",
		Operand:   query.GT,
		Arguments: []any{float32(0.4999)},
	}

	matchResult, matchErr := ProcessFilterOnBounds[float32](filter, &bounds)

	if matchErr != nil {
		t.Errorf("unexpected error %v", matchErr)
	} else if matchResult != schema.FullIntersection {
		t.Errorf("expected full intersection, got %s", matchResult.String())
	}

}
func TestHeaderNoIntersectFilter(t *testing.T) {

	bounds := schema.NewBoundsFromValues(0.5, 0.8)

	filter := query.FilterCondition{
		Field:     "value",
		Operand:   query.LT,
		Arguments: []any{float32(0.4999)},
	}

	matchResult, matchErr := ProcessFilterOnBounds[float32](filter, &bounds)

	if matchErr != nil {
		t.Errorf("unexpected error %v", matchErr)
	} else if matchResult != schema.NoIntersection {
		t.Errorf("expected no intersection")
	}

}

func TestHeaderPartialIntersectFilter(t *testing.T) {

	bounds := schema.NewBoundsFromValues(0.5, 0.8)

	filter := query.FilterCondition{
		Field:     "value",
		Operand:   query.LT,
		Arguments: []any{float32(0.5999)},
	}

	matchResult, matchErr := ProcessFilterOnBounds[float32](filter, &bounds)

	if matchErr != nil {
		t.Errorf("unexpected error %v", matchErr)
	} else if matchResult != schema.PartialIntersection {
		t.Errorf("expected partial intersection")
	}

}
