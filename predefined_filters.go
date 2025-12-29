package main

import (
	"time"

	"github.com/dot5enko/simple-column-db/manager/query"
)

func gen_test_filters() [][]query.FilterCondition {

	year := time.Hour * 24 * 30 * 12
	month := time.Hour * 24 * 30

	fromNowFilter := func(offset time.Duration, fieldName string) query.FilterCondition {

		tnow := time.Now()

		return query.FilterCondition{
			Field:     fieldName,
			Operand:   query.RANGE,
			Arguments: []any{uint64(tnow.Add(-offset).Unix()), uint64(tnow.Unix())},
		}
	}

	predefinedFilters := [][]query.FilterCondition{

		{
			fromNowFilter(year*4, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.RANGE,
				Arguments: []any{uint64(4), uint64(6)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.4999)},
			},
			// {
			// 	Field:     "value",
			// 	Operand:   query.LT,
			// 	Arguments: []any{float32(0.4999)},
			// },
		},

		{
			fromNowFilter(year*4, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(4)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.6999)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.8)},
			},
		},
		{
			fromNowFilter(year*4, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(4)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.6999)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.8)},
			},
		},
		{
			fromNowFilter(year*4, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(6)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.6499)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.7)},
			},
		},
		{
			fromNowFilter(year*4, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(5)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.6499)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.75)},
			},
		},

		{
			fromNowFilter(year, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(4)},
			},
			{
				Field:     "value",
				Operand:   query.GT,
				Arguments: []any{float32(0.75)},
			},
		},
		{
			fromNowFilter(year, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(4)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.75)},
			},
		},
		{
			fromNowFilter(year, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(5)},
			},
		},
		{
			fromNowFilter(month*8, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(6)},
			},
		},
		{
			fromNowFilter(month*32, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(7)},
			},
		},
		{
			fromNowFilter(month*24, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(2)},
			},
		},
		{
			fromNowFilter(month*24, "created_at"),
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(1)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.6)},
			},
		},
		{
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(1)},
			},
			{
				Field:     "value",
				Operand:   query.LT,
				Arguments: []any{float32(0.6)},
			},
		},
		{
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(6)},
			},
			{
				Field:     "value",
				Operand:   query.RANGE,
				Arguments: []any{float32(0.65), float32(0.75)},
			},
		},
		{
			{
				Field:     "monitor_id",
				Operand:   query.EQ,
				Arguments: []any{uint64(1)},
			},
		},
		{
			fromNowFilter(month*24, "created_at"),
		},
	}

	return predefinedFilters
}
