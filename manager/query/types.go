package query

import (
	"fmt"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

const ExecutorChunkSizeBlocks = 10

var (
	ErrSchemaNotFound = fmt.Errorf("schema not found")
)

type (
	Segment struct {
		Slab uuid.UUID

		StartBlock int
		Size       int
	}

	BlockChunk struct {
		GlobalBlockOffset uint64

		// for each field there will be an array of segments
		// thats why we need a "map" here, for speed we use numeric array instead
		// indices correspond to the order of fields in schema object
		ChunkSegmentsByFieldIndexMap [][]Segment
	}

	QueryPlan struct {
		Schema                schema.Schema
		FilterGroupedByFields []FilterGroupedRT
		BlockChunks           []BlockChunk

		FilterSize int
	}

	// chunk
	SingleChunk struct {
		Segments     []Segment
		BlocksFilled int
	}

	ColumnChunks struct {
		List []SingleChunk
	}

	QueryOptions struct {
	}

	Query struct {
		Filter []FilterCondition
		Select []Selector
	}
)
