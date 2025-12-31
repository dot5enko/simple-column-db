package executortypes

import (
	"time"

	"github.com/dot5enko/simple-column-db/lists"
	"github.com/dot5enko/simple-column-db/manager/query"
	"github.com/dot5enko/simple-column-db/schema"
)

type BlockMergerContext struct {
	Schema         schema.Schema
	AbsOffsetStart uint64
	FilterColumn   []query.FilterConditionRuntime

	FilterColumnRuntimeCache []query.RuntimeFilterCache

	FilterSize int

	Blocks                    []BlockRuntimeInfo
	CurrentBlockProcessingIdx int

	AbsBlockMaps []lists.IndiceUnmerged

	QueryPlan *query.QueryPlan

	IoTime time.Duration
}
