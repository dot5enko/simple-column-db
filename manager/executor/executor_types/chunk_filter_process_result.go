package executortypes

import (
	"time"

	"github.com/dot5enko/simple-column-db/manager/rtconfig"
)

type ChunkFilterProcessResult struct {
	SkippedBlocksDueToHeaderFiltering int64
	ProcessedBlocks                   int64
	FullSkips                         int64

	TotalItems   int64
	WastedMerges int64

	LockTook int64
	PlanTook int64
	PureLock int64

	TookSelectors int64

	IoTime int64

	// set on query planner
	TotalChunks        int64
	TotalQueryDuration time.Duration

	SelectorBuffers [rtconfig.MAX_SELECTORS_PER_QUERY][]byte
}

type SingleColumnProcessingResult struct {
	SkippedBlocksDueToHeaderFiltering int
	ProcessedBlocks                   int
	FullSkips                         int

	MatchedItems int
}
