package executortypes

import (
	"sync/atomic"
	"time"

	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type BlockRuntimeFilterCache struct {
	MatchResult schema.BoundsFilterMatchResult
	Bounds      schema.BoundsFloat
}

type historyEntry struct {
	Time     time.Time
	Action   int
	DataType schema.FieldType

	BlockIdx uint64
	Slab     uuid.UUID
	Thread   int
}

type BlockRuntimeInfo struct {
	BlockHeader *schema.DiskHeader

	HeaderFilterMatchResult [MaxFiltersPerField]BlockRuntimeFilterCache

	// rt debug
	changes      [64]historyEntry
	changesCount atomic.Int64
}

func (b *BlockRuntimeInfo) GetDebugHistory() []historyEntry {
	return b.changes[:b.changesCount.Load()]
}

func (b *BlockRuntimeInfo) SetRuntimeValue(rtv *schema.RuntimeBlockData, threadIdx int) {

	tnow := time.Now()
	actionIdx := b.changesCount.Add(1)
	changesRef := &b.changes[actionIdx]

	// b.Val = rtv
	panic("algo changed, SetRuntimeValue is no more used")

	action := 1
	if rtv == nil {
		action = 0
	} else {
		changesRef.DataType = rtv.Header.DataType
		changesRef.Slab = rtv.Slab
		changesRef.BlockIdx = rtv.BlockIndice
	}

	changesRef.Thread = threadIdx
	changesRef.Action = action
	changesRef.Time = tnow

}
