package perf

import (
	"log"
	"log/slog"
	"runtime"
)

func AllocsDetection() func() {
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	it0 := mstats

	return func() {

		var mstats2 runtime.MemStats

		runtime.ReadMemStats(&mstats2)

		it := mstats2

		mallocs := it.Mallocs - it0.Mallocs
		// frees := it.Frees - it0.Frees

		bytesSize := 0

		if mallocs > 0 {

			log.Printf(" ===== %d Mallocs", mallocs)

			for i := range it.BySize { // AllocObjects is the number of allocated objects.
				has := it.BySize[i].Mallocs - it0.BySize[i].Mallocs
				if has > 0 {
					bytesSize = int(it.BySize[i].Size)
					slog.Info(" \t+++ mem stats", "alloc_size", bytesSize, "count", has)
				}
			}

		}
	}
}
