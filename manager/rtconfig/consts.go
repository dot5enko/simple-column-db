package rtconfig

const SLAB_DISK_SIZE_MB = 10
const ROWS_PER_BLOCK = 32 * 1024
const BLOCKS_PER_CHUNK = 40

const ROWS_PER_CHUNK = ROWS_PER_BLOCK * BLOCKS_PER_CHUNK

// query executor
const QUERY_MAX_FILTERS_PER_FIELD = 16
const QUERY_MAX_SELECTORS = 32

// cache

const CACHE_STANDBY_SLABS = 16
const CACHE_PRECACHED_SLABS = 32
const CACHE_PRECACHED_SLAB_HEADERS = 32

func init() {

	if ROWS_PER_BLOCK%8 != 0 {
		panic("rows per block must be power of two")
	}

}
