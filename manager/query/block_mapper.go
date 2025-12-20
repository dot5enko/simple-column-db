package query

import "github.com/dot5enko/simple-column-db/schema"

type BlockMapping struct {
	BlockOffset int
	IndexOffset int
}

func MapBlockIndex(
	groupIdxA int,
	groupIdxB int,
	blockIDA int,
	indexInBlockA int,
) BlockMapping {

	globalIndex := blockIDA*schema.BlockRowsSize + indexInBlockA

	blockIDB := globalIndex / schema.BlockRowsSize
	indexInBlockB := globalIndex % schema.BlockRowsSize

	return BlockMapping{
		BlockOffset: blockIDB - blockIDA,
		IndexOffset: indexInBlockB - indexInBlockA,
	}
}
