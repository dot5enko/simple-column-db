package block

const SlabBlocks = 128

// slab of blocks on disk

// *--------------------------------*
// | version        				|
// *--------------------------------*
// | slab meta						|
// *--------------------------------*
// | unfinished block header		|
// | unfinished block data			|
// *--------------------------------*
// | block headers 1 ... n 			|
// *--------------------------------*
// | compressed block data 1... n	|
// *--------------------------------*

type DiskBlockSlab struct {
	Version uint16

	BlocksTotal     uint16
	BlocksFinalized uint16

	SchemaFieldId uint8
	Type          uint16

	CompressionType uint8

	// unfinished block header and data, always uncompressed
	UnfinishedBlockHeader DiskHeader
	UnfinishedBlockData   []byte

	Headers [SlabBlocks]DiskHeader

	// blocks compressed data
	BlocksCompressedData []byte
}
