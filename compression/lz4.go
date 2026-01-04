package compression

import (
	"github.com/pierrec/lz4/v4"
)

func CompressLz4(src []byte, output []byte) (int, error) {

	var c lz4.Compressor
	return c.CompressBlock(src, output)

}

func OutputBufferForData(inputSize int) int {
	return lz4.CompressBlockBound(inputSize)
}

func DecompressLz4(src []byte, output []byte) (int, error) {

	return lz4.UncompressBlock(src, output)

}
