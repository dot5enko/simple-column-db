package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/compression"
	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
)

func (sm *SlabManager) TrimFinalizedBlocksSize(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
) error {

	if slab.BlocksFinalized < slab.BlocksTotal {
		return nil
	}

	headersSize := slab.BlocksTotal * schema.TotalHeaderSize

	fileManager, slabErr := sm.GetSlabFile(schemaObject, slab.Uid, true)
	if slabErr != nil {
		return fmt.Errorf("unable to get slab file : %s", slabErr.Error())
	}

	defer fileManager.Close()

	finalSize := int64(schema.SlabHeaderFixedSize+headersSize) + int64(slab.CompressedSlabContentSize)

	log.Printf(" >> trimmed slab %s to %d bytes [compressed data : %d]", slab.Uid.String(), finalSize, slab.CompressedSlabContentSize)

	return fileManager.Raw().Truncate(finalSize)
}

// todo work on thread safety
func (sm *SlabManager) UpdateBlockHeaderAndDataOnDisk(
	s schema.Schema,
	slab *schema.DiskSlabHeader,
	block *schema.RuntimeBlockData,
) error {

	foundIdx := -1
	for idx, it := range slab.BlockHeaders {
		if it.Uid == block.Header.Uid {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		return fmt.Errorf("block with uid `%s` doesn't exist in slab", block.Header.Uid.String())
	}

	slabReadCache, slabCacheIdx := sm.fullSlabBufferRing.Get()
	defer sm.fullSlabBufferRing.Return(slabCacheIdx)

	{
		singleBlockUncompressedSize := slab.Type.BlockSize()
		blockDataOffset := singleBlockUncompressedSize * foundIdx

		headersHeaderOffset := schema.TotalHeaderSize * uint64(foundIdx)
		slabHeaderAbsOffset := schema.SlabHeaderFixedSize + headersHeaderOffset
		headersSize := schema.TotalHeaderSize * int(slab.BlocksTotal)

		writeBuf := bytes.NewBuffer(slabReadCache[:0])
		dataSize, writeErr := io.DumpNumbersArrayBlockAny(writeBuf, block.DataTypedArray)
		if writeErr != nil {
			return fmt.Errorf("unable to finalize block : %s", writeErr.Error())
		}

		slabCacheItem := sm.GetSlabFromCache(slab.Uid)
		if slabCacheItem == nil {
			return fmt.Errorf("unable to find slab cache item, need to load whole slab from disk first")
		}

		copy(slabCacheItem.Data[blockDataOffset:], writeBuf.Bytes())

		// compress whole slab

		compressionSizeTotal := dataSize * int(slabCacheItem.Header.BlocksTotal)

		if false {

			start := time.Now()

			compressedSize, compressErr := compression.CompressLz4(slabCacheItem.Data[:compressionSizeTotal], slabReadCache)
			if compressedSize > 0 {
				compressionTook := time.Since(start)

				showSize := 128

				spew.Dump("compression input ", slabCacheItem.Data[:showSize], slabCacheItem.Data[compressionSizeTotal-showSize:compressionSizeTotal])

				if compressErr != nil {
					return fmt.Errorf("unable to compress slab data : %s", compressErr.Error())
				}

				// log.Printf(" input : %d -> output %d", dataSize*int(slab.BlocksFinalized+1), compressedSize)

				compressRatio := float64(compressedSize) / float64(compressionSizeTotal)
				fillRatio := float64(slab.BlocksFinalized) / float64(slab.BlocksTotal)

				color.Yellow(" compressed slab [type=%s][%d/%d] %d -> %d [%.2f%%] fill %.2f%% %.2fms", slab.Type.String(), slab.BlocksFinalized, slab.BlocksTotal, compressionSizeTotal, compressedSize, compressRatio*100.0, fillRatio*100, compressionTook.Seconds()*1000)

				spew.Dump("compressed data", slabReadCache[:showSize], slabReadCache[compressedSize-showSize:compressedSize])

				slab.CompressedSlabContentSize = uint64(compressedSize)
				slab.CompressionType = 1
			} else {
				slab.CompressedSlabContentSize = uint64(compressionSizeTotal)
			}
		} else {
			slab.CompressedSlabContentSize = uint64(compressionSizeTotal)
		}

		// header update
		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to get slab file : %s", slabErr.Error())
		}

		defer fileManager.Close()

		buf := bits.NewEncodeBuffer(sm.SlabBlockHeadersReadBuffer[:], binary.LittleEndian)
		serializedBytes, headerBytesErr := block.Header.WriteTo(&buf)

		// log.Printf("%s block bounds written : min %.2f. items in block : %d", block.BlockHeader.Uid.String(), block.BlockHeader.Bounds.Min, block.BlockHeader.Items)

		if headerBytesErr != nil {
			return fmt.Errorf("unable to serialize block header, header won't serialize : %s", headerBytesErr.Error())
		} else {
			headerBlockUpdateErr := fileManager.WriteAt(sm.SlabBlockHeadersReadBuffer[:], int(slabHeaderAbsOffset), serializedBytes)
			if headerBlockUpdateErr != nil {
				return fmt.Errorf("unable to update block header : %s", headerBlockUpdateErr.Error())
			}
		}

		var writeDataErr error

		if slab.CompressionType != 0 {
			writeDataErr = fileManager.WriteAt(slabReadCache[:slab.CompressedSlabContentSize], int(schema.SlabHeaderFixedSize+headersSize), int(slab.CompressedSlabContentSize))
		} else {
			writeDataErr = fileManager.WriteAt(slabCacheItem.Data[:], schema.SlabHeaderFixedSize+headersSize, int(slab.CompressedSlabContentSize))
		}

		if writeDataErr != nil {
			return fmt.Errorf("unable to update block data : %s", writeDataErr.Error())
		}

		return nil
	}

	// return nil
}
