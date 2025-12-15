package manager

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type CacheStats struct {
	Reads   int
	Created time.Time
}

type BlockCacheItem struct {
	header  *schema.DiskHeader
	runtime any // RuntimeBlockData

	rtStats *CacheStats
}

type SlabCacheItem struct {
	header *schema.DiskSlabHeader

	data []byte

	rtStats *CacheStats
}

type SlabManager struct {
	cache  map[[32]byte]BlockCacheItem
	locker sync.RWMutex

	slabCacheItem   map[uuid.UUID]SlabCacheItem
	slabCacheLocker sync.RWMutex

	SlabHeaderReaderBuffer     [schema.SlabHeaderFixedSize]byte
	SlabBlockHeadersReadBuffer [256 * schema.TotalHeaderSize]byte // max blocks per slab ? TODO: check

	BufferForCompressedData10Mb [10 * 1024 * 1024]byte // 10mb buffer for decompression
}

func (m *SlabManager) GetSlabFromCache(uid uuid.UUID) *SlabCacheItem {
	return m.getSlabFromCache(uid)
}
func (m *SlabManager) getSlabFromCache(uid uuid.UUID) *SlabCacheItem {
	m.slabCacheLocker.Lock()
	defer m.slabCacheLocker.Unlock()

	if item, ok := m.slabCacheItem[uid]; ok {

		item.rtStats.Reads++
		return &item
	}

	return nil
}

func (m *SlabManager) LoadSlabToCache(schemaObject schema.Schema, slabUid uuid.UUID, mm *Manager) (result *schema.DiskSlabHeader, e error) {

	before := time.Now()
	defer func() {
		loadTook := time.Since(before).Microseconds()
		log.Printf("slab load took %dus", loadTook)
	}()

	slabHeader := m.getSlabFromCache(slabUid)

	if slabHeader != nil {
		return slabHeader.header, nil
	} else {

		tn := time.Now()

		slabStoragePath := mm.GetSlabPath(schemaObject, slabUid)

		fileReader := io.NewFileReader(slabStoragePath)
		openErr := fileReader.OpenForReadOnly(true)
		if openErr != nil {
			e = openErr
		} else {

			headerReadErr := fileReader.ReadAt(m.SlabHeaderReaderBuffer[:], 0, int(schema.SlabHeaderFixedSize))

			if headerReadErr != nil {
				e = headerReadErr
				return
			} else {

				result = &schema.DiskSlabHeader{}

				headerParseErr := result.FromBytes(bytes.NewReader(m.SlabHeaderReaderBuffer[:]))
				if headerParseErr != nil {
					e = headerParseErr
					return
				} else {

					// read the rest of headers, and their content

					result.CompressedBlockHeaders = make([]schema.DiskHeader, result.BlocksFinalized)

					nonZeroHeadersSize := int(result.BlocksFinalized) * int(schema.TotalHeaderSize)
					headersReadErr := fileReader.ReadAt(m.SlabBlockHeadersReadBuffer[:], int(schema.SlabHeaderFixedSize), nonZeroHeadersSize)

					if headersReadErr != nil {
						e = headersReadErr
						return
					} else {
						for i := 0; i < len(result.CompressedBlockHeaders); i++ {
							headerBuffer := m.SlabBlockHeadersReadBuffer[i*int(schema.TotalHeaderSize):]
							headerDecodeErr := result.CompressedBlockHeaders[i].FromBytes(bytes.NewReader(headerBuffer))

							if headerDecodeErr != nil {
								e = headerDecodeErr
								return
							}
						}
					}

					// read compressed data
					dataOffset := int(schema.SlabHeaderFixedSize) + int(nonZeroHeadersSize)
					readCompressedDataErr := fileReader.ReadAt(m.BufferForCompressedData10Mb[:], dataOffset, int(result.CompressedSlabContentSize))

					dataBuffer := make([]byte, result.UncompressedSlabContentSize)

					if readCompressedDataErr != nil {
						e = readCompressedDataErr
						return
					} else {

						// decode compressed data here
						// todo. as now all the data are stored uncompressed, so just copy them

						copy(dataBuffer, m.BufferForCompressedData10Mb[:result.CompressedSlabContentSize])
					}

					item := SlabCacheItem{
						header:  result,
						rtStats: &CacheStats{Created: tn, Reads: 1},
						data:    dataBuffer,
					}

					m.slabCacheLocker.Lock()
					defer m.slabCacheLocker.Unlock()

					m.slabCacheItem[slabUid] = item

				}
			}

		}
	}

	return
}

func GetUniqueBlockId(slab, block uuid.UUID) [32]byte {

	uid := [32]byte{}

	copy(uid[0:], slab[:])
	copy(uid[16:], block[:])

	return uid
}

func (m *SlabManager) getBlockFromCache(slab, block uuid.UUID) *BlockCacheItem {

	m.slabCacheLocker.Lock()
	defer m.slabCacheLocker.Unlock()

	uid := GetUniqueBlockId(slab, block)

	if item, ok := m.cache[uid]; ok {

		item.rtStats.Reads++
		return &item
	}

	return nil
}

// load block from slab
func (m *SlabManager) LoadBlockToRuntimeBlockData(
	schemaObject schema.Schema,
	slab *schema.DiskSlabHeader,
	block uuid.UUID,
	mm *Manager,
) (any, error) {

	cached := m.getBlockFromCache(slab.Uid, block)

	if cached != nil {
		return cached.runtime, nil
	} else {
		// put into cache

		var blockHeader schema.DiskHeader
		blockIdx := -1

		for idx, it := range slab.CompressedBlockHeaders {
			if it.GroupUid == block {
				blockHeader = it
				blockIdx = idx
				break
			}
		}

		if blockIdx < 0 {
			return nil, errors.New("block not found")
		} else {

			// blockItemSize := blockHeader.DataType.Size()
			// blockSize := blockItemSize * int(schema.BlockRowsSize)

			slabCache := m.getSlabFromCache(slab.Uid)
			if slabCache == nil {
				_, loadSlabErr := m.LoadSlabToCache(schemaObject, slab.Uid, mm)
				if loadSlabErr != nil {
					return nil, loadSlabErr
				}
				slabCache = m.getSlabFromCache(slab.Uid)
				if slabCache == nil {
					panic("cache should be loaded by now, probably out of memory?")
				}
			}

			blockRawData := slabCache.data[blockHeader.StartOffset:]

			runtimeBlockData, runtimeDecodeErr := DecodeRawBlockData(blockRawData, blockHeader)

			if runtimeDecodeErr != nil {
				return nil, runtimeDecodeErr
			} else {
				m.locker.Lock()
				defer m.locker.Unlock()

				blockId := GetUniqueBlockId(slab.Uid, block)

				m.cache[blockId] = BlockCacheItem{
					header:  &blockHeader,
					runtime: runtimeBlockData,
					rtStats: &CacheStats{Created: time.Now(), Reads: 1},
				}

				return runtimeBlockData, nil
			}

		}

	}

}

func DecodeRawBlockData(blockData []byte, bheader schema.DiskHeader) (any, error) {

	switch bheader.DataType {
	// case schema.Int8FieldType:
	// 	return nil, errors.New("not implemented")
	// case schema.Int16FieldType:
	// 	return nil, errors.New("not implemented")
	// case schema.Int32FieldType:
	// 	return nil, errors.New("not implemented")
	// case schema.Int64FieldType:
	// 	return nil, errors.New("not implemented")
	case schema.Float64FieldType:
		result := bits.MapBytesToArray[float64](blockData, schema.BlockRowsSize)
		runtimeData := schema.NewRuntimeBlockDataFromSlice(result)
		runtimeData.Header = bheader

		return runtimeData, nil
	case schema.Float32FieldType:
		result := bits.MapBytesToArray[float32](blockData, schema.BlockRowsSize)
		runtimeData := schema.NewRuntimeBlockDataFromSlice(result)
		runtimeData.Header = bheader

		return runtimeData, nil
	case schema.Uint64FieldType:

		result := bits.MapBytesToArray[uint64](blockData, schema.BlockRowsSize)
		runtimeData := schema.NewRuntimeBlockDataFromSlice(result)
		runtimeData.Header = bheader

		return runtimeData, nil
	// case schema.Uint8FieldType:
	// 	return nil, errors.New("not implemented")
	// case schema.Uint32FieldType:
	// 	return nil, errors.New("not implemented")
	// case schema.Uint16FieldType:
	// 	return nil, errors.New("not implemented")
	default:
		return nil, fmt.Errorf("unknown type while decoding raw block data: %s", bheader.DataType.String())
	}

}
