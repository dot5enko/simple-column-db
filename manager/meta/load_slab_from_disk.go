package meta

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dot5enko/simple-column-db/compression"
	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

// reading should be thread safe
// alloc free
func (m *SlabManager) LoadSlabHeaderToCache(schemaObject *schema.Schema, slabUid uuid.UUID) (result *schema.DiskSlabHeader, e error) {

	slabHeader := m.getSlabHeaderFromCache(slabUid)

	if slabHeader != nil {
		return slabHeader.Header, nil
	} else {

		v, err, _ := m.rt.loadGroup.Do(slabUid.String(), func() (any, error) {

			slabReadCache, slabCacheIdx := m.buffers.fullSlabBufferRing.Get()
			headerReadBuffer, headerBufferIdx := m.buffers.headerReaderBufferRing.Get()

			// no need to block this resources for whole duration of func
			// todo optimize
			defer func() {
				m.buffers.fullSlabBufferRing.Return(slabCacheIdx)
				m.buffers.headerReaderBufferRing.Return(headerBufferIdx)
			}()

			// slog.Info("loading slab to cache from disk", "slab_uid", slabUid.String())

			fileReader, openErr := m.GetSlabFile(*schemaObject, slabUid, false)
			fileReader.SetPerfStats(m.GetSession())

			if openErr != nil {
				return nil, openErr
			} else {
				defer fileReader.Close()

				headerReadErr := fileReader.ReadAt(headerReadBuffer, 0, int(schema.SlabHeaderFixedSize))

				if headerReadErr != nil {
					return nil, fmt.Errorf("unable to read slab header : %s", headerReadErr.Error())
				} else {

					// ioTime := time.Since(readStart).Seconds()

					var headerCacheEntryId uint16
					result = m.buffers.slabHeaderCache.Get()

					headerBytes := bytes.NewReader(headerReadBuffer)
					headerParseErr := result.FromBytes(headerBytes)
					if headerParseErr != nil {
						return nil, headerParseErr
					} else {

						// read the rest of headers, and their content
						// todo use preallocated buffer
						result.BlockHeaders = make([]schema.DiskHeader, result.BlocksTotal)

						// allBlocksHeaderSize := int(result.BlocksTotal) * int(schema.TotalHeaderSize)
						nonEmptyHeadersSize := int(result.BlocksFinalized) * int(schema.TotalHeaderSize) // finalized + current

						if result.BlocksFinalized < result.BlocksTotal {
							nonEmptyHeadersSize += int(schema.TotalHeaderSize)
						}

						// we use here slab read cache to save resources
						headersReadErr := fileReader.ReadAt(slabReadCache, int(schema.SlabHeaderFixedSize), nonEmptyHeadersSize)

						if headersReadErr != nil {
							return nil, fmt.Errorf("unable to read data while LoadSlabToCache: %s", headersReadErr.Error())
						} else {

							blocksToIterate := int(result.BlocksFinalized) + 1
							if blocksToIterate >= int(result.BlocksTotal) {
								blocksToIterate = int(result.BlocksTotal)
							}

							for i := 0; i < blocksToIterate; i++ {
								blockOffset := i * int(schema.TotalHeaderSize)
								headerBuffer := slabReadCache[blockOffset:]

								headerDecodeErr := result.BlockHeaders[i].FromBytes(bytes.NewReader(headerBuffer))

								if headerDecodeErr != nil {
									return nil, headerDecodeErr
								}
							}
						}

					}

					m.rt.slabHeaderCacheLocker.Lock()
					defer m.rt.slabHeaderCacheLocker.Unlock()

					m.rt.slabHeaderCacheItem[slabUid] = &cache.SlabCacheItem{
						CacheEntryId: headerCacheEntryId,
						Header:       result,
						RtStats:      &cache.CacheStats{
							// Created: time.Now()
						},
					}

					return result, nil

				}
			}
		})

		if err != nil {
			return nil, err
		}

		return v.(*schema.DiskSlabHeader), nil

	}

}

func (m *SlabManager) invalidateCache(slabUid uuid.UUID, item2 *cache.SlabDataCacheItem) {

	rt := m.rt
	buffers := m.buffers

	rt.slabDataCacheLocker.Lock()
	defer rt.slabDataCacheLocker.Unlock()

	rt.locker.Lock()
	defer rt.locker.Unlock()

	keysToRemove := [][32]byte{}

	for key, _ := range rt.cache {
		slabId, slabIdErr := uuid.FromBytes(key[:16])
		if slabIdErr == nil && slabId == slabUid {
			keysToRemove = append(keysToRemove, key)
		}
	}

	for _, it := range keysToRemove {

		delete(rt.cache, it)
	}

	delete(rt.slabDataCache, slabUid)
	buffers.slabRuntimeCache.Return(item2)
}

var decodings atomic.Int32

var SimulateCacheInvalidation = false

func (m *SlabManager) LoadSlabDataContents(schemaObject *schema.Schema, uid uuid.UUID) (*cache.SlabDataCacheItem, error) {

	var result *schema.DiskSlabHeader

	slabData := m.getSlabDataFromCache(uid)
	if slabData != nil {
		return slabData, nil
	}

	var headerLoadErr error
	result, headerLoadErr = m.LoadSlabHeaderToCache(schemaObject, uid)
	if headerLoadErr != nil {
		return nil, headerLoadErr
	}

	// fix key construction, do not use allocations
	key := "d-" + uid.String()

	v, err, _ := m.rt.loadGroup.Do(key, func() (any, error) {

		// read compressed data
		allBlocksHeaderSize := int(result.BlocksTotal) * int(schema.TotalHeaderSize)
		dataOffset := int(schema.SlabHeaderFixedSize) + allBlocksHeaderSize

		fileReader, openErr := m.GetSlabFile(*schemaObject, uid, false)
		if openErr != nil {
			return nil, openErr
		}

		statSession := m.GetSession()
		fileReader.SetPerfStats(statSession)

		defer fileReader.Close()
		item := m.buffers.slabRuntimeCache.Get()

		// todo improve this part
		// should be done on .Get inside RingBuffer
		item.Reset()

		// at this point we need to lock slab's data for reading
		// as it may be compressed
		readCompressedDataErr := fileReader.ReadAt(item.Data[:], dataOffset, int(result.CompressedSlabContentSize))

		if readCompressedDataErr != nil {
			return nil, readCompressedDataErr
		} else {

			fieldName := schemaObject.Columns[result.SchemaFieldId-1].Name

			var decompressFunc func(src []byte, output []byte) (int, error) = compression.DecompressSnappy
			var compressFunc func(input, output []byte) (int, error) = compression.CompressSnappy

			if result.CompressionType != 0 {

				// panic("compression not implemented while LoadSlabDataContents")
				switch result.CompressionType {
				case 1:

					item2 := m.buffers.slabRuntimeCache.Get()
					item2.Reset()

					dStart := time.Now()
					_, decompressErr := decompressFunc(item.Data[:result.CompressedSlabContentSize], item2.Data[:])
					if decompressErr != nil {
						spew.Dump("input buffers to decompress ", item.Data[:256], decompressErr.Error())
						return nil, fmt.Errorf("unable to decompress slab data [input length %d, outputd buffer: %d]: %s", result.CompressedSlabContentSize, len(item.Data[:]), decompressErr.Error())
					}

					decTook := time.Since(dStart).Seconds()
					mbSize := float64(result.CompressedSlabContentSize) / 1024.0 / 1024.0

					decodedIs := decodings.Add(1)

					color.Blue(" slab (%.2fMB) decompress took %.2fms %d. IO : %.2f", mbSize, decTook*1000, decodedIs, statSession.IoTime.Seconds()*1000)

					// _ = mbSize
					// _ = decTook

					m.rt.slabDataCacheLocker.Lock()
					defer m.rt.slabDataCacheLocker.Unlock()

					m.rt.slabDataCache[uid] = item2

					m.buffers.slabRuntimeCache.Return(item)

					if SimulateCacheInvalidation {
						time.AfterFunc(time.Millisecond*60, func() {
							m.invalidateCache(uid, item2)
						})
					}

					return item2, nil

				default:
					return nil, fmt.Errorf("unsupported compression type: %d", result.CompressionType)
				}

			} else if false {
				// compress test

				bufferSizeExpected := int(float64(result.CompressedSlabContentSize) * 1.5)
				outputBuffer := make([]byte, bufferSizeExpected)

				compressedSize, compressErr := compressFunc(item.Data[:result.CompressedSlabContentSize], outputBuffer)

				if compressErr != nil {
					color.Red("compress error: %s", compressErr.Error())
				} else {

					curSize := float64(compressedSize) / float64(result.CompressedSlabContentSize)

					if curSize > 0.5 {
						color.Red(" - ~~~ compressed neglectable [%s] FROM %d -> %d [%.2f%%]", fieldName, result.CompressedSlabContentSize, compressedSize, curSize*100.0)
					} else {
						color.Green(" - ~~~ compressed [%s] FROM %d -> %d [%.2f%%]", fieldName, result.CompressedSlabContentSize, compressedSize, curSize*100.0)

						if true {
							fileReader1, updateSlabContentFileOpenErr := m.GetSlabFile(*schemaObject, uid, true)
							if updateSlabContentFileOpenErr != nil {
								color.Yellow("unable to open file for slab compression: %s", updateSlabContentFileOpenErr.Error())
							}
							defer fileReader1.Close()

							result.CompressedSlabContentSize = uint64(compressedSize)
							result.CompressionType = 1

							headerUpdateErr := m.UpdateSlabHeaderOnDisk(*schemaObject, result)

							if headerUpdateErr != nil {
								color.Yellow(" - xxx unable to update compressed header: %s", headerUpdateErr.Error())
								return nil, headerUpdateErr
							} else {
								color.Cyan(" - ~~~ updated slab header. now the size is %d", result.CompressedSlabContentSize)
							}

							blocksInSlab := result.Type.BlocksPerSlab()
							headersHeaderOffset := schema.TotalHeaderSize * uint64(blocksInSlab)
							slabContentOffset := schema.SlabHeaderFixedSize + headersHeaderOffset

							slabCompressionErr := fileReader1.WriteAt(outputBuffer, int(slabContentOffset), int(result.CompressedSlabContentSize))
							if slabCompressionErr != nil {
								color.Yellow(" - xxx unable to write compressed data: %s", slabCompressionErr.Error())
								return nil, slabCompressionErr
							} else {
								color.Cyan(" - ~~~ updated slab header. now the size is %d", result.CompressedSlabContentSize)
							}
						}

					}
				}
			}

			m.rt.slabDataCacheLocker.Lock()
			defer m.rt.slabDataCacheLocker.Unlock()

			m.rt.slabDataCache[uid] = item

			if SimulateCacheInvalidation {
				time.AfterFunc(time.Millisecond*60, func() {
					m.invalidateCache(uid, item)
				})
			}

			return item, nil
		}
	})

	if err != nil {
		return nil, err
	}

	return v.(*cache.SlabDataCacheItem), nil

}
