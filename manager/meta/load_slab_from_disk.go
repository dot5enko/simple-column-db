package meta

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dot5enko/simple-column-db/compression"
	"github.com/dot5enko/simple-column-db/manager/cache"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

// reading should be thread safe
// alloc free
func (m *SlabManager) LoadSlabHeaderToCache(schemaObject *schema.Schema, slabUid uuid.UUID) (result *schema.DiskSlabHeader, e error) {

	slabHeader := m.getSlabHeaderFromCache(slabUid)

	if slabHeader != nil {
		return slabHeader.Header, nil
	} else {

		v, err, _ := m.loadGroup.Do(slabUid.String(), func() (any, error) {

			slabReadCache, slabCacheIdx := m.fullSlabBufferRing.Get()
			headerReadBuffer, headerBufferIdx := m.headerReaderBufferRing.Get()

			// no need to block this resources for whole duration of func
			// todo optimize
			defer func() {
				m.fullSlabBufferRing.Return(slabCacheIdx)
				m.headerReaderBufferRing.Return(headerBufferIdx)
			}()

			// slog.Info("loading slab to cache from disk", "slab_uid", slabUid.String())

			fileReader, openErr := m.GetSlabFile(*schemaObject, slabUid, false)
			if openErr != nil {
				return nil, openErr
			} else {
				defer fileReader.Close()

				// readStart := time.Now()
				headerReadErr := fileReader.ReadAt(headerReadBuffer, 0, int(schema.SlabHeaderFixedSize))
				// slog.Info("read slab from disk", "slab_uid", slabUid.String())

				if headerReadErr != nil {
					return nil, fmt.Errorf("unable to read slab header : %s", headerReadErr.Error())
				} else {

					// ioTime := time.Since(readStart).Seconds()

					var headerCacheEntryId uint16
					result, headerCacheEntryId = m.slabHeaderCache.Get()

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

					m.slabHeaderCacheLocker.Lock()
					defer m.slabHeaderCacheLocker.Unlock()

					m.slabHeaderCacheItem[slabUid] = &cache.SlabCacheItem{
						CacheEntryId: headerCacheEntryId,
						Header:       result,
						RtStats:      &cache.CacheStats{Created: time.Now()},
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

	log.Printf("--load slab data contents: %s", key)

	v, err, _ := m.loadGroup.Do(key, func() (any, error) {

		// read compressed data
		log.Printf("load slab data contents: %s", key)

		allBlocksHeaderSize := int(result.BlocksTotal) * int(schema.TotalHeaderSize)
		dataOffset := int(schema.SlabHeaderFixedSize) + allBlocksHeaderSize

		fileReader, openErr := m.GetSlabFile(*schemaObject, uid, false)
		if openErr != nil {
			return nil, openErr
		}

		defer fileReader.Close()
		item, slabId := m.slabRuntimeCache.Get()

		// todo improve this part
		// should be done on .Get inside RingBuffer
		item.Reset()
		item.RtStats.CacheEntryId = slabId

		// at this point we need to lock slab's data for reading
		// as it may be compressed
		readCompressedDataErr := fileReader.ReadAt(item.Data[:], dataOffset, int(result.CompressedSlabContentSize))

		if readCompressedDataErr != nil {
			return nil, readCompressedDataErr
		} else {

			if result.CompressionType != 0 {

				panic("compression not implemented while LoadSlabDataContents")

				switch result.CompressionType {
				case 1:
					_, decompressErr := compression.DecompressLz4(item.Data[:result.CompressedSlabContentSize], item.Data[:])
					if decompressErr != nil {

						spew.Dump("input buffers to decompress ", item.Data[:256])

						return nil, fmt.Errorf("unable to decompress slab data [input length %d, outputd buffer: %d]: %s", result.CompressedSlabContentSize, len(item.Data[:]), decompressErr.Error())
					}
				default:
					return nil, fmt.Errorf("unsupported compression type: %d", result.CompressionType)
				}
			}

			m.slabDataCacheLocker.RLock()
			defer m.slabDataCacheLocker.RUnlock()

			m.slabDataCache[uid] = item

			return item, nil
		}
	})

	if err != nil {
		return nil, err
	}

	return v.(*cache.SlabDataCacheItem), nil

}
