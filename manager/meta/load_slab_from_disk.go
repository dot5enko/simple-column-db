package meta

import (
	"bytes"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/dot5enko/simple-column-db/compression"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

// reading should be thread safe
// alloc free
func (m *SlabManager) LoadSlabToCache(schemaObject *schema.Schema, slabUid uuid.UUID) (result *schema.DiskSlabHeader, e error) {

	slabHeader := m.getSlabFromCache(slabUid)

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

					result = &schema.DiskSlabHeader{}

					headerBytes := bytes.NewReader(headerReadBuffer)
					headerParseErr := result.FromBytes(headerBytes)
					if headerParseErr != nil {
						return nil, headerParseErr
					} else {

						// read the rest of headers, and their content
						// todo use preallocated buffer
						result.BlockHeaders = make([]schema.DiskHeader, result.BlocksTotal)

						allBlocksHeaderSize := int(result.BlocksTotal) * int(schema.TotalHeaderSize)
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

						// read compressed data
						dataOffset := int(schema.SlabHeaderFixedSize) + allBlocksHeaderSize
						readCompressedDataErr := fileReader.ReadAt(slabReadCache, dataOffset, int(result.CompressedSlabContentSize))

						if readCompressedDataErr != nil {
							return nil, readCompressedDataErr
						} else {

							item, cacheErr := m.cacheManager.GetCacheEntry()
							if cacheErr != nil {
								return nil, cacheErr
							}

							item.Header = result

							if result.CompressionType == 0 {
								copy(item.Data[:], slabReadCache[:result.CompressedSlabContentSize])
							} else {
								switch result.CompressionType {
								case 1:
									_, decompressErr := compression.DecompressLz4(slabReadCache[:result.CompressedSlabContentSize], item.Data[:])
									if decompressErr != nil {

										spew.Dump("input buffers to decompress ", slabReadCache[:256])

										return nil, fmt.Errorf("unable to decompress slab data [input length %d, outputd buffer: %d]: %s", result.CompressedSlabContentSize, len(item.Data[:]), decompressErr.Error())
									}
								default:
									return nil, fmt.Errorf("unsupported compression type: %d", result.CompressionType)
								}
							}

							m.slabCacheLocker.Lock()
							defer m.slabCacheLocker.Unlock()

							m.slabCacheItem[slabUid] = item

							return item.Header, nil
						}

					}
				}
			}
		})

		if err != nil {
			return nil, err
		}

		return v.(*schema.DiskSlabHeader), nil

	}

}
