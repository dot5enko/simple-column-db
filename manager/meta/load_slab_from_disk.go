package meta

import (
	"bytes"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/dot5enko/simple-column-db/compression"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (m *SlabManager) LoadSlabToCache(schemaObject schema.Schema, slabUid uuid.UUID) (result *schema.DiskSlabHeader, e error) {

	slabHeader := m.getSlabFromCache(slabUid)

	if slabHeader != nil {
		return slabHeader.Header, nil
	} else {

		v, err, _ := m.loadGroup.Do(slabUid.String(), func() (any, error) {
			// slog.Info("loading slab to cache from disk", "slab_uid", slabUid.String())

			fileReader, openErr := m.GetSlabFile(schemaObject, slabUid, false)
			if openErr != nil {
				return nil, openErr
			} else {

				// readStart := time.Now()
				headerReadErr := fileReader.ReadAt(m.SlabHeaderReaderBuffer[:], 0, int(schema.SlabHeaderFixedSize))
				if headerReadErr != nil {
					return nil, fmt.Errorf("unable to read slab header : %s", headerReadErr.Error())
				} else {

					// ioTime := time.Since(readStart).Seconds()

					// color.Red("reading slab header %s. IO: %.2fms", slabUid.String(), ioTime*1000)

					result = &schema.DiskSlabHeader{}

					// fmt.Printf(" >> rsh reading slab header %s : \n >> rsh %v\n", slabUid.String(), m.SlabHeaderReaderBuffer[:schema.SlabHeaderFixedSize])

					headerBytes := bytes.NewReader(m.SlabHeaderReaderBuffer[:schema.SlabHeaderFixedSize])
					headerParseErr := result.FromBytes(headerBytes)
					if headerParseErr != nil {
						return nil, headerParseErr
					} else {

						// read the rest of headers, and their content

						result.BlockHeaders = make([]schema.DiskHeader, result.BlocksTotal)

						allBlocksHeaderSize := int(result.BlocksTotal) * int(schema.TotalHeaderSize)
						nonEmptyHeadersSize := int(result.BlocksFinalized) * int(schema.TotalHeaderSize) // finalized + current

						if result.BlocksFinalized < result.BlocksTotal {
							nonEmptyHeadersSize += int(schema.TotalHeaderSize)
						}

						headersReadErr := fileReader.ReadAt(m.SlabBlockHeadersReadBuffer[:], int(schema.SlabHeaderFixedSize), nonEmptyHeadersSize)

						if headersReadErr != nil {
							return nil, fmt.Errorf("unable to read data while LoadSlabToCache: %s", headersReadErr.Error())
						} else {

							blocksToIterate := int(result.BlocksFinalized) + 1
							if blocksToIterate >= int(result.BlocksTotal) {
								blocksToIterate = int(result.BlocksTotal)
							}

							// color.Red("iterate blocks : %d (finalized: %d/ total : %d)", blocksToIterate, result.BlocksFinalized, result.BlocksTotal)

							for i := 0; i < blocksToIterate; i++ {

								// func(i int) {

								blockOffset := i * int(schema.TotalHeaderSize)
								headerBuffer := m.SlabBlockHeadersReadBuffer[blockOffset:]

								headerDecodeErr := result.BlockHeaders[i].FromBytes(bytes.NewReader(headerBuffer))

								if headerDecodeErr != nil {
									return nil, headerDecodeErr
								}
								// }(j)
							}
						}

						// read compressed data
						dataOffset := int(schema.SlabHeaderFixedSize) + allBlocksHeaderSize
						readCompressedDataErr := fileReader.ReadAt(m.BufferForCompressedData10Mb[:], dataOffset, int(result.CompressedSlabContentSize))

						if readCompressedDataErr != nil {
							return nil, readCompressedDataErr
						} else {

							item, cacheErr := m.cacheManager.GetCacheEntry()
							if cacheErr != nil {
								return nil, cacheErr
							}

							item.Header = result

							if result.CompressionType == 0 {
								copy(item.Data[:], m.BufferForCompressedData10Mb[:result.CompressedSlabContentSize])
							} else {
								switch result.CompressionType {
								case 1:
									_, decompressErr := compression.DecompressLz4(m.BufferForCompressedData10Mb[:result.CompressedSlabContentSize], item.Data[:])
									if decompressErr != nil {

										spew.Dump("input buffers to decompress ", m.BufferForCompressedData10Mb[:256])

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

	return
}
