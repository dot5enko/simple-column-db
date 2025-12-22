package manager

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

		fileReader, openErr := m.GetSlabFile(schemaObject, slabUid, false)
		if openErr != nil {
			e = openErr
			return
		} else {

			headerReadErr := fileReader.ReadAt(m.SlabHeaderReaderBuffer[:], 0, int(schema.SlabHeaderFixedSize))

			if headerReadErr != nil {
				e = fmt.Errorf("unable to read slab header : %s", headerReadErr.Error())
				return
			} else {

				result = &schema.DiskSlabHeader{}

				// fmt.Printf(" >> rsh reading slab header %s : \n >> rsh %v\n", slabUid.String(), m.SlabHeaderReaderBuffer[:schema.SlabHeaderFixedSize])

				headerBytes := bytes.NewReader(m.SlabHeaderReaderBuffer[:schema.SlabHeaderFixedSize])
				headerParseErr := result.FromBytes(headerBytes)
				if headerParseErr != nil {
					e = headerParseErr
					return
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
						e = fmt.Errorf("unable to read data while LoadSlabToCache: %s", headersReadErr.Error())
						return
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
								e = headerDecodeErr
								return
							}
							// }(j)
						}
					}

					// read compressed data
					dataOffset := int(schema.SlabHeaderFixedSize) + allBlocksHeaderSize
					readCompressedDataErr := fileReader.ReadAt(m.BufferForCompressedData10Mb[:], dataOffset, int(result.CompressedSlabContentSize))

					if readCompressedDataErr != nil {
						e = readCompressedDataErr
						return
					} else {

						item, cacheErr := m.cacheManager.GetCacheEntry()
						if cacheErr != nil {
							e = cacheErr
							return
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

									e = fmt.Errorf("unable to decompress slab data [input length %d, outputd buffer: %d]: %s", result.CompressedSlabContentSize, len(item.Data[:]), decompressErr.Error())
									return
								}
							default:
								e = fmt.Errorf("unsupported compression type: %d", result.CompressionType)
								return
							}
						}

						m.slabCacheLocker.Lock()
						defer m.slabCacheLocker.Unlock()

						m.slabCacheItem[slabUid] = item
					}

				}
			}

		}
	}

	return
}
