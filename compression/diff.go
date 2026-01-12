package compression

import (
	"bytes"
)

type Meta struct {
	First uint64
	Count uint32
	Codec uint8 // 1=delta+zigzag, 2=xor

	SkipSize int
}

func zigzagEncode(x int64) uint64 {
	return uint64((x << 1) ^ (x >> 63))
}

func zigzagDecode(x uint64) int64 {
	return int64((x >> 1) ^ uint64(-(x & 1)))
}

func CompressDiff(input []uint64) ([]byte, Meta, error) {
	if len(input) == 0 {
		return nil, Meta{}, nil
	}

	var buf bytes.Buffer
	prev := input[0]

	for i := 1; i < len(input); i++ {
		d := int64(input[i] - prev)
		zz := zigzagEncode(d)
		for zz >= 0x80 {
			buf.WriteByte(byte(zz) | 0x80)
			zz >>= 7
		}
		buf.WriteByte(byte(zz))
		prev = input[i]
	}

	meta := Meta{
		First: input[0],
		Count: uint32(len(input)),
		Codec: 1,
	}
	return buf.Bytes(), meta, nil
}

func DecompressDiff(data []byte, meta Meta, out []uint64) ([]uint64, error) {
	if meta.Count == 0 {
		return out[:0], nil
	}

	if cap(out) < int(meta.Count) {
		out = make([]uint64, meta.Count)
	} else {
		out = out[:meta.Count]
	}

	out[0] = meta.First
	prev := meta.First
	idx := 1

	var shift uint
	var v uint64

	for _, b := range data {
		v |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			d := zigzagDecode(v)
			prev = uint64(int64(prev) + d)
			out[idx] = prev
			idx++
			v = 0
			shift = 0
		} else {
			shift += 7
		}
	}
	return out, nil
}

func CompressDiffWithSkips(input []uint64, skipsSize int) ([]byte, Meta, error) {

	if len(input) == 0 {
		return nil, Meta{}, nil
	}

	var buf bytes.Buffer
	var blockInfo []byte

	prev := input[0]
	blockCount := 0
	multiByteCount := 0

	for i := 1; i <= len(input); i++ {
		if i == len(input) || blockCount == skipsSize {
			// write block info: number of multi-byte values in this block
			blockInfo = append(blockInfo, byte(multiByteCount))
			blockCount = 0
			multiByteCount = 0
		}
		if i == len(input) {
			break
		}

		d := int64(input[i] - prev)
		zz := zigzagEncode(d)

		// count if diff needs more than 1 byte
		if zz >= 0x80 {
			multiByteCount++
		}

		for zz >= 0x80 {
			buf.WriteByte(byte(zz) | 0x80)
			zz >>= 7
		}
		buf.WriteByte(byte(zz))
		prev = input[i]
		blockCount++
	}

	// prepend block info to compressed data
	final := append(blockInfo, buf.Bytes()...)

	meta := Meta{
		First:    input[0],
		Count:    uint32(len(input)),
		Codec:    2, // updated codec,
		SkipSize: skipsSize,
	}
	return final, meta, nil
}

// func processBlockOfSingleByteDiffs(buf *byte, bufIdx int, metaCount int, prev uint64, out *uint64, idx int) (int, int, uint64)
//
//go:noescape
//go:nosplit
func processBlockOfSingleByteDiffs(buf []byte, bufIdx int, metaCount int, prev uint64, out []uint64, idx int) (int, int, uint64)

//  {

// 	for j := 0; j < 64 && idx < metaCount; j++ {
// 		v := uint64(buf[bufIdx])
// 		bufIdx++
// 		prev += uint64(int64((v >> 1) ^ uint64(-(v & 1))))
// 		out[idx] = prev
// 		idx++
// 	}

// 	return bufIdx, idx, prev

// }

func DecompressDiffWithSkips(data []byte, meta Meta, out []uint64) ([]uint64, error) {
	if meta.Count == 0 {
		return out[:0], nil
	}

	if cap(out) < int(meta.Count) {
		out = make([]uint64, meta.Count)
	} else {
		out = out[:meta.Count]
	}

	out[0] = meta.First
	prev := meta.First
	idx := 1

	// number of blocks
	numBlocks := (int(meta.Count) - 1 + (meta.SkipSize - 1)) / meta.SkipSize

	// log.Printf(" -- num blocks : %d", numBlocks)

	blockInfo := data[:numBlocks]
	buf := data[numBlocks:]

	bufIdx := 0

	for mbIdx, multiByteCount := range blockInfo {
		if multiByteCount == 0 {
			// fast path: all 1-byte diffs

			metaCount := int(meta.Count)

			for j := 0; j < meta.SkipSize && idx < metaCount; j++ {
				v := uint64(buf[bufIdx])
				bufIdx++
				prev += uint64(int64((v >> 1) ^ uint64(-(v & 1))))
				out[idx] = prev
				idx++
			}

			// bufIdx, idx, prev = processBlockOfSingleByteDiffs(
			// 	buf,
			// 	bufIdx,
			// 	metaCount,
			// 	prev,
			// 	out,
			// 	idx,
			// )

			_ = mbIdx
			// log.Printf(" block %d  processed ", mbIdx)

		} else {
			// slow path: normal varint decoding
			var shift uint
			var v uint64
			end := idx + meta.SkipSize
			if end > int(meta.Count) {
				end = int(meta.Count)
			}
			for idx < end {
				b := buf[bufIdx]
				bufIdx++
				v |= uint64(b&0x7F) << shift
				if b&0x80 == 0 {
					d := uint64(int64((v >> 1) ^ uint64(-(v & 1))))
					prev += d
					out[idx] = prev
					idx++
					v = 0
					shift = 0
				} else {
					shift += 7
				}
			}
		}
	}

	return out, nil
}
