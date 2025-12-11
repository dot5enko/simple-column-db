package io

import (
	"encoding/binary"
	"fmt"

	"github.com/dot5enko/simple-column-db/bits"
	"github.com/dot5enko/simple-column-db/block"
	"github.com/dot5enko/simple-column-db/schema"
)

type HeaderReader struct {
}

func (h *HeaderReader) FromBytes(input []byte) (header block.DiskHeader, topErr error) {

	reader := bits.NewBinReader(input, binary.LittleEndian)

	header.GroupUid, topErr = reader.ReadUUID()
	if topErr != nil {
		return header, fmt.Errorf("unable to decode block header guid: %s", topErr.Error())
	}

	header.StartOffset, topErr = reader.ReadU64()
	if topErr != nil {
		return header, fmt.Errorf("unable to decode block header start offset: %s", topErr.Error())
	}
	header.CompressedSize, topErr = reader.ReadU64()
	if topErr != nil {
		return header, fmt.Errorf("unable to decode block header compressed size: %s", topErr.Error())
	}

	columnTypeRaw, topErr := reader.ReadU8()
	if topErr != nil {
		return header, fmt.Errorf("unable to decode block header column type: %s", topErr.Error())
	}

	columnType := schema.FieldType(columnTypeRaw)

	// read max/min values
	switch columnType {
	case schema.Int8FieldType, schema.Int16FieldType, schema.Int32FieldType, schema.Int64FieldType:
		header.MaxIValue = reader.MustReadI64()
		header.MinIValue = reader.MustReadI64()
	case schema.Uint8FieldType, schema.Uint16FieldType, schema.Uint32FieldType, schema.Uint64FieldType:
		header.MaxIValue = int64(reader.MustReadU64())
		header.MinIValue = int64(reader.MustReadU64())
	case schema.Float32FieldType, schema.Float64FieldType:
		header.MaxFValue = reader.MustReadF64()
		header.MinFValue = reader.MustReadF64()
	default:
		panic(fmt.Sprintf("unsupported field type: %d", columnType.String()))
	}

	return header, nil

}
