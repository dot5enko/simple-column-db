package manager

import (
	"encoding/binary"

	"github.com/dot5enko/simple-column-db/bits"
)

type IngestBuffer struct {
	dataBuffer []byte

	FieldsLayout []string

	bitWriter *bits.BitWriter
}

func NewIngestBuffer(fieldsLayout []string) *IngestBuffer {

	b := &IngestBuffer{
		FieldsLayout: fieldsLayout,
		dataBuffer:   []byte{},
	}

	bitWriter := bits.NewEncodeBuffer(b.dataBuffer, binary.LittleEndian)
	b.bitWriter = &bitWriter

	return b
}

func IngestBufferFromBinary(binData []byte, fields []string) *IngestBuffer {
	return &IngestBuffer{
		dataBuffer:   binData,
		FieldsLayout: fields,
	}
}

func (b *IngestBuffer) AddRows(rows []any) {

}
