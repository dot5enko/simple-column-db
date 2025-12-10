package compression

import (
	"bytes"

	"github.com/pierrec/lz4/v4"
)

func CompressLz4(src []byte, output *bytes.Buffer) error {
	zw := lz4.NewWriter(output)

	zw.Write(src)
	flushErr := zw.Flush()

	if flushErr != nil {
		return flushErr
	}

	return zw.Close()
}
