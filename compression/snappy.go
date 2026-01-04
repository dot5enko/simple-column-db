package compression

import (
	"errors"

	"github.com/golang/snappy"
)

func CompressSnappy(src []byte, dst []byte) (int, error) {

	result := snappy.Encode(dst, src)
	return len(result), nil
}

func DecompressSnappy(src []byte, dst []byte) (int, error) {

	len, lenErr := snappy.DecodedLen(src)

	if lenErr != nil {
		return 0, lenErr
	}

	if cap(dst) < len {
		return 0, errors.New("dst buffer too small")
	}

	_, err := snappy.Decode(dst, src)
	return len, err

}
