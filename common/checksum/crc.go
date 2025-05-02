package checksum

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// ErrMismatch indicates a checksum verification failure due to
// a derived checksum not being equal to expected checksum
var ErrMismatch = errors.New("checksum mismatch error")

type Marshaler interface {
	Marshal() ([]byte, error)
}

// GenerateCRC32 generates an IEEE crc32 checksum on the
// serilized byte array of the given thrift object. The
// serialization proto used will be of type thriftRW
func GenerateCRC32(
	payload Marshaler,
	payloadVersion int32,
) (*persistencespb.Checksum, error) {

	payloadBytes, err := payload.Marshal()
	if err != nil {
		return nil, err
	}

	crc := crc32.ChecksumIEEE(payloadBytes)
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc)
	return &persistencespb.Checksum{
		Value:   checksum,
		Version: payloadVersion,
		Flavor:  enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY,
	}, nil
}

// Verify verifies that the checksum generated from the
// given thrift object matches the specified expected checksum
// Return ErrMismatch when checksums mismatch
func Verify(
	payload Marshaler,
	checksum *persistencespb.Checksum,
) error {

	if checksum.Flavor != enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY {
		return fmt.Errorf("unknown checksum flavor %v", checksum.Flavor)
	}

	expected, err := GenerateCRC32(payload, checksum.Version)
	if err != nil {
		return err
	}

	if !bytes.Equal(expected.GetValue(), checksum.GetValue()) {
		return ErrMismatch
	}

	return nil
}
