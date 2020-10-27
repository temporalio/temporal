// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package checksum

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/gogo/protobuf/proto"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// ErrMismatch indicates a checksum verification failure due to
// a derived checksum not being equal to expected checksum
var ErrMismatch = errors.New("checksum mismatch error")

// GenerateCRC32 generates an IEEE crc32 checksum on the
// serilized byte array of the given thrift object. The
// serialization proto used will be of type thriftRW
func GenerateCRC32(
	payload proto.Marshaler,
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
	payload proto.Marshaler,
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
