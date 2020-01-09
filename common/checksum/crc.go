// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"hash/crc32"

	"github.com/uber/cadence/common/codec"
)

// GenerateCRC32 generates an IEEE crc32 checksum on the
// serilized byte array of the given thrift object. The
// serialization proto used will be of type thriftRW
func GenerateCRC32(
	payload codec.ThriftObject,
	payloadVersion int,
) (Checksum, error) {

	encoder := codec.NewThriftRWEncoder()
	payloadBytes, err := encoder.Encode(payload)
	if err != nil {
		return Checksum{}, err
	}

	crc := crc32.ChecksumIEEE(payloadBytes)
	checksum := make([]byte, 4)
	binary.BigEndian.PutUint32(checksum, crc)
	return Checksum{
		Value:   checksum,
		Version: payloadVersion,
		Flavor:  FlavorIEEECRC32OverThriftBinary,
	}, nil
}

// Verify verifies that the checksum generated from the
// given thrift object matches the specified expected checksum
// Return ErrMismatch when checksums mismatch
func Verify(
	payload codec.ThriftObject,
	checksum Checksum,
) error {

	if !checksum.Flavor.IsValid() || checksum.Flavor != FlavorIEEECRC32OverThriftBinary {
		return fmt.Errorf("unknown checksum flavor %v", checksum.Flavor)
	}

	expected, err := GenerateCRC32(payload, checksum.Version)
	if err != nil {
		return err
	}

	if !bytes.Equal(expected.Value, checksum.Value) {
		return ErrMismatch
	}

	return nil
}
