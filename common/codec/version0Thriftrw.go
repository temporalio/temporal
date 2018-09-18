// Copyright (c) 2017 Uber Technologies, Inc.
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

package codec

import (
	"bytes"

	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
)

type (
	// ThriftRWEncoder is an implementation using thrift rw for binary encoding / decoding
	// NOTE: this encoder only works for thrift struct
	ThriftRWEncoder struct {
	}
)

var _ BinaryEncoder = (*ThriftRWEncoder)(nil)

// NewThriftRWEncoder generate a new ThriftRWEncoder
func NewThriftRWEncoder() *ThriftRWEncoder {
	return &ThriftRWEncoder{}
}

// Encode encode the object
func (t *ThriftRWEncoder) Encode(obj ThriftObject) ([]byte, error) {
	if obj == nil {
		return nil, MsgPayloadNotThriftEncoded
	}
	var writer bytes.Buffer
	// use the first byte to version the serialization
	err := writer.WriteByte(preambleVersion0)
	if err != nil {
		return nil, err
	}
	val, err := obj.ToWire()
	if err != nil {
		return nil, err
	}
	err = protocol.Binary.Encode(val, &writer)
	if err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

// Decode decode the object
func (t *ThriftRWEncoder) Decode(binary []byte, val ThriftObject) error {
	if len(binary) < 1 {
		return MissingBinaryEncodingVersion
	}

	version := binary[0]
	if version != preambleVersion0 {
		return InvalidBinaryEncodingVersion
	}

	reader := bytes.NewReader(binary[1:])
	wireVal, err := protocol.Binary.Decode(reader, wire.TStruct)
	if err != nil {
		return err
	}

	return val.FromWire(wireVal)
}
