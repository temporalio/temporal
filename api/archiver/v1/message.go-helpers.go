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

package archiver

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type HistoryBlobHeader to the protobuf v3 wire format
func (val *HistoryBlobHeader) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryBlobHeader from the protobuf v3 wire format
func (val *HistoryBlobHeader) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryBlobHeader) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryBlobHeader values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryBlobHeader) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryBlobHeader
	switch t := that.(type) {
	case *HistoryBlobHeader:
		that1 = t
	case HistoryBlobHeader:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HistoryBlob to the protobuf v3 wire format
func (val *HistoryBlob) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryBlob from the protobuf v3 wire format
func (val *HistoryBlob) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryBlob) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryBlob values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryBlob) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryBlob
	switch t := that.(type) {
	case *HistoryBlob:
		that1 = t
	case HistoryBlob:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type VisibilityRecord to the protobuf v3 wire format
func (val *VisibilityRecord) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VisibilityRecord from the protobuf v3 wire format
func (val *VisibilityRecord) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VisibilityRecord) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VisibilityRecord values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VisibilityRecord) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VisibilityRecord
	switch t := that.(type) {
	case *VisibilityRecord:
		that1 = t
	case VisibilityRecord:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
