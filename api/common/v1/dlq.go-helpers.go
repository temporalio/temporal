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

package commonspb

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type HistoryTask to the protobuf v3 wire format
func (val *HistoryTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryTask from the protobuf v3 wire format
func (val *HistoryTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryTask
	switch t := that.(type) {
	case *HistoryTask:
		that1 = t
	case HistoryTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HistoryDLQTaskMetadata to the protobuf v3 wire format
func (val *HistoryDLQTaskMetadata) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryDLQTaskMetadata from the protobuf v3 wire format
func (val *HistoryDLQTaskMetadata) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryDLQTaskMetadata) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryDLQTaskMetadata values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryDLQTaskMetadata) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryDLQTaskMetadata
	switch t := that.(type) {
	case *HistoryDLQTaskMetadata:
		that1 = t
	case HistoryDLQTaskMetadata:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HistoryDLQTask to the protobuf v3 wire format
func (val *HistoryDLQTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryDLQTask from the protobuf v3 wire format
func (val *HistoryDLQTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryDLQTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryDLQTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryDLQTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryDLQTask
	switch t := that.(type) {
	case *HistoryDLQTask:
		that1 = t
	case HistoryDLQTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HistoryDLQKey to the protobuf v3 wire format
func (val *HistoryDLQKey) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryDLQKey from the protobuf v3 wire format
func (val *HistoryDLQKey) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryDLQKey) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryDLQKey values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryDLQKey) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryDLQKey
	switch t := that.(type) {
	case *HistoryDLQKey:
		that1 = t
	case HistoryDLQKey:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
