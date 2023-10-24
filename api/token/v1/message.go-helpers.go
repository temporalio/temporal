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

package token

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type HistoryContinuation to the protobuf v3 wire format
func (val *HistoryContinuation) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryContinuation from the protobuf v3 wire format
func (val *HistoryContinuation) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryContinuation) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryContinuation values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryContinuation) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryContinuation
	switch t := that.(type) {
	case *HistoryContinuation:
		that1 = t
	case HistoryContinuation:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type RawHistoryContinuation to the protobuf v3 wire format
func (val *RawHistoryContinuation) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type RawHistoryContinuation from the protobuf v3 wire format
func (val *RawHistoryContinuation) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *RawHistoryContinuation) Size() int {
	return proto.Size(val)
}

// Equal returns whether two RawHistoryContinuation values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *RawHistoryContinuation) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *RawHistoryContinuation
	switch t := that.(type) {
	case *RawHistoryContinuation:
		that1 = t
	case RawHistoryContinuation:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type Task to the protobuf v3 wire format
func (val *Task) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type Task from the protobuf v3 wire format
func (val *Task) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *Task) Size() int {
	return proto.Size(val)
}

// Equal returns whether two Task values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *Task) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *Task
	switch t := that.(type) {
	case *Task:
		that1 = t
	case Task:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueryTask to the protobuf v3 wire format
func (val *QueryTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueryTask from the protobuf v3 wire format
func (val *QueryTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueryTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueryTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueryTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueryTask
	switch t := that.(type) {
	case *QueryTask:
		that1 = t
	case QueryTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
