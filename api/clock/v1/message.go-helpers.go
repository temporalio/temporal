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

package clock

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type VectorClock to the protobuf v3 wire format
func (val *VectorClock) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VectorClock from the protobuf v3 wire format
func (val *VectorClock) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VectorClock) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VectorClock values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VectorClock) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VectorClock
	switch t := that.(type) {
	case *VectorClock:
		that1 = t
	case VectorClock:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HybridLogicalClock to the protobuf v3 wire format
func (val *HybridLogicalClock) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HybridLogicalClock from the protobuf v3 wire format
func (val *HybridLogicalClock) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HybridLogicalClock) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HybridLogicalClock values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HybridLogicalClock) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HybridLogicalClock
	switch t := that.(type) {
	case *HybridLogicalClock:
		that1 = t
	case HybridLogicalClock:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
