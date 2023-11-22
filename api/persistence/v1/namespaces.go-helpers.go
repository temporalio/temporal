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

package persistence

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type NamespaceDetail to the protobuf v3 wire format
func (val *NamespaceDetail) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NamespaceDetail from the protobuf v3 wire format
func (val *NamespaceDetail) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NamespaceDetail) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NamespaceDetail values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NamespaceDetail) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NamespaceDetail
	switch t := that.(type) {
	case *NamespaceDetail:
		that1 = t
	case NamespaceDetail:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NamespaceInfo to the protobuf v3 wire format
func (val *NamespaceInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NamespaceInfo from the protobuf v3 wire format
func (val *NamespaceInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NamespaceInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NamespaceInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NamespaceInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NamespaceInfo
	switch t := that.(type) {
	case *NamespaceInfo:
		that1 = t
	case NamespaceInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NamespaceConfig to the protobuf v3 wire format
func (val *NamespaceConfig) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NamespaceConfig from the protobuf v3 wire format
func (val *NamespaceConfig) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NamespaceConfig) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NamespaceConfig values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NamespaceConfig) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NamespaceConfig
	switch t := that.(type) {
	case *NamespaceConfig:
		that1 = t
	case NamespaceConfig:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NamespaceReplicationConfig to the protobuf v3 wire format
func (val *NamespaceReplicationConfig) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NamespaceReplicationConfig from the protobuf v3 wire format
func (val *NamespaceReplicationConfig) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NamespaceReplicationConfig) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NamespaceReplicationConfig values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NamespaceReplicationConfig) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NamespaceReplicationConfig
	switch t := that.(type) {
	case *NamespaceReplicationConfig:
		that1 = t
	case NamespaceReplicationConfig:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type FailoverStatus to the protobuf v3 wire format
func (val *FailoverStatus) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type FailoverStatus from the protobuf v3 wire format
func (val *FailoverStatus) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *FailoverStatus) Size() int {
	return proto.Size(val)
}

// Equal returns whether two FailoverStatus values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *FailoverStatus) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *FailoverStatus
	switch t := that.(type) {
	case *FailoverStatus:
		that1 = t
	case FailoverStatus:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
