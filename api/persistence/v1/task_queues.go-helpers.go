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

// Marshal an object of type BuildId to the protobuf v3 wire format
func (val *BuildId) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type BuildId from the protobuf v3 wire format
func (val *BuildId) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *BuildId) Size() int {
	return proto.Size(val)
}

// Equal returns whether two BuildId values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *BuildId) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *BuildId
	switch t := that.(type) {
	case *BuildId:
		that1 = t
	case BuildId:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type CompatibleVersionSet to the protobuf v3 wire format
func (val *CompatibleVersionSet) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type CompatibleVersionSet from the protobuf v3 wire format
func (val *CompatibleVersionSet) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *CompatibleVersionSet) Size() int {
	return proto.Size(val)
}

// Equal returns whether two CompatibleVersionSet values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *CompatibleVersionSet) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *CompatibleVersionSet
	switch t := that.(type) {
	case *CompatibleVersionSet:
		that1 = t
	case CompatibleVersionSet:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type VersioningData to the protobuf v3 wire format
func (val *VersioningData) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VersioningData from the protobuf v3 wire format
func (val *VersioningData) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VersioningData) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VersioningData values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VersioningData) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VersioningData
	switch t := that.(type) {
	case *VersioningData:
		that1 = t
	case VersioningData:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TaskQueueUserData to the protobuf v3 wire format
func (val *TaskQueueUserData) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TaskQueueUserData from the protobuf v3 wire format
func (val *TaskQueueUserData) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TaskQueueUserData) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TaskQueueUserData values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TaskQueueUserData) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TaskQueueUserData
	switch t := that.(type) {
	case *TaskQueueUserData:
		that1 = t
	case TaskQueueUserData:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type VersionedTaskQueueUserData to the protobuf v3 wire format
func (val *VersionedTaskQueueUserData) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VersionedTaskQueueUserData from the protobuf v3 wire format
func (val *VersionedTaskQueueUserData) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VersionedTaskQueueUserData) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VersionedTaskQueueUserData values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VersionedTaskQueueUserData) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VersionedTaskQueueUserData
	switch t := that.(type) {
	case *VersionedTaskQueueUserData:
		that1 = t
	case VersionedTaskQueueUserData:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
