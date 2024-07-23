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

// Code generated by protoc-gen-go-helpers. DO NOT EDIT.
package persistence

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type StateMachineNode to the protobuf v3 wire format
func (val *StateMachineNode) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineNode from the protobuf v3 wire format
func (val *StateMachineNode) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineNode) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineNode values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineNode) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineNode
	switch t := that.(type) {
	case *StateMachineNode:
		that1 = t
	case StateMachineNode:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineMap to the protobuf v3 wire format
func (val *StateMachineMap) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineMap from the protobuf v3 wire format
func (val *StateMachineMap) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineMap) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineMap values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineMap) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineMap
	switch t := that.(type) {
	case *StateMachineMap:
		that1 = t
	case StateMachineMap:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineKey to the protobuf v3 wire format
func (val *StateMachineKey) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineKey from the protobuf v3 wire format
func (val *StateMachineKey) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineKey) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineKey values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineKey) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineKey
	switch t := that.(type) {
	case *StateMachineKey:
		that1 = t
	case StateMachineKey:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineRef to the protobuf v3 wire format
func (val *StateMachineRef) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineRef from the protobuf v3 wire format
func (val *StateMachineRef) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineRef) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineRef values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineRef) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineRef
	switch t := that.(type) {
	case *StateMachineRef:
		that1 = t
	case StateMachineRef:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineTaskInfo to the protobuf v3 wire format
func (val *StateMachineTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineTaskInfo from the protobuf v3 wire format
func (val *StateMachineTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineTaskInfo
	switch t := that.(type) {
	case *StateMachineTaskInfo:
		that1 = t
	case StateMachineTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineTimerGroup to the protobuf v3 wire format
func (val *StateMachineTimerGroup) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineTimerGroup from the protobuf v3 wire format
func (val *StateMachineTimerGroup) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineTimerGroup) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineTimerGroup values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineTimerGroup) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineTimerGroup
	switch t := that.(type) {
	case *StateMachineTimerGroup:
		that1 = t
	case StateMachineTimerGroup:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type VersionedTransition to the protobuf v3 wire format
func (val *VersionedTransition) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type VersionedTransition from the protobuf v3 wire format
func (val *VersionedTransition) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *VersionedTransition) Size() int {
	return proto.Size(val)
}

// Equal returns whether two VersionedTransition values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *VersionedTransition) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *VersionedTransition
	switch t := that.(type) {
	case *VersionedTransition:
		that1 = t
	case VersionedTransition:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineTombstoneBatch to the protobuf v3 wire format
func (val *StateMachineTombstoneBatch) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineTombstoneBatch from the protobuf v3 wire format
func (val *StateMachineTombstoneBatch) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineTombstoneBatch) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineTombstoneBatch values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineTombstoneBatch) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineTombstoneBatch
	switch t := that.(type) {
	case *StateMachineTombstoneBatch:
		that1 = t
	case StateMachineTombstoneBatch:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachineTombstone to the protobuf v3 wire format
func (val *StateMachineTombstone) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachineTombstone from the protobuf v3 wire format
func (val *StateMachineTombstone) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachineTombstone) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachineTombstone values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachineTombstone) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachineTombstone
	switch t := that.(type) {
	case *StateMachineTombstone:
		that1 = t
	case StateMachineTombstone:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type StateMachinePath to the protobuf v3 wire format
func (val *StateMachinePath) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type StateMachinePath from the protobuf v3 wire format
func (val *StateMachinePath) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *StateMachinePath) Size() int {
	return proto.Size(val)
}

// Equal returns whether two StateMachinePath values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *StateMachinePath) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *StateMachinePath
	switch t := that.(type) {
	case *StateMachinePath:
		that1 = t
	case StateMachinePath:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
