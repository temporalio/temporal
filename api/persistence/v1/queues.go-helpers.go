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

// Marshal an object of type QueueState to the protobuf v3 wire format
func (val *QueueState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueueState from the protobuf v3 wire format
func (val *QueueState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueueState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueueState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueueState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueueState
	switch t := that.(type) {
	case *QueueState:
		that1 = t
	case QueueState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueueReaderState to the protobuf v3 wire format
func (val *QueueReaderState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueueReaderState from the protobuf v3 wire format
func (val *QueueReaderState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueueReaderState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueueReaderState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueueReaderState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueueReaderState
	switch t := that.(type) {
	case *QueueReaderState:
		that1 = t
	case QueueReaderState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueueSliceScope to the protobuf v3 wire format
func (val *QueueSliceScope) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueueSliceScope from the protobuf v3 wire format
func (val *QueueSliceScope) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueueSliceScope) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueueSliceScope values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueueSliceScope) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueueSliceScope
	switch t := that.(type) {
	case *QueueSliceScope:
		that1 = t
	case QueueSliceScope:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type QueueSliceRange to the protobuf v3 wire format
func (val *QueueSliceRange) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueueSliceRange from the protobuf v3 wire format
func (val *QueueSliceRange) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueueSliceRange) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueueSliceRange values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueueSliceRange) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueueSliceRange
	switch t := that.(type) {
	case *QueueSliceRange:
		that1 = t
	case QueueSliceRange:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ReadQueueMessagesNextPageToken to the protobuf v3 wire format
func (val *ReadQueueMessagesNextPageToken) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReadQueueMessagesNextPageToken from the protobuf v3 wire format
func (val *ReadQueueMessagesNextPageToken) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReadQueueMessagesNextPageToken) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReadQueueMessagesNextPageToken values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReadQueueMessagesNextPageToken) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReadQueueMessagesNextPageToken
	switch t := that.(type) {
	case *ReadQueueMessagesNextPageToken:
		that1 = t
	case ReadQueueMessagesNextPageToken:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ListQueuesNextPageToken to the protobuf v3 wire format
func (val *ListQueuesNextPageToken) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ListQueuesNextPageToken from the protobuf v3 wire format
func (val *ListQueuesNextPageToken) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ListQueuesNextPageToken) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ListQueuesNextPageToken values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ListQueuesNextPageToken) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ListQueuesNextPageToken
	switch t := that.(type) {
	case *ListQueuesNextPageToken:
		that1 = t
	case ListQueuesNextPageToken:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

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

// Marshal an object of type QueuePartition to the protobuf v3 wire format
func (val *QueuePartition) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type QueuePartition from the protobuf v3 wire format
func (val *QueuePartition) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *QueuePartition) Size() int {
	return proto.Size(val)
}

// Equal returns whether two QueuePartition values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *QueuePartition) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *QueuePartition
	switch t := that.(type) {
	case *QueuePartition:
		that1 = t
	case QueuePartition:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type Queue to the protobuf v3 wire format
func (val *Queue) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type Queue from the protobuf v3 wire format
func (val *Queue) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *Queue) Size() int {
	return proto.Size(val)
}

// Equal returns whether two Queue values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *Queue) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *Queue
	switch t := that.(type) {
	case *Queue:
		that1 = t
	case Queue:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
