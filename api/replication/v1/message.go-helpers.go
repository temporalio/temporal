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

package repication

import (
	"google.golang.org/protobuf/proto"
)

// Marshal an object of type ReplicationTask to the protobuf v3 wire format
func (val *ReplicationTask) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReplicationTask from the protobuf v3 wire format
func (val *ReplicationTask) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReplicationTask) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReplicationTask values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReplicationTask) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReplicationTask
	switch t := that.(type) {
	case *ReplicationTask:
		that1 = t
	case ReplicationTask:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ReplicationToken to the protobuf v3 wire format
func (val *ReplicationToken) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReplicationToken from the protobuf v3 wire format
func (val *ReplicationToken) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReplicationToken) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReplicationToken values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReplicationToken) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReplicationToken
	switch t := that.(type) {
	case *ReplicationToken:
		that1 = t
	case ReplicationToken:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncShardStatus to the protobuf v3 wire format
func (val *SyncShardStatus) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncShardStatus from the protobuf v3 wire format
func (val *SyncShardStatus) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncShardStatus) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncShardStatus values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncShardStatus) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncShardStatus
	switch t := that.(type) {
	case *SyncShardStatus:
		that1 = t
	case SyncShardStatus:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncReplicationState to the protobuf v3 wire format
func (val *SyncReplicationState) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncReplicationState from the protobuf v3 wire format
func (val *SyncReplicationState) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncReplicationState) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncReplicationState values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncReplicationState) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncReplicationState
	switch t := that.(type) {
	case *SyncReplicationState:
		that1 = t
	case SyncReplicationState:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ReplicationMessages to the protobuf v3 wire format
func (val *ReplicationMessages) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReplicationMessages from the protobuf v3 wire format
func (val *ReplicationMessages) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReplicationMessages) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReplicationMessages values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReplicationMessages) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReplicationMessages
	switch t := that.(type) {
	case *ReplicationMessages:
		that1 = t
	case ReplicationMessages:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type WorkflowReplicationMessages to the protobuf v3 wire format
func (val *WorkflowReplicationMessages) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type WorkflowReplicationMessages from the protobuf v3 wire format
func (val *WorkflowReplicationMessages) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *WorkflowReplicationMessages) Size() int {
	return proto.Size(val)
}

// Equal returns whether two WorkflowReplicationMessages values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *WorkflowReplicationMessages) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *WorkflowReplicationMessages
	switch t := that.(type) {
	case *WorkflowReplicationMessages:
		that1 = t
	case WorkflowReplicationMessages:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type ReplicationTaskInfo to the protobuf v3 wire format
func (val *ReplicationTaskInfo) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type ReplicationTaskInfo from the protobuf v3 wire format
func (val *ReplicationTaskInfo) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *ReplicationTaskInfo) Size() int {
	return proto.Size(val)
}

// Equal returns whether two ReplicationTaskInfo values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *ReplicationTaskInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *ReplicationTaskInfo
	switch t := that.(type) {
	case *ReplicationTaskInfo:
		that1 = t
	case ReplicationTaskInfo:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type NamespaceTaskAttributes to the protobuf v3 wire format
func (val *NamespaceTaskAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type NamespaceTaskAttributes from the protobuf v3 wire format
func (val *NamespaceTaskAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *NamespaceTaskAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two NamespaceTaskAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *NamespaceTaskAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *NamespaceTaskAttributes
	switch t := that.(type) {
	case *NamespaceTaskAttributes:
		that1 = t
	case NamespaceTaskAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncShardStatusTaskAttributes to the protobuf v3 wire format
func (val *SyncShardStatusTaskAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncShardStatusTaskAttributes from the protobuf v3 wire format
func (val *SyncShardStatusTaskAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncShardStatusTaskAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncShardStatusTaskAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncShardStatusTaskAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncShardStatusTaskAttributes
	switch t := that.(type) {
	case *SyncShardStatusTaskAttributes:
		that1 = t
	case SyncShardStatusTaskAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncActivityTaskAttributes to the protobuf v3 wire format
func (val *SyncActivityTaskAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncActivityTaskAttributes from the protobuf v3 wire format
func (val *SyncActivityTaskAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncActivityTaskAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncActivityTaskAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncActivityTaskAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncActivityTaskAttributes
	switch t := that.(type) {
	case *SyncActivityTaskAttributes:
		that1 = t
	case SyncActivityTaskAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type HistoryTaskAttributes to the protobuf v3 wire format
func (val *HistoryTaskAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type HistoryTaskAttributes from the protobuf v3 wire format
func (val *HistoryTaskAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *HistoryTaskAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two HistoryTaskAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *HistoryTaskAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *HistoryTaskAttributes
	switch t := that.(type) {
	case *HistoryTaskAttributes:
		that1 = t
	case HistoryTaskAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type SyncWorkflowStateTaskAttributes to the protobuf v3 wire format
func (val *SyncWorkflowStateTaskAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type SyncWorkflowStateTaskAttributes from the protobuf v3 wire format
func (val *SyncWorkflowStateTaskAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *SyncWorkflowStateTaskAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two SyncWorkflowStateTaskAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *SyncWorkflowStateTaskAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *SyncWorkflowStateTaskAttributes
	switch t := that.(type) {
	case *SyncWorkflowStateTaskAttributes:
		that1 = t
	case SyncWorkflowStateTaskAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}

// Marshal an object of type TaskQueueUserDataAttributes to the protobuf v3 wire format
func (val *TaskQueueUserDataAttributes) Marshal() ([]byte, error) {
	return proto.Marshal(val)
}

// Unmarshal an object of type TaskQueueUserDataAttributes from the protobuf v3 wire format
func (val *TaskQueueUserDataAttributes) Unmarshal(buf []byte) error {
	return proto.Unmarshal(buf, val)
}

// Size returns the size of the object, in bytes, once serialized
func (val *TaskQueueUserDataAttributes) Size() int {
	return proto.Size(val)
}

// Equal returns whether two TaskQueueUserDataAttributes values are equivalent by recursively
// comparing the message's fields.
// For more information see the documentation for
// https://pkg.go.dev/google.golang.org/protobuf/proto#Equal
func (this *TaskQueueUserDataAttributes) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	var that1 *TaskQueueUserDataAttributes
	switch t := that.(type) {
	case *TaskQueueUserDataAttributes:
		that1 = t
	case TaskQueueUserDataAttributes:
		that1 = &t
	default:
		return false
	}

	return proto.Equal(this, that1)
}
