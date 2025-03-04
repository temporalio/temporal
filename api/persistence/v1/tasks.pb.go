// The MIT License
//
// Copyright (c) 2020 Temporal Technologies, Inc.
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// plugins:
// 	protoc-gen-go
// 	protoc
// source: temporal/server/api/persistence/v1/tasks.proto

package persistence

import (
	reflect "reflect"
	sync "sync"

	v12 "go.temporal.io/api/common/v1"
	v13 "go.temporal.io/api/enums/v1"
	v1 "go.temporal.io/server/api/clock/v1"
	v11 "go.temporal.io/server/api/taskqueue/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// task column
type AllocatedTaskInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data   *TaskInfo `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	TaskId int64     `protobuf:"varint,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
}

func (x *AllocatedTaskInfo) Reset() {
	*x = AllocatedTaskInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AllocatedTaskInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AllocatedTaskInfo) ProtoMessage() {}

func (x *AllocatedTaskInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AllocatedTaskInfo.ProtoReflect.Descriptor instead.
func (*AllocatedTaskInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{0}
}

func (x *AllocatedTaskInfo) GetData() *TaskInfo {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *AllocatedTaskInfo) GetTaskId() int64 {
	if x != nil {
		return x.TaskId
	}
	return 0
}

type TaskInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId      string                 `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	WorkflowId       string                 `protobuf:"bytes,2,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	RunId            string                 `protobuf:"bytes,3,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	ScheduledEventId int64                  `protobuf:"varint,4,opt,name=scheduled_event_id,json=scheduledEventId,proto3" json:"scheduled_event_id,omitempty"`
	CreateTime       *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	ExpiryTime       *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=expiry_time,json=expiryTime,proto3" json:"expiry_time,omitempty"`
	Clock            *v1.VectorClock        `protobuf:"bytes,7,opt,name=clock,proto3" json:"clock,omitempty"`
	// How this task should be directed. (Missing means the default for
	// TaskVersionDirective, which is unversioned.)
	VersionDirective *v11.TaskVersionDirective `protobuf:"bytes,8,opt,name=version_directive,json=versionDirective,proto3" json:"version_directive,omitempty"`
	// Stamp field allows to differentiate between different instances of the same task
	Stamp    int32         `protobuf:"varint,9,opt,name=stamp,proto3" json:"stamp,omitempty"`
	Priority *v12.Priority `protobuf:"bytes,10,opt,name=priority,proto3" json:"priority,omitempty"`
}

func (x *TaskInfo) Reset() {
	*x = TaskInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskInfo) ProtoMessage() {}

func (x *TaskInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskInfo.ProtoReflect.Descriptor instead.
func (*TaskInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{1}
}

func (x *TaskInfo) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *TaskInfo) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

func (x *TaskInfo) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *TaskInfo) GetScheduledEventId() int64 {
	if x != nil {
		return x.ScheduledEventId
	}
	return 0
}

func (x *TaskInfo) GetCreateTime() *timestamppb.Timestamp {
	if x != nil {
		return x.CreateTime
	}
	return nil
}

func (x *TaskInfo) GetExpiryTime() *timestamppb.Timestamp {
	if x != nil {
		return x.ExpiryTime
	}
	return nil
}

func (x *TaskInfo) GetClock() *v1.VectorClock {
	if x != nil {
		return x.Clock
	}
	return nil
}

func (x *TaskInfo) GetVersionDirective() *v11.TaskVersionDirective {
	if x != nil {
		return x.VersionDirective
	}
	return nil
}

func (x *TaskInfo) GetStamp() int32 {
	if x != nil {
		return x.Stamp
	}
	return 0
}

func (x *TaskInfo) GetPriority() *v12.Priority {
	if x != nil {
		return x.Priority
	}
	return nil
}

// task_queue column
type TaskQueueInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId string            `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	Name        string            `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	TaskType    v13.TaskQueueType `protobuf:"varint,3,opt,name=task_type,json=taskType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_type,omitempty"`
	Kind        v13.TaskQueueKind `protobuf:"varint,4,opt,name=kind,proto3,enum=temporal.api.enums.v1.TaskQueueKind" json:"kind,omitempty"`
	// After data is migrated into subqueues, this contains a copy of the ack level for subqueue 0.
	AckLevel       int64                  `protobuf:"varint,5,opt,name=ack_level,json=ackLevel,proto3" json:"ack_level,omitempty"`
	ExpiryTime     *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=expiry_time,json=expiryTime,proto3" json:"expiry_time,omitempty"`
	LastUpdateTime *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=last_update_time,json=lastUpdateTime,proto3" json:"last_update_time,omitempty"`
	// After data is migrated into subqueues, this contains a copy of the count for subqueue 0.
	ApproximateBacklogCount int64 `protobuf:"varint,8,opt,name=approximate_backlog_count,json=approximateBacklogCount,proto3" json:"approximate_backlog_count,omitempty"`
	// Subqueues contains one entry for each subqueue in this physical task queue.
	// Tasks are split into subqueues to implement priority and fairness.
	// Subqueues are indexed starting from 0, the zero subqueue is always present
	// and corresponds to the "main" queue before subqueues were introduced.
	//
	// The message at index n describes the subqueue at index n.
	//
	// Each subqueue has its own ack level and approx backlog count, but they share
	// the range id. For compatibility, ack level and backlog count for subqueue 0
	// is copied into TaskQueueInfo.
	Subqueues []*SubqueueInfo `protobuf:"bytes,9,rep,name=subqueues,proto3" json:"subqueues,omitempty"`
}

func (x *TaskQueueInfo) Reset() {
	*x = TaskQueueInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueueInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueueInfo) ProtoMessage() {}

func (x *TaskQueueInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskQueueInfo.ProtoReflect.Descriptor instead.
func (*TaskQueueInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{2}
}

func (x *TaskQueueInfo) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *TaskQueueInfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TaskQueueInfo) GetTaskType() v13.TaskQueueType {
	if x != nil {
		return x.TaskType
	}
	return v13.TaskQueueType(0)
}

func (x *TaskQueueInfo) GetKind() v13.TaskQueueKind {
	if x != nil {
		return x.Kind
	}
	return v13.TaskQueueKind(0)
}

func (x *TaskQueueInfo) GetAckLevel() int64 {
	if x != nil {
		return x.AckLevel
	}
	return 0
}

func (x *TaskQueueInfo) GetExpiryTime() *timestamppb.Timestamp {
	if x != nil {
		return x.ExpiryTime
	}
	return nil
}

func (x *TaskQueueInfo) GetLastUpdateTime() *timestamppb.Timestamp {
	if x != nil {
		return x.LastUpdateTime
	}
	return nil
}

func (x *TaskQueueInfo) GetApproximateBacklogCount() int64 {
	if x != nil {
		return x.ApproximateBacklogCount
	}
	return 0
}

func (x *TaskQueueInfo) GetSubqueues() []*SubqueueInfo {
	if x != nil {
		return x.Subqueues
	}
	return nil
}

type SubqueueInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Key is the information used by a splitting algorithm to decide which tasks should go in
	// this subqueue. It should not change after being registered in TaskQueueInfo.
	Key *SubqueueKey `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The rest are mutable state for the subqueue:
	AckLevel                int64 `protobuf:"varint,2,opt,name=ack_level,json=ackLevel,proto3" json:"ack_level,omitempty"`
	ApproximateBacklogCount int64 `protobuf:"varint,3,opt,name=approximate_backlog_count,json=approximateBacklogCount,proto3" json:"approximate_backlog_count,omitempty"`
}

func (x *SubqueueInfo) Reset() {
	*x = SubqueueInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubqueueInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubqueueInfo) ProtoMessage() {}

func (x *SubqueueInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubqueueInfo.ProtoReflect.Descriptor instead.
func (*SubqueueInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{3}
}

func (x *SubqueueInfo) GetKey() *SubqueueKey {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *SubqueueInfo) GetAckLevel() int64 {
	if x != nil {
		return x.AckLevel
	}
	return 0
}

func (x *SubqueueInfo) GetApproximateBacklogCount() int64 {
	if x != nil {
		return x.ApproximateBacklogCount
	}
	return 0
}

type SubqueueKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Each subqueue contains tasks from only one priority level.
	Priority int32 `protobuf:"varint,1,opt,name=priority,proto3" json:"priority,omitempty"`
}

func (x *SubqueueKey) Reset() {
	*x = SubqueueKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubqueueKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubqueueKey) ProtoMessage() {}

func (x *SubqueueKey) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubqueueKey.ProtoReflect.Descriptor instead.
func (*SubqueueKey) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{4}
}

func (x *SubqueueKey) GetPriority() int32 {
	if x != nil {
		return x.Priority
	}
	return 0
}

type TaskKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FireTime *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=fire_time,json=fireTime,proto3" json:"fire_time,omitempty"`
	TaskId   int64                  `protobuf:"varint,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
}

func (x *TaskKey) Reset() {
	*x = TaskKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskKey) ProtoMessage() {}

func (x *TaskKey) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskKey.ProtoReflect.Descriptor instead.
func (*TaskKey) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP(), []int{5}
}

func (x *TaskKey) GetFireTime() *timestamppb.Timestamp {
	if x != nil {
		return x.FireTime
	}
	return nil
}

func (x *TaskKey) GetTaskId() int64 {
	if x != nil {
		return x.TaskId
	}
	return 0
}

var File_temporal_server_api_persistence_v1_tasks_proto protoreflect.FileDescriptor

var file_temporal_server_api_persistence_v1_tasks_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2f,
	0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x22, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65,
	0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2a, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6c, 0x6f, 0x63,
	0x6b, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x76,
	0x0a, 0x11, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x65, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x6e,
	0x66, 0x6f, 0x12, 0x44, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x2c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x1b, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x22,
	0xaf, 0x04, 0x0a, 0x08, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x25, 0x0a, 0x0c, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00,
	0x12, 0x23, 0x0a, 0x0b, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x19, 0x0a, 0x06, 0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x05, 0x72, 0x75, 0x6e, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x30, 0x0a,
	0x12, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f,
	0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x10, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c,
	0x65, 0x64, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3f, 0x0a, 0x0b,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x63, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3f, 0x0a, 0x0b, 0x65, 0x78, 0x70,
	0x69, 0x72, 0x79, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x79, 0x54,
	0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x43, 0x0a, 0x05, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x18,
	0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2e,
	0x76, 0x31, 0x2e, 0x56, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x05, 0x63,
	0x6c, 0x6f, 0x63, 0x6b, 0x42, 0x02, 0x68, 0x00, 0x12, 0x67, 0x0a, 0x11, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x5f, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x18, 0x08, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x36, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x44, 0x69, 0x72,
	0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x52, 0x10, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x44, 0x69,
	0x72, 0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x18, 0x0a, 0x05, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x18, 0x09, 0x20, 0x01, 0x28, 0x05, 0x52, 0x05, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x40, 0x0a, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x18, 0x0a,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x72, 0x69, 0x6f,
	0x72, 0x69, 0x74, 0x79, 0x52, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79, 0x42, 0x02,
	0x68, 0x00, 0x22, 0x93, 0x04, 0x0a, 0x0d, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49,
	0x6e, 0x66, 0x6f, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x16, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x45,
	0x0a, 0x09, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x24, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65,
	0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x08, 0x74, 0x61, 0x73, 0x6b, 0x54, 0x79, 0x70, 0x65, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x3c, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x24,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75,
	0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x4b, 0x69,
	0x6e, 0x64, 0x52, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1f, 0x0a, 0x09, 0x61,
	0x63, 0x6b, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x61,
	0x63, 0x6b, 0x4c, 0x65, 0x76, 0x65, 0x6c, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3f, 0x0a, 0x0b, 0x65, 0x78,
	0x70, 0x69, 0x72, 0x79, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x65, 0x78, 0x70, 0x69, 0x72, 0x79,
	0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x48, 0x0a, 0x10, 0x6c, 0x61, 0x73, 0x74, 0x5f,
	0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0e, 0x6c, 0x61, 0x73, 0x74,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3e, 0x0a,
	0x19, 0x61, 0x70, 0x70, 0x72, 0x6f, 0x78, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x5f, 0x62, 0x61, 0x63, 0x6b,
	0x6c, 0x6f, 0x67, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x17,
	0x61, 0x70, 0x70, 0x72, 0x6f, 0x78, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x6c, 0x6f,
	0x67, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x12, 0x52, 0x0a, 0x09, 0x73, 0x75, 0x62,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x53, 0x75, 0x62, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x09, 0x73, 0x75, 0x62,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x22, 0xb6, 0x01, 0x0a, 0x0c, 0x53, 0x75,
	0x62, 0x71, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x45, 0x0a, 0x03, 0x6b, 0x65, 0x79,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x75, 0x62, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x4b, 0x65, 0x79, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1f, 0x0a, 0x09,
	0x61, 0x63, 0x6b, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08,
	0x61, 0x63, 0x6b, 0x4c, 0x65, 0x76, 0x65, 0x6c, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3e, 0x0a, 0x19, 0x61,
	0x70, 0x70, 0x72, 0x6f, 0x78, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x5f, 0x62, 0x61, 0x63, 0x6b, 0x6c, 0x6f,
	0x67, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x17, 0x61, 0x70,
	0x70, 0x72, 0x6f, 0x78, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x6c, 0x6f, 0x67, 0x43,
	0x6f, 0x75, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x22, 0x2d, 0x0a, 0x0b, 0x53, 0x75, 0x62, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x4b, 0x65, 0x79, 0x12, 0x1e, 0x0a, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74,
	0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x70, 0x72, 0x69, 0x6f, 0x72, 0x69, 0x74, 0x79,
	0x42, 0x02, 0x68, 0x00, 0x22, 0x63, 0x0a, 0x07, 0x54, 0x61, 0x73, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x3b,
	0x0a, 0x09, 0x66, 0x69, 0x72, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x08, 0x66, 0x69, 0x72, 0x65,
	0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1b, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x42,
	0x02, 0x68, 0x00, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70,
	0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2f, 0x76, 0x31, 0x3b, 0x70, 0x65, 0x72,
	0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_persistence_v1_tasks_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_tasks_proto_rawDescData = file_temporal_server_api_persistence_v1_tasks_proto_rawDesc
)

func file_temporal_server_api_persistence_v1_tasks_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_tasks_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_tasks_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_persistence_v1_tasks_proto_rawDescData)
	})
	return file_temporal_server_api_persistence_v1_tasks_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_tasks_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_temporal_server_api_persistence_v1_tasks_proto_goTypes = []interface{}{
	(*AllocatedTaskInfo)(nil),        // 0: temporal.server.api.persistence.v1.AllocatedTaskInfo
	(*TaskInfo)(nil),                 // 1: temporal.server.api.persistence.v1.TaskInfo
	(*TaskQueueInfo)(nil),            // 2: temporal.server.api.persistence.v1.TaskQueueInfo
	(*SubqueueInfo)(nil),             // 3: temporal.server.api.persistence.v1.SubqueueInfo
	(*SubqueueKey)(nil),              // 4: temporal.server.api.persistence.v1.SubqueueKey
	(*TaskKey)(nil),                  // 5: temporal.server.api.persistence.v1.TaskKey
	(*timestamppb.Timestamp)(nil),    // 6: google.protobuf.Timestamp
	(*v1.VectorClock)(nil),           // 7: temporal.server.api.clock.v1.VectorClock
	(*v11.TaskVersionDirective)(nil), // 8: temporal.server.api.taskqueue.v1.TaskVersionDirective
	(*v12.Priority)(nil),             // 9: temporal.api.common.v1.Priority
	(v13.TaskQueueType)(0),           // 10: temporal.api.enums.v1.TaskQueueType
	(v13.TaskQueueKind)(0),           // 11: temporal.api.enums.v1.TaskQueueKind
}
var file_temporal_server_api_persistence_v1_tasks_proto_depIdxs = []int32{
	1,  // 0: temporal.server.api.persistence.v1.AllocatedTaskInfo.data:type_name -> temporal.server.api.persistence.v1.TaskInfo
	6,  // 1: temporal.server.api.persistence.v1.TaskInfo.create_time:type_name -> google.protobuf.Timestamp
	6,  // 2: temporal.server.api.persistence.v1.TaskInfo.expiry_time:type_name -> google.protobuf.Timestamp
	7,  // 3: temporal.server.api.persistence.v1.TaskInfo.clock:type_name -> temporal.server.api.clock.v1.VectorClock
	8,  // 4: temporal.server.api.persistence.v1.TaskInfo.version_directive:type_name -> temporal.server.api.taskqueue.v1.TaskVersionDirective
	9,  // 5: temporal.server.api.persistence.v1.TaskInfo.priority:type_name -> temporal.api.common.v1.Priority
	10, // 6: temporal.server.api.persistence.v1.TaskQueueInfo.task_type:type_name -> temporal.api.enums.v1.TaskQueueType
	11, // 7: temporal.server.api.persistence.v1.TaskQueueInfo.kind:type_name -> temporal.api.enums.v1.TaskQueueKind
	6,  // 8: temporal.server.api.persistence.v1.TaskQueueInfo.expiry_time:type_name -> google.protobuf.Timestamp
	6,  // 9: temporal.server.api.persistence.v1.TaskQueueInfo.last_update_time:type_name -> google.protobuf.Timestamp
	3,  // 10: temporal.server.api.persistence.v1.TaskQueueInfo.subqueues:type_name -> temporal.server.api.persistence.v1.SubqueueInfo
	4,  // 11: temporal.server.api.persistence.v1.SubqueueInfo.key:type_name -> temporal.server.api.persistence.v1.SubqueueKey
	6,  // 12: temporal.server.api.persistence.v1.TaskKey.fire_time:type_name -> google.protobuf.Timestamp
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_tasks_proto_init() }
func file_temporal_server_api_persistence_v1_tasks_proto_init() {
	if File_temporal_server_api_persistence_v1_tasks_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AllocatedTaskInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskQueueInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SubqueueInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SubqueueKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_temporal_server_api_persistence_v1_tasks_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_persistence_v1_tasks_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_tasks_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_tasks_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_tasks_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_tasks_proto = out.File
	file_temporal_server_api_persistence_v1_tasks_proto_rawDesc = nil
	file_temporal_server_api_persistence_v1_tasks_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_tasks_proto_depIdxs = nil
}
