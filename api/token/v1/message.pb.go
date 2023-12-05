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
// versions:
// source: temporal/server/api/token/v1/message.proto

package token

import (
	reflect "reflect"
	sync "sync"

	v11 "go.temporal.io/server/api/clock/v1"
	v1 "go.temporal.io/server/api/history/v1"
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

type HistoryContinuation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	RunId                 string                        `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	FirstEventId          int64                         `protobuf:"varint,2,opt,name=first_event_id,json=firstEventId,proto3" json:"first_event_id,omitempty"`
	NextEventId           int64                         `protobuf:"varint,3,opt,name=next_event_id,json=nextEventId,proto3" json:"next_event_id,omitempty"`
	IsWorkflowRunning     bool                          `protobuf:"varint,5,opt,name=is_workflow_running,json=isWorkflowRunning,proto3" json:"is_workflow_running,omitempty"`
	PersistenceToken      []byte                        `protobuf:"bytes,6,opt,name=persistence_token,json=persistenceToken,proto3" json:"persistence_token,omitempty"`
	TransientWorkflowTask *v1.TransientWorkflowTaskInfo `protobuf:"bytes,7,opt,name=transient_workflow_task,json=transientWorkflowTask,proto3" json:"transient_workflow_task,omitempty"`
	BranchToken           []byte                        `protobuf:"bytes,8,opt,name=branch_token,json=branchToken,proto3" json:"branch_token,omitempty"`
	VersionHistoryItem    *v1.VersionHistoryItem        `protobuf:"bytes,10,opt,name=version_history_item,json=versionHistoryItem,proto3" json:"version_history_item,omitempty"`
}

func (x *HistoryContinuation) Reset() {
	*x = HistoryContinuation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryContinuation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryContinuation) ProtoMessage() {}

func (x *HistoryContinuation) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryContinuation.ProtoReflect.Descriptor instead.
func (*HistoryContinuation) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_token_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *HistoryContinuation) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *HistoryContinuation) GetFirstEventId() int64 {
	if x != nil {
		return x.FirstEventId
	}
	return 0
}

func (x *HistoryContinuation) GetNextEventId() int64 {
	if x != nil {
		return x.NextEventId
	}
	return 0
}

func (x *HistoryContinuation) GetIsWorkflowRunning() bool {
	if x != nil {
		return x.IsWorkflowRunning
	}
	return false
}

func (x *HistoryContinuation) GetPersistenceToken() []byte {
	if x != nil {
		return x.PersistenceToken
	}
	return nil
}

func (x *HistoryContinuation) GetTransientWorkflowTask() *v1.TransientWorkflowTaskInfo {
	if x != nil {
		return x.TransientWorkflowTask
	}
	return nil
}

func (x *HistoryContinuation) GetBranchToken() []byte {
	if x != nil {
		return x.BranchToken
	}
	return nil
}

func (x *HistoryContinuation) GetVersionHistoryItem() *v1.VersionHistoryItem {
	if x != nil {
		return x.VersionHistoryItem
	}
	return nil
}

type RawHistoryContinuation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId       string               `protobuf:"bytes,10,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	WorkflowId        string               `protobuf:"bytes,2,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	RunId             string               `protobuf:"bytes,3,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	StartEventId      int64                `protobuf:"varint,4,opt,name=start_event_id,json=startEventId,proto3" json:"start_event_id,omitempty"`
	StartEventVersion int64                `protobuf:"varint,5,opt,name=start_event_version,json=startEventVersion,proto3" json:"start_event_version,omitempty"`
	EndEventId        int64                `protobuf:"varint,6,opt,name=end_event_id,json=endEventId,proto3" json:"end_event_id,omitempty"`
	EndEventVersion   int64                `protobuf:"varint,7,opt,name=end_event_version,json=endEventVersion,proto3" json:"end_event_version,omitempty"`
	PersistenceToken  []byte               `protobuf:"bytes,8,opt,name=persistence_token,json=persistenceToken,proto3" json:"persistence_token,omitempty"`
	VersionHistories  *v1.VersionHistories `protobuf:"bytes,9,opt,name=version_histories,json=versionHistories,proto3" json:"version_histories,omitempty"`
}

func (x *RawHistoryContinuation) Reset() {
	*x = RawHistoryContinuation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RawHistoryContinuation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RawHistoryContinuation) ProtoMessage() {}

func (x *RawHistoryContinuation) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RawHistoryContinuation.ProtoReflect.Descriptor instead.
func (*RawHistoryContinuation) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_token_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *RawHistoryContinuation) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *RawHistoryContinuation) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

func (x *RawHistoryContinuation) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *RawHistoryContinuation) GetStartEventId() int64 {
	if x != nil {
		return x.StartEventId
	}
	return 0
}

func (x *RawHistoryContinuation) GetStartEventVersion() int64 {
	if x != nil {
		return x.StartEventVersion
	}
	return 0
}

func (x *RawHistoryContinuation) GetEndEventId() int64 {
	if x != nil {
		return x.EndEventId
	}
	return 0
}

func (x *RawHistoryContinuation) GetEndEventVersion() int64 {
	if x != nil {
		return x.EndEventVersion
	}
	return 0
}

func (x *RawHistoryContinuation) GetPersistenceToken() []byte {
	if x != nil {
		return x.PersistenceToken
	}
	return nil
}

func (x *RawHistoryContinuation) GetVersionHistories() *v1.VersionHistories {
	if x != nil {
		return x.VersionHistories
	}
	return nil
}

type Task struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId      string                 `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	WorkflowId       string                 `protobuf:"bytes,2,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	RunId            string                 `protobuf:"bytes,3,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	ScheduledEventId int64                  `protobuf:"varint,4,opt,name=scheduled_event_id,json=scheduledEventId,proto3" json:"scheduled_event_id,omitempty"`
	Attempt          int32                  `protobuf:"varint,5,opt,name=attempt,proto3" json:"attempt,omitempty"`
	ActivityId       string                 `protobuf:"bytes,6,opt,name=activity_id,json=activityId,proto3" json:"activity_id,omitempty"`
	WorkflowType     string                 `protobuf:"bytes,7,opt,name=workflow_type,json=workflowType,proto3" json:"workflow_type,omitempty"`
	ActivityType     string                 `protobuf:"bytes,8,opt,name=activity_type,json=activityType,proto3" json:"activity_type,omitempty"`
	Clock            *v11.VectorClock       `protobuf:"bytes,9,opt,name=clock,proto3" json:"clock,omitempty"`
	StartedEventId   int64                  `protobuf:"varint,10,opt,name=started_event_id,json=startedEventId,proto3" json:"started_event_id,omitempty"`
	Version          int64                  `protobuf:"varint,11,opt,name=version,proto3" json:"version,omitempty"`
	StartedTime      *timestamppb.Timestamp `protobuf:"bytes,12,opt,name=started_time,json=startedTime,proto3" json:"started_time,omitempty"`
}

func (x *Task) Reset() {
	*x = Task{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Task) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Task) ProtoMessage() {}

func (x *Task) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Task.ProtoReflect.Descriptor instead.
func (*Task) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_token_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *Task) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *Task) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

func (x *Task) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *Task) GetScheduledEventId() int64 {
	if x != nil {
		return x.ScheduledEventId
	}
	return 0
}

func (x *Task) GetAttempt() int32 {
	if x != nil {
		return x.Attempt
	}
	return 0
}

func (x *Task) GetActivityId() string {
	if x != nil {
		return x.ActivityId
	}
	return ""
}

func (x *Task) GetWorkflowType() string {
	if x != nil {
		return x.WorkflowType
	}
	return ""
}

func (x *Task) GetActivityType() string {
	if x != nil {
		return x.ActivityType
	}
	return ""
}

func (x *Task) GetClock() *v11.VectorClock {
	if x != nil {
		return x.Clock
	}
	return nil
}

func (x *Task) GetStartedEventId() int64 {
	if x != nil {
		return x.StartedEventId
	}
	return 0
}

func (x *Task) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *Task) GetStartedTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedTime
	}
	return nil
}

type QueryTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId string `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	TaskQueue   string `protobuf:"bytes,2,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
	TaskId      string `protobuf:"bytes,3,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
}

func (x *QueryTask) Reset() {
	*x = QueryTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryTask) ProtoMessage() {}

func (x *QueryTask) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryTask.ProtoReflect.Descriptor instead.
func (*QueryTask) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_token_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *QueryTask) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *QueryTask) GetTaskQueue() string {
	if x != nil {
		return x.TaskQueue
	}
	return ""
}

func (x *QueryTask) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

type NexusTask struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId string `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	TaskQueue   string `protobuf:"bytes,2,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
	TaskId      string `protobuf:"bytes,3,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
}

func (x *NexusTask) Reset() {
	*x = NexusTask{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NexusTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusTask) ProtoMessage() {}

func (x *NexusTask) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_token_v1_message_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusTask.ProtoReflect.Descriptor instead.
func (*NexusTask) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_token_v1_message_proto_rawDescGZIP(), []int{4}
}

func (x *NexusTask) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *NexusTask) GetTaskQueue() string {
	if x != nil {
		return x.TaskQueue
	}
	return ""
}

func (x *NexusTask) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

var File_temporal_server_api_token_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_token_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2a, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1c, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x2e, 0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2a, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2c, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x68, 0x69, 0x73,
	0x74, 0x6f, 0x72, 0x79, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xd5, 0x03, 0x0a, 0x13, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72,
	0x79, 0x43, 0x6f, 0x6e, 0x74, 0x69, 0x6e, 0x75, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x15, 0x0a,
	0x06, 0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72,
	0x75, 0x6e, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0e, 0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x66, 0x69,
	0x72, 0x73, 0x74, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0d, 0x6e, 0x65,
	0x78, 0x74, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x0b, 0x6e, 0x65, 0x78, 0x74, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2e,
	0x0a, 0x13, 0x69, 0x73, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x72, 0x75,
	0x6e, 0x6e, 0x69, 0x6e, 0x67, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x11, 0x69, 0x73, 0x57,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x75, 0x6e, 0x6e, 0x69, 0x6e, 0x67, 0x12, 0x2b,
	0x0a, 0x11, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x5f, 0x74, 0x6f,
	0x6b, 0x65, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x10, 0x70, 0x65, 0x72, 0x73, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x71, 0x0a, 0x17, 0x74,
	0x72, 0x61, 0x6e, 0x73, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x39, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x72,
	0x61, 0x6e, 0x73, 0x69, 0x65, 0x6e, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54,
	0x61, 0x73, 0x6b, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x15, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x65,
	0x6e, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x21,
	0x0a, 0x0c, 0x62, 0x72, 0x61, 0x6e, 0x63, 0x68, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x08,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x0b, 0x62, 0x72, 0x61, 0x6e, 0x63, 0x68, 0x54, 0x6f, 0x6b, 0x65,
	0x6e, 0x12, 0x64, 0x0a, 0x14, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x68, 0x69, 0x73,
	0x74, 0x6f, 0x72, 0x79, 0x5f, 0x69, 0x74, 0x65, 0x6d, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x32, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31,
	0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x49,
	0x74, 0x65, 0x6d, 0x52, 0x12, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74,
	0x6f, 0x72, 0x79, 0x49, 0x74, 0x65, 0x6d, 0x4a, 0x04, 0x08, 0x09, 0x10, 0x0a, 0x22, 0xa9, 0x03,
	0x0a, 0x16, 0x52, 0x61, 0x77, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x74,
	0x69, 0x6e, 0x75, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b,
	0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x1f, 0x0a, 0x0b, 0x77,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x12, 0x15, 0x0a, 0x06,
	0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72, 0x75,
	0x6e, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0e, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2e, 0x0a, 0x13, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x11, 0x73, 0x74, 0x61, 0x72, 0x74, 0x45, 0x76, 0x65,
	0x6e, 0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x20, 0x0a, 0x0c, 0x65, 0x6e, 0x64,
	0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0a, 0x65, 0x6e, 0x64, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x2a, 0x0a, 0x11, 0x65,
	0x6e, 0x64, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0f, 0x65, 0x6e, 0x64, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x2b, 0x0a, 0x11, 0x70, 0x65, 0x72, 0x73, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x10, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x54,
	0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x5d, 0x0a, 0x11, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f,
	0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x69, 0x65, 0x73, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x30, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31,
	0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x69, 0x65,
	0x73, 0x52, 0x10, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72,
	0x69, 0x65, 0x73, 0x4a, 0x04, 0x08, 0x01, 0x10, 0x02, 0x22, 0xd8, 0x03, 0x0a, 0x04, 0x54, 0x61,
	0x73, 0x6b, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x1f, 0x0a, 0x0b, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b,
	0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x12, 0x15, 0x0a, 0x06, 0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72, 0x75, 0x6e, 0x49, 0x64, 0x12, 0x2c, 0x0a,
	0x12, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x10, 0x73, 0x63, 0x68, 0x65, 0x64,
	0x75, 0x6c, 0x65, 0x64, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x61,
	0x74, 0x74, 0x65, 0x6d, 0x70, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x61, 0x74,
	0x74, 0x65, 0x6d, 0x70, 0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74,
	0x79, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x61, 0x63, 0x74, 0x69,
	0x76, 0x69, 0x74, 0x79, 0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c,
	0x6f, 0x77, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x77,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x79, 0x70, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x61,
	0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0c, 0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x3f, 0x0a, 0x05, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x29, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2e, 0x76, 0x31, 0x2e, 0x56,
	0x65, 0x63, 0x74, 0x6f, 0x72, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x05, 0x63, 0x6c, 0x6f, 0x63,
	0x6b, 0x12, 0x28, 0x0a, 0x10, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x5f, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0e, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x65, 0x64, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x76, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x3d, 0x0a, 0x0c, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x0c, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0b, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64,
	0x54, 0x69, 0x6d, 0x65, 0x22, 0x66, 0x0a, 0x09, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x61, 0x73,
	0x6b, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x22, 0x66, 0x0a, 0x09,
	0x4e, 0x65, 0x78, 0x75, 0x73, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a,
	0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x74,
	0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61,
	0x73, 0x6b, 0x49, 0x64, 0x42, 0x2a, 0x5a, 0x28, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70,
	0x69, 0x2f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x2f, 0x76, 0x31, 0x3b, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_token_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_token_v1_message_proto_rawDescData = file_temporal_server_api_token_v1_message_proto_rawDesc
)

func file_temporal_server_api_token_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_token_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_token_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_token_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_token_v1_message_proto_rawDescData
}

var file_temporal_server_api_token_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_temporal_server_api_token_v1_message_proto_goTypes = []interface{}{
	(*HistoryContinuation)(nil),          // 0: temporal.server.api.token.v1.HistoryContinuation
	(*RawHistoryContinuation)(nil),       // 1: temporal.server.api.token.v1.RawHistoryContinuation
	(*Task)(nil),                         // 2: temporal.server.api.token.v1.Task
	(*QueryTask)(nil),                    // 3: temporal.server.api.token.v1.QueryTask
	(*NexusTask)(nil),                    // 4: temporal.server.api.token.v1.NexusTask
	(*v1.TransientWorkflowTaskInfo)(nil), // 5: temporal.server.api.history.v1.TransientWorkflowTaskInfo
	(*v1.VersionHistoryItem)(nil),        // 6: temporal.server.api.history.v1.VersionHistoryItem
	(*v1.VersionHistories)(nil),          // 7: temporal.server.api.history.v1.VersionHistories
	(*v11.VectorClock)(nil),              // 8: temporal.server.api.clock.v1.VectorClock
	(*timestamppb.Timestamp)(nil),        // 9: google.protobuf.Timestamp
}
var file_temporal_server_api_token_v1_message_proto_depIdxs = []int32{
	5, // 0: temporal.server.api.token.v1.HistoryContinuation.transient_workflow_task:type_name -> temporal.server.api.history.v1.TransientWorkflowTaskInfo
	6, // 1: temporal.server.api.token.v1.HistoryContinuation.version_history_item:type_name -> temporal.server.api.history.v1.VersionHistoryItem
	7, // 2: temporal.server.api.token.v1.RawHistoryContinuation.version_histories:type_name -> temporal.server.api.history.v1.VersionHistories
	8, // 3: temporal.server.api.token.v1.Task.clock:type_name -> temporal.server.api.clock.v1.VectorClock
	9, // 4: temporal.server.api.token.v1.Task.started_time:type_name -> google.protobuf.Timestamp
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_temporal_server_api_token_v1_message_proto_init() }
func file_temporal_server_api_token_v1_message_proto_init() {
	if File_temporal_server_api_token_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_token_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryContinuation); i {
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
		file_temporal_server_api_token_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RawHistoryContinuation); i {
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
		file_temporal_server_api_token_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Task); i {
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
		file_temporal_server_api_token_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryTask); i {
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
		file_temporal_server_api_token_v1_message_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NexusTask); i {
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
			RawDescriptor: file_temporal_server_api_token_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_token_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_token_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_token_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_token_v1_message_proto = out.File
	file_temporal_server_api_token_v1_message_proto_rawDesc = nil
	file_temporal_server_api_token_v1_message_proto_goTypes = nil
	file_temporal_server_api_token_v1_message_proto_depIdxs = nil
}
