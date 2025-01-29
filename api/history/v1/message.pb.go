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
// source: temporal/server/api/history/v1/message.proto

package history

import (
	reflect "reflect"
	sync "sync"

	v11 "go.temporal.io/api/common/v1"
	v1 "go.temporal.io/api/history/v1"
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

type TransientWorkflowTaskInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// A list of history events that are to be appended to the "real" workflow history.
	HistorySuffix []*v1.HistoryEvent `protobuf:"bytes,3,rep,name=history_suffix,json=historySuffix,proto3" json:"history_suffix,omitempty"`
}

func (x *TransientWorkflowTaskInfo) Reset() {
	*x = TransientWorkflowTaskInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TransientWorkflowTaskInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransientWorkflowTaskInfo) ProtoMessage() {}

func (x *TransientWorkflowTaskInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TransientWorkflowTaskInfo.ProtoReflect.Descriptor instead.
func (*TransientWorkflowTaskInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *TransientWorkflowTaskInfo) GetHistorySuffix() []*v1.HistoryEvent {
	if x != nil {
		return x.HistorySuffix
	}
	return nil
}

// VersionHistoryItem contains signal eventId and the corresponding version.
type VersionHistoryItem struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EventId int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	Version int64 `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
}

func (x *VersionHistoryItem) Reset() {
	*x = VersionHistoryItem{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionHistoryItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionHistoryItem) ProtoMessage() {}

func (x *VersionHistoryItem) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionHistoryItem.ProtoReflect.Descriptor instead.
func (*VersionHistoryItem) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *VersionHistoryItem) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *VersionHistoryItem) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

// VersionHistory contains the version history of a branch.
type VersionHistory struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BranchToken []byte                `protobuf:"bytes,1,opt,name=branch_token,json=branchToken,proto3" json:"branch_token,omitempty"`
	Items       []*VersionHistoryItem `protobuf:"bytes,2,rep,name=items,proto3" json:"items,omitempty"`
}

func (x *VersionHistory) Reset() {
	*x = VersionHistory{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionHistory) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionHistory) ProtoMessage() {}

func (x *VersionHistory) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionHistory.ProtoReflect.Descriptor instead.
func (*VersionHistory) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *VersionHistory) GetBranchToken() []byte {
	if x != nil {
		return x.BranchToken
	}
	return nil
}

func (x *VersionHistory) GetItems() []*VersionHistoryItem {
	if x != nil {
		return x.Items
	}
	return nil
}

// VersionHistories contains all version histories from all branches.
type VersionHistories struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CurrentVersionHistoryIndex int32             `protobuf:"varint,1,opt,name=current_version_history_index,json=currentVersionHistoryIndex,proto3" json:"current_version_history_index,omitempty"`
	Histories                  []*VersionHistory `protobuf:"bytes,2,rep,name=histories,proto3" json:"histories,omitempty"`
}

func (x *VersionHistories) Reset() {
	*x = VersionHistories{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VersionHistories) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VersionHistories) ProtoMessage() {}

func (x *VersionHistories) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VersionHistories.ProtoReflect.Descriptor instead.
func (*VersionHistories) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *VersionHistories) GetCurrentVersionHistoryIndex() int32 {
	if x != nil {
		return x.CurrentVersionHistoryIndex
	}
	return 0
}

func (x *VersionHistories) GetHistories() []*VersionHistory {
	if x != nil {
		return x.Histories
	}
	return nil
}

type TaskKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId   int64                  `protobuf:"varint,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	FireTime *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=fire_time,json=fireTime,proto3" json:"fire_time,omitempty"`
}

func (x *TaskKey) Reset() {
	*x = TaskKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskKey) ProtoMessage() {}

func (x *TaskKey) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[4]
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
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{4}
}

func (x *TaskKey) GetTaskId() int64 {
	if x != nil {
		return x.TaskId
	}
	return 0
}

func (x *TaskKey) GetFireTime() *timestamppb.Timestamp {
	if x != nil {
		return x.FireTime
	}
	return nil
}

type TaskRange struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	InclusiveMinTaskKey *TaskKey `protobuf:"bytes,1,opt,name=inclusive_min_task_key,json=inclusiveMinTaskKey,proto3" json:"inclusive_min_task_key,omitempty"`
	ExclusiveMaxTaskKey *TaskKey `protobuf:"bytes,2,opt,name=exclusive_max_task_key,json=exclusiveMaxTaskKey,proto3" json:"exclusive_max_task_key,omitempty"`
}

func (x *TaskRange) Reset() {
	*x = TaskRange{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskRange) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskRange) ProtoMessage() {}

func (x *TaskRange) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskRange.ProtoReflect.Descriptor instead.
func (*TaskRange) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{5}
}

func (x *TaskRange) GetInclusiveMinTaskKey() *TaskKey {
	if x != nil {
		return x.InclusiveMinTaskKey
	}
	return nil
}

func (x *TaskRange) GetExclusiveMaxTaskKey() *TaskKey {
	if x != nil {
		return x.ExclusiveMaxTaskKey
	}
	return nil
}

type StrippedHistoryEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EventId int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	Version int64 `protobuf:"varint,4,opt,name=version,proto3" json:"version,omitempty"`
}

func (x *StrippedHistoryEvent) Reset() {
	*x = StrippedHistoryEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StrippedHistoryEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StrippedHistoryEvent) ProtoMessage() {}

func (x *StrippedHistoryEvent) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StrippedHistoryEvent.ProtoReflect.Descriptor instead.
func (*StrippedHistoryEvent) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{6}
}

func (x *StrippedHistoryEvent) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *StrippedHistoryEvent) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

type StrippedHistoryEvents struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Events []*StrippedHistoryEvent `protobuf:"bytes,1,rep,name=events,proto3" json:"events,omitempty"`
}

func (x *StrippedHistoryEvents) Reset() {
	*x = StrippedHistoryEvents{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StrippedHistoryEvents) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StrippedHistoryEvents) ProtoMessage() {}

func (x *StrippedHistoryEvents) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StrippedHistoryEvents.ProtoReflect.Descriptor instead.
func (*StrippedHistoryEvents) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{7}
}

func (x *StrippedHistoryEvents) GetEvents() []*StrippedHistoryEvent {
	if x != nil {
		return x.Events
	}
	return nil
}

// api-linter: core::0158::response-next-page-token-field=disabled
// api-linter: core::0158::response-repeated-first-field=disabled
// api-linter: core::0158::response-plural-first-field=disabled
// GetWorkflowExecutionHistoryResponse is the response to GetWorkflowExecutionHistory call which returns raw bytes of
// history events. Frontend service deserialize the response as HistoryEvents as this field is byte compatible with
// the serialized form of HistoryEvents.
type GetWorkflowExecutionHistoryResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	History [][]byte `protobuf:"bytes,1,rep,name=history,proto3" json:"history,omitempty"`
	// Raw history is an alternate representation of history that may be returned if configured on
	// the frontend. This is not supported by all SDKs. Either this or `history` will be set.
	RawHistory []*v11.DataBlob `protobuf:"bytes,2,rep,name=raw_history,json=rawHistory,proto3" json:"raw_history,omitempty"`
	// Will be set if there are more history events than were included in this response
	NextPageToken []byte `protobuf:"bytes,3,opt,name=next_page_token,json=nextPageToken,proto3" json:"next_page_token,omitempty"`
	Archived      bool   `protobuf:"varint,4,opt,name=archived,proto3" json:"archived,omitempty"`
}

func (x *GetWorkflowExecutionHistoryResponse) Reset() {
	*x = GetWorkflowExecutionHistoryResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetWorkflowExecutionHistoryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetWorkflowExecutionHistoryResponse) ProtoMessage() {}

func (x *GetWorkflowExecutionHistoryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_history_v1_message_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetWorkflowExecutionHistoryResponse.ProtoReflect.Descriptor instead.
func (*GetWorkflowExecutionHistoryResponse) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_history_v1_message_proto_rawDescGZIP(), []int{8}
}

func (x *GetWorkflowExecutionHistoryResponse) GetHistory() [][]byte {
	if x != nil {
		return x.History
	}
	return nil
}

func (x *GetWorkflowExecutionHistoryResponse) GetRawHistory() []*v11.DataBlob {
	if x != nil {
		return x.RawHistory
	}
	return nil
}

func (x *GetWorkflowExecutionHistoryResponse) GetNextPageToken() []byte {
	if x != nil {
		return x.NextPageToken
	}
	return nil
}

func (x *GetWorkflowExecutionHistoryResponse) GetArchived() bool {
	if x != nil {
		return x.Archived
	}
	return false
}

var File_temporal_server_api_history_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_history_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2c, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2f, 0x76, 0x31, 0x2f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x25, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x79, 0x0a, 0x19, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x65, 0x6e, 0x74, 0x57,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x50,
	0x0a, 0x0e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x5f, 0x73, 0x75, 0x66, 0x66, 0x69, 0x78, 0x18,
	0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x69,
	0x73, 0x74, 0x6f, 0x72, 0x79, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x52, 0x0d, 0x68, 0x69, 0x73, 0x74, 0x6f,
	0x72, 0x79, 0x53, 0x75, 0x66, 0x66, 0x69, 0x78, 0x42, 0x02, 0x68, 0x00, 0x4a, 0x04, 0x08, 0x01,
	0x10, 0x02, 0x4a, 0x04, 0x08, 0x02, 0x10, 0x03, 0x22, 0x51, 0x0a, 0x12, 0x56, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x49, 0x74, 0x65, 0x6d, 0x12, 0x1d, 0x0a, 0x08,
	0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1c, 0x0a, 0x07, 0x76, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x22, 0x85, 0x01, 0x0a, 0x0e, 0x56, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x25, 0x0a, 0x0c, 0x62, 0x72, 0x61, 0x6e,
	0x63, 0x68, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0b, 0x62,
	0x72, 0x61, 0x6e, 0x63, 0x68, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4c, 0x0a,
	0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x32, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x05, 0x69,
	0x74, 0x65, 0x6d, 0x73, 0x42, 0x02, 0x68, 0x00, 0x22, 0xab, 0x01, 0x0a, 0x10, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x69, 0x65, 0x73, 0x12, 0x45, 0x0a, 0x1d, 0x63,
	0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x68, 0x69,
	0x73, 0x74, 0x6f, 0x72, 0x79, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x1a, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x42, 0x02, 0x68, 0x00, 0x12,
	0x50, 0x0a, 0x09, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x69, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x2e, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31,
	0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x52, 0x09,
	0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x69, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x22, 0x63, 0x0a,
	0x07, 0x54, 0x61, 0x73, 0x6b, 0x4b, 0x65, 0x79, 0x12, 0x1b, 0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x3b, 0x0a, 0x09, 0x66, 0x69, 0x72, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x08, 0x66, 0x69, 0x72, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x22, 0xcf, 0x01,
	0x0a, 0x09, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x60, 0x0a, 0x16, 0x69, 0x6e,
	0x63, 0x6c, 0x75, 0x73, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x69, 0x6e, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x27, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69,
	0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x4b, 0x65, 0x79, 0x52,
	0x13, 0x69, 0x6e, 0x63, 0x6c, 0x75, 0x73, 0x69, 0x76, 0x65, 0x4d, 0x69, 0x6e, 0x54, 0x61, 0x73,
	0x6b, 0x4b, 0x65, 0x79, 0x42, 0x02, 0x68, 0x00, 0x12, 0x60, 0x0a, 0x16, 0x65, 0x78, 0x63, 0x6c, 0x75,
	0x73, 0x69, 0x76, 0x65, 0x5f, 0x6d, 0x61, 0x78, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x6b, 0x65, 0x79,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x27, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f,
	0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x4b, 0x65, 0x79, 0x52, 0x13, 0x65, 0x78,
	0x63, 0x6c, 0x75, 0x73, 0x69, 0x76, 0x65, 0x4d, 0x61, 0x78, 0x54, 0x61, 0x73, 0x6b, 0x4b, 0x65,
	0x79, 0x42, 0x02, 0x68, 0x00, 0x22, 0x53, 0x0a, 0x14, 0x53, 0x74, 0x72, 0x69, 0x70, 0x70, 0x65, 0x64,
	0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x1d, 0x0a, 0x08, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1c, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x22, 0x69, 0x0a, 0x15, 0x53, 0x74, 0x72, 0x69, 0x70, 0x70, 0x65,
	0x64, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x50, 0x0a,
	0x06, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x34, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x74, 0x72, 0x69,
	0x70, 0x70, 0x65, 0x64, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x52, 0x06, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x42, 0x02, 0x68, 0x00, 0x22, 0xd6, 0x01, 0x0a, 0x23,
	0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x1c, 0x0a, 0x07, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0c, 0x52, 0x07, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x42, 0x02, 0x68, 0x00, 0x12, 0x45, 0x0a,
	0x0b, 0x72, 0x61, 0x77, 0x5f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x42, 0x6c,
	0x6f, 0x62, 0x52, 0x0a, 0x72, 0x61, 0x77, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x2a, 0x0a, 0x0f, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x6f,
	0x6b, 0x65, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0d, 0x6e, 0x65, 0x78, 0x74, 0x50, 0x61,
	0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1e, 0x0a, 0x08, 0x61,
	0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x61, 0x72,
	0x63, 0x68, 0x69, 0x76, 0x65, 0x64, 0x42, 0x02, 0x68, 0x00, 0x42, 0x2e, 0x5a, 0x2c, 0x67, 0x6f, 0x2e,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2f, 0x76, 0x31, 0x3b,
	0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_history_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_history_v1_message_proto_rawDescData = file_temporal_server_api_history_v1_message_proto_rawDesc
)

func file_temporal_server_api_history_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_history_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_history_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_history_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_history_v1_message_proto_rawDescData
}

var file_temporal_server_api_history_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_temporal_server_api_history_v1_message_proto_goTypes = []interface{}{
	(*TransientWorkflowTaskInfo)(nil),           // 0: temporal.server.api.history.v1.TransientWorkflowTaskInfo
	(*VersionHistoryItem)(nil),                  // 1: temporal.server.api.history.v1.VersionHistoryItem
	(*VersionHistory)(nil),                      // 2: temporal.server.api.history.v1.VersionHistory
	(*VersionHistories)(nil),                    // 3: temporal.server.api.history.v1.VersionHistories
	(*TaskKey)(nil),                             // 4: temporal.server.api.history.v1.TaskKey
	(*TaskRange)(nil),                           // 5: temporal.server.api.history.v1.TaskRange
	(*StrippedHistoryEvent)(nil),                // 6: temporal.server.api.history.v1.StrippedHistoryEvent
	(*StrippedHistoryEvents)(nil),               // 7: temporal.server.api.history.v1.StrippedHistoryEvents
	(*GetWorkflowExecutionHistoryResponse)(nil), // 8: temporal.server.api.history.v1.GetWorkflowExecutionHistoryResponse
	(*v1.HistoryEvent)(nil),                     // 9: temporal.api.history.v1.HistoryEvent
	(*timestamppb.Timestamp)(nil),               // 10: google.protobuf.Timestamp
	(*v11.DataBlob)(nil),                        // 11: temporal.api.common.v1.DataBlob
}
var file_temporal_server_api_history_v1_message_proto_depIdxs = []int32{
	9,  // 0: temporal.server.api.history.v1.TransientWorkflowTaskInfo.history_suffix:type_name -> temporal.api.history.v1.HistoryEvent
	1,  // 1: temporal.server.api.history.v1.VersionHistory.items:type_name -> temporal.server.api.history.v1.VersionHistoryItem
	2,  // 2: temporal.server.api.history.v1.VersionHistories.histories:type_name -> temporal.server.api.history.v1.VersionHistory
	10, // 3: temporal.server.api.history.v1.TaskKey.fire_time:type_name -> google.protobuf.Timestamp
	4,  // 4: temporal.server.api.history.v1.TaskRange.inclusive_min_task_key:type_name -> temporal.server.api.history.v1.TaskKey
	4,  // 5: temporal.server.api.history.v1.TaskRange.exclusive_max_task_key:type_name -> temporal.server.api.history.v1.TaskKey
	6,  // 6: temporal.server.api.history.v1.StrippedHistoryEvents.events:type_name -> temporal.server.api.history.v1.StrippedHistoryEvent
	11, // 7: temporal.server.api.history.v1.GetWorkflowExecutionHistoryResponse.raw_history:type_name -> temporal.api.common.v1.DataBlob
	8,  // [8:8] is the sub-list for method output_type
	8,  // [8:8] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
}

func init() { file_temporal_server_api_history_v1_message_proto_init() }
func file_temporal_server_api_history_v1_message_proto_init() {
	if File_temporal_server_api_history_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_history_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TransientWorkflowTaskInfo); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionHistoryItem); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionHistory); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VersionHistories); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskRange); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StrippedHistoryEvent); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StrippedHistoryEvents); i {
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
		file_temporal_server_api_history_v1_message_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetWorkflowExecutionHistoryResponse); i {
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
			RawDescriptor: file_temporal_server_api_history_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_history_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_history_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_history_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_history_v1_message_proto = out.File
	file_temporal_server_api_history_v1_message_proto_rawDesc = nil
	file_temporal_server_api_history_v1_message_proto_goTypes = nil
	file_temporal_server_api_history_v1_message_proto_depIdxs = nil
}
