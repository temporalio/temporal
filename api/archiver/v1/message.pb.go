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
// source: temporal/server/api/archiver/v1/message.proto

package archiver

import (
	reflect "reflect"
	sync "sync"

	v12 "go.temporal.io/api/common/v1"
	v11 "go.temporal.io/api/enums/v1"
	v1 "go.temporal.io/api/history/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type HistoryBlobHeader struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Namespace            string `protobuf:"bytes,1,opt,name=namespace,proto3" json:"namespace,omitempty"`
	NamespaceId          string `protobuf:"bytes,2,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	WorkflowId           string `protobuf:"bytes,3,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	RunId                string `protobuf:"bytes,4,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	IsLast               bool   `protobuf:"varint,5,opt,name=is_last,json=isLast,proto3" json:"is_last,omitempty"`
	FirstFailoverVersion int64  `protobuf:"varint,6,opt,name=first_failover_version,json=firstFailoverVersion,proto3" json:"first_failover_version,omitempty"`
	LastFailoverVersion  int64  `protobuf:"varint,7,opt,name=last_failover_version,json=lastFailoverVersion,proto3" json:"last_failover_version,omitempty"`
	FirstEventId         int64  `protobuf:"varint,8,opt,name=first_event_id,json=firstEventId,proto3" json:"first_event_id,omitempty"`
	LastEventId          int64  `protobuf:"varint,9,opt,name=last_event_id,json=lastEventId,proto3" json:"last_event_id,omitempty"`
	EventCount           int64  `protobuf:"varint,10,opt,name=event_count,json=eventCount,proto3" json:"event_count,omitempty"`
}

func (x *HistoryBlobHeader) Reset() {
	*x = HistoryBlobHeader{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryBlobHeader) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryBlobHeader) ProtoMessage() {}

func (x *HistoryBlobHeader) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryBlobHeader.ProtoReflect.Descriptor instead.
func (*HistoryBlobHeader) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_archiver_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *HistoryBlobHeader) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *HistoryBlobHeader) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *HistoryBlobHeader) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

func (x *HistoryBlobHeader) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *HistoryBlobHeader) GetIsLast() bool {
	if x != nil {
		return x.IsLast
	}
	return false
}

func (x *HistoryBlobHeader) GetFirstFailoverVersion() int64 {
	if x != nil {
		return x.FirstFailoverVersion
	}
	return 0
}

func (x *HistoryBlobHeader) GetLastFailoverVersion() int64 {
	if x != nil {
		return x.LastFailoverVersion
	}
	return 0
}

func (x *HistoryBlobHeader) GetFirstEventId() int64 {
	if x != nil {
		return x.FirstEventId
	}
	return 0
}

func (x *HistoryBlobHeader) GetLastEventId() int64 {
	if x != nil {
		return x.LastEventId
	}
	return 0
}

func (x *HistoryBlobHeader) GetEventCount() int64 {
	if x != nil {
		return x.EventCount
	}
	return 0
}

type HistoryBlob struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Header *HistoryBlobHeader `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	Body   []*v1.History      `protobuf:"bytes,2,rep,name=body,proto3" json:"body,omitempty"`
}

func (x *HistoryBlob) Reset() {
	*x = HistoryBlob{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HistoryBlob) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HistoryBlob) ProtoMessage() {}

func (x *HistoryBlob) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HistoryBlob.ProtoReflect.Descriptor instead.
func (*HistoryBlob) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_archiver_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *HistoryBlob) GetHeader() *HistoryBlobHeader {
	if x != nil {
		return x.Header
	}
	return nil
}

func (x *HistoryBlob) GetBody() []*v1.History {
	if x != nil {
		return x.Body
	}
	return nil
}

// VisibilityRecord is a single workflow visibility record in archive.
type VisibilityRecord struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceId        string                      `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	Namespace          string                      `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
	WorkflowId         string                      `protobuf:"bytes,3,opt,name=workflow_id,json=workflowId,proto3" json:"workflow_id,omitempty"`
	RunId              string                      `protobuf:"bytes,4,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	WorkflowTypeName   string                      `protobuf:"bytes,5,opt,name=workflow_type_name,json=workflowTypeName,proto3" json:"workflow_type_name,omitempty"`
	StartTime          *timestamppb.Timestamp      `protobuf:"bytes,6,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	ExecutionTime      *timestamppb.Timestamp      `protobuf:"bytes,7,opt,name=execution_time,json=executionTime,proto3" json:"execution_time,omitempty"`
	CloseTime          *timestamppb.Timestamp      `protobuf:"bytes,8,opt,name=close_time,json=closeTime,proto3" json:"close_time,omitempty"`
	Status             v11.WorkflowExecutionStatus `protobuf:"varint,9,opt,name=status,proto3,enum=temporal.api.enums.v1.WorkflowExecutionStatus" json:"status,omitempty"`
	HistoryLength      int64                       `protobuf:"varint,10,opt,name=history_length,json=historyLength,proto3" json:"history_length,omitempty"`
	Memo               *v12.Memo                   `protobuf:"bytes,11,opt,name=memo,proto3" json:"memo,omitempty"`
	SearchAttributes   map[string]string           `protobuf:"bytes,12,rep,name=search_attributes,json=searchAttributes,proto3" json:"search_attributes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	HistoryArchivalUri string                      `protobuf:"bytes,13,opt,name=history_archival_uri,json=historyArchivalUri,proto3" json:"history_archival_uri,omitempty"`
	ExecutionDuration  *durationpb.Duration        `protobuf:"bytes,14,opt,name=execution_duration,json=executionDuration,proto3" json:"execution_duration,omitempty"`
}

func (x *VisibilityRecord) Reset() {
	*x = VisibilityRecord{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VisibilityRecord) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VisibilityRecord) ProtoMessage() {}

func (x *VisibilityRecord) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_archiver_v1_message_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VisibilityRecord.ProtoReflect.Descriptor instead.
func (*VisibilityRecord) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_archiver_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *VisibilityRecord) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *VisibilityRecord) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *VisibilityRecord) GetWorkflowId() string {
	if x != nil {
		return x.WorkflowId
	}
	return ""
}

func (x *VisibilityRecord) GetRunId() string {
	if x != nil {
		return x.RunId
	}
	return ""
}

func (x *VisibilityRecord) GetWorkflowTypeName() string {
	if x != nil {
		return x.WorkflowTypeName
	}
	return ""
}

func (x *VisibilityRecord) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

func (x *VisibilityRecord) GetExecutionTime() *timestamppb.Timestamp {
	if x != nil {
		return x.ExecutionTime
	}
	return nil
}

func (x *VisibilityRecord) GetCloseTime() *timestamppb.Timestamp {
	if x != nil {
		return x.CloseTime
	}
	return nil
}

func (x *VisibilityRecord) GetStatus() v11.WorkflowExecutionStatus {
	if x != nil {
		return x.Status
	}
	return v11.WorkflowExecutionStatus(0)
}

func (x *VisibilityRecord) GetHistoryLength() int64 {
	if x != nil {
		return x.HistoryLength
	}
	return 0
}

func (x *VisibilityRecord) GetMemo() *v12.Memo {
	if x != nil {
		return x.Memo
	}
	return nil
}

func (x *VisibilityRecord) GetSearchAttributes() map[string]string {
	if x != nil {
		return x.SearchAttributes
	}
	return nil
}

func (x *VisibilityRecord) GetHistoryArchivalUri() string {
	if x != nil {
		return x.HistoryArchivalUri
	}
	return ""
}

func (x *VisibilityRecord) GetExecutionDuration() *durationpb.Duration {
	if x != nil {
		return x.ExecutionDuration
	}
	return nil
}

var File_temporal_server_api_archiver_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_archiver_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2d, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f,
	0x61, 0x70, 0x69, 0x2f, 0x61, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x72, 0x2f, 0x76, 0x31, 0x2f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1f, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x61, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x72, 0x2e, 0x76, 0x31, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x25, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79,
	0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x24, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e,
	0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa2, 0x03, 0x0a, 0x11, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x42,
	0x6c, 0x6f, 0x62, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x20, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x23, 0x0a, 0x0b,
	0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12,
	0x19, 0x0a, 0x06, 0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05,
	0x72, 0x75, 0x6e, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1b, 0x0a, 0x07, 0x69, 0x73, 0x5f, 0x6c,
	0x61, 0x73, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x69, 0x73, 0x4c, 0x61, 0x73, 0x74,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x38, 0x0a, 0x16, 0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x66, 0x61, 0x69,
	0x6c, 0x6f, 0x76, 0x65, 0x72, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x14, 0x66, 0x69, 0x72, 0x73, 0x74, 0x46, 0x61, 0x69, 0x6c, 0x6f, 0x76, 0x65, 0x72,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x36, 0x0a, 0x15, 0x6c, 0x61,
	0x73, 0x74, 0x5f, 0x66, 0x61, 0x69, 0x6c, 0x6f, 0x76, 0x65, 0x72, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x03, 0x52, 0x13, 0x6c, 0x61, 0x73, 0x74, 0x46, 0x61, 0x69,
	0x6c, 0x6f, 0x76, 0x65, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x28,
	0x0a, 0x0e, 0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x66, 0x69, 0x72, 0x73, 0x74, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x26, 0x0a, 0x0d, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x09, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x6c, 0x61, 0x73,
	0x74, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x23, 0x0a, 0x0b, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x22, 0x97,
	0x01, 0x0a, 0x0b, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x42, 0x6c, 0x6f, 0x62, 0x12, 0x4e, 0x0a,
	0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x32, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x61, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x69, 0x73,
	0x74, 0x6f, 0x72, 0x79, 0x42, 0x6c, 0x6f, 0x62, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x52, 0x06, 0x68,
	0x65, 0x61, 0x64, 0x65, 0x72, 0x42, 0x02, 0x68, 0x00, 0x12, 0x38, 0x0a, 0x04, 0x62, 0x6f, 0x64, 0x79,
	0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x2e, 0x76, 0x31, 0x2e, 0x48,
	0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x52, 0x04, 0x62, 0x6f, 0x64, 0x79, 0x42, 0x02, 0x68, 0x00, 0x22,
	0x8a, 0x07, 0x0a, 0x10, 0x56, 0x69, 0x73, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x65, 0x63, 0x6f,
	0x72, 0x64, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x20, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x23, 0x0a, 0x0b, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c,
	0x6f, 0x77, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x77, 0x6f, 0x72, 0x6b,
	0x66, 0x6c, 0x6f, 0x77, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x19, 0x0a, 0x06, 0x72, 0x75, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72, 0x75, 0x6e, 0x49, 0x64, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x30, 0x0a, 0x12, 0x77, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x74,
	0x79, 0x70, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x77,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x79, 0x70, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x42, 0x02,
	0x68, 0x00, 0x12, 0x3d, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x45, 0x0a,
	0x0e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0d, 0x65,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12,
	0x3d, 0x0a, 0x0a, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x63, 0x6c,
	0x6f, 0x73, 0x65, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4a, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2e, 0x2e, 0x74, 0x65, 0x6d, 0x70,
	0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31,
	0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f,
	0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x42, 0x02,
	0x68, 0x00, 0x12, 0x29, 0x0a, 0x0e, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x5f, 0x6c, 0x65, 0x6e,
	0x67, 0x74, 0x68, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72,
	0x79, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x42, 0x02, 0x68, 0x00, 0x12, 0x34, 0x0a, 0x04, 0x6d, 0x65,
	0x6d, 0x6f, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x4d,
	0x65, 0x6d, 0x6f, 0x52, 0x04, 0x6d, 0x65, 0x6d, 0x6f, 0x42, 0x02, 0x68, 0x00, 0x12, 0x78, 0x0a, 0x11,
	0x73, 0x65, 0x61, 0x72, 0x63, 0x68, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73,
	0x18, 0x0c, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x47, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x61, 0x72, 0x63, 0x68, 0x69,
	0x76, 0x65, 0x72, 0x2e, 0x76, 0x31, 0x2e, 0x56, 0x69, 0x73, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79,
	0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2e, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x41, 0x74, 0x74, 0x72,
	0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x10, 0x73, 0x65, 0x61, 0x72,
	0x63, 0x68, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x12,
	0x34, 0x0a, 0x14, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x5f, 0x61, 0x72, 0x63, 0x68, 0x69, 0x76,
	0x61, 0x6c, 0x5f, 0x75, 0x72, 0x69, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12, 0x68, 0x69, 0x73,
	0x74, 0x6f, 0x72, 0x79, 0x41, 0x72, 0x63, 0x68, 0x69, 0x76, 0x61, 0x6c, 0x55, 0x72, 0x69, 0x42, 0x02,
	0x68, 0x00, 0x12, 0x4c, 0x0a, 0x12, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x64,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x0e, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x11, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x1a, 0x4b, 0x0a, 0x15, 0x53,
	0x65, 0x61, 0x72, 0x63, 0x68, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x45, 0x6e, 0x74,
	0x72, 0x79, 0x12, 0x14, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x42, 0x02, 0x68, 0x00, 0x12, 0x18, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x02, 0x68, 0x00, 0x3a,
	0x02, 0x38, 0x01, 0x42, 0x30, 0x5a, 0x2e, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61,
	0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x72, 0x2f, 0x76, 0x31, 0x3b, 0x61, 0x72, 0x63, 0x68, 0x69, 0x76,
	0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_archiver_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_archiver_v1_message_proto_rawDescData = file_temporal_server_api_archiver_v1_message_proto_rawDesc
)

func file_temporal_server_api_archiver_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_archiver_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_archiver_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_archiver_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_archiver_v1_message_proto_rawDescData
}

var file_temporal_server_api_archiver_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_temporal_server_api_archiver_v1_message_proto_goTypes = []interface{}{
	(*HistoryBlobHeader)(nil),        // 0: temporal.server.api.archiver.v1.HistoryBlobHeader
	(*HistoryBlob)(nil),              // 1: temporal.server.api.archiver.v1.HistoryBlob
	(*VisibilityRecord)(nil),         // 2: temporal.server.api.archiver.v1.VisibilityRecord
	nil,                              // 3: temporal.server.api.archiver.v1.VisibilityRecord.SearchAttributesEntry
	(*v1.History)(nil),               // 4: temporal.api.history.v1.History
	(*timestamppb.Timestamp)(nil),    // 5: google.protobuf.Timestamp
	(v11.WorkflowExecutionStatus)(0), // 6: temporal.api.enums.v1.WorkflowExecutionStatus
	(*v12.Memo)(nil),                 // 7: temporal.api.common.v1.Memo
	(*durationpb.Duration)(nil),      // 8: google.protobuf.Duration
}
var file_temporal_server_api_archiver_v1_message_proto_depIdxs = []int32{
	0, // 0: temporal.server.api.archiver.v1.HistoryBlob.header:type_name -> temporal.server.api.archiver.v1.HistoryBlobHeader
	4, // 1: temporal.server.api.archiver.v1.HistoryBlob.body:type_name -> temporal.api.history.v1.History
	5, // 2: temporal.server.api.archiver.v1.VisibilityRecord.start_time:type_name -> google.protobuf.Timestamp
	5, // 3: temporal.server.api.archiver.v1.VisibilityRecord.execution_time:type_name -> google.protobuf.Timestamp
	5, // 4: temporal.server.api.archiver.v1.VisibilityRecord.close_time:type_name -> google.protobuf.Timestamp
	6, // 5: temporal.server.api.archiver.v1.VisibilityRecord.status:type_name -> temporal.api.enums.v1.WorkflowExecutionStatus
	7, // 6: temporal.server.api.archiver.v1.VisibilityRecord.memo:type_name -> temporal.api.common.v1.Memo
	3, // 7: temporal.server.api.archiver.v1.VisibilityRecord.search_attributes:type_name -> temporal.server.api.archiver.v1.VisibilityRecord.SearchAttributesEntry
	8, // 8: temporal.server.api.archiver.v1.VisibilityRecord.execution_duration:type_name -> google.protobuf.Duration
	9, // [9:9] is the sub-list for method output_type
	9, // [9:9] is the sub-list for method input_type
	9, // [9:9] is the sub-list for extension type_name
	9, // [9:9] is the sub-list for extension extendee
	0, // [0:9] is the sub-list for field type_name
}

func init() { file_temporal_server_api_archiver_v1_message_proto_init() }
func file_temporal_server_api_archiver_v1_message_proto_init() {
	if File_temporal_server_api_archiver_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_archiver_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryBlobHeader); i {
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
		file_temporal_server_api_archiver_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HistoryBlob); i {
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
		file_temporal_server_api_archiver_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VisibilityRecord); i {
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
			RawDescriptor: file_temporal_server_api_archiver_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_archiver_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_archiver_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_archiver_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_archiver_v1_message_proto = out.File
	file_temporal_server_api_archiver_v1_message_proto_rawDesc = nil
	file_temporal_server_api_archiver_v1_message_proto_goTypes = nil
	file_temporal_server_api_archiver_v1_message_proto_depIdxs = nil
}
