// The MIT License
//
// Copyright (c) 2019 Temporal Technologies, Inc.
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
// source: temporal/server/api/checksum/v1/message.proto

package checksum

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	v11 "go.temporal.io/api/enums/v1"
	v1 "go.temporal.io/server/api/enums/v1"
	v12 "go.temporal.io/server/api/history/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type MutableStateChecksumPayload struct {
	state                             protoimpl.MessageState      `protogen:"open.v1"`
	CancelRequested                   bool                        `protobuf:"varint,1,opt,name=cancel_requested,json=cancelRequested,proto3" json:"cancel_requested,omitempty"`
	State                             v1.WorkflowExecutionState   `protobuf:"varint,2,opt,name=state,proto3,enum=temporal.server.api.enums.v1.WorkflowExecutionState" json:"state,omitempty"`
	Status                            v11.WorkflowExecutionStatus `protobuf:"varint,3,opt,name=status,proto3,enum=temporal.api.enums.v1.WorkflowExecutionStatus" json:"status,omitempty"`
	LastWriteVersion                  int64                       `protobuf:"varint,4,opt,name=last_write_version,json=lastWriteVersion,proto3" json:"last_write_version,omitempty"`
	LastWriteEventId                  int64                       `protobuf:"varint,5,opt,name=last_write_event_id,json=lastWriteEventId,proto3" json:"last_write_event_id,omitempty"`
	LastFirstEventId                  int64                       `protobuf:"varint,6,opt,name=last_first_event_id,json=lastFirstEventId,proto3" json:"last_first_event_id,omitempty"`
	NextEventId                       int64                       `protobuf:"varint,7,opt,name=next_event_id,json=nextEventId,proto3" json:"next_event_id,omitempty"`
	LastProcessedEventId              int64                       `protobuf:"varint,8,opt,name=last_processed_event_id,json=lastProcessedEventId,proto3" json:"last_processed_event_id,omitempty"`
	SignalCount                       int64                       `protobuf:"varint,9,opt,name=signal_count,json=signalCount,proto3" json:"signal_count,omitempty"`
	ActivityCount                     int64                       `protobuf:"varint,21,opt,name=activity_count,json=activityCount,proto3" json:"activity_count,omitempty"`
	ChildExecutionCount               int64                       `protobuf:"varint,22,opt,name=child_execution_count,json=childExecutionCount,proto3" json:"child_execution_count,omitempty"`
	UserTimerCount                    int64                       `protobuf:"varint,23,opt,name=user_timer_count,json=userTimerCount,proto3" json:"user_timer_count,omitempty"`
	RequestCancelExternalCount        int64                       `protobuf:"varint,24,opt,name=request_cancel_external_count,json=requestCancelExternalCount,proto3" json:"request_cancel_external_count,omitempty"`
	SignalExternalCount               int64                       `protobuf:"varint,25,opt,name=signal_external_count,json=signalExternalCount,proto3" json:"signal_external_count,omitempty"`
	WorkflowTaskAttempt               int32                       `protobuf:"varint,10,opt,name=workflow_task_attempt,json=workflowTaskAttempt,proto3" json:"workflow_task_attempt,omitempty"`
	WorkflowTaskVersion               int64                       `protobuf:"varint,11,opt,name=workflow_task_version,json=workflowTaskVersion,proto3" json:"workflow_task_version,omitempty"`
	WorkflowTaskScheduledEventId      int64                       `protobuf:"varint,12,opt,name=workflow_task_scheduled_event_id,json=workflowTaskScheduledEventId,proto3" json:"workflow_task_scheduled_event_id,omitempty"`
	WorkflowTaskStartedEventId        int64                       `protobuf:"varint,13,opt,name=workflow_task_started_event_id,json=workflowTaskStartedEventId,proto3" json:"workflow_task_started_event_id,omitempty"`
	PendingTimerStartedEventIds       []int64                     `protobuf:"varint,14,rep,packed,name=pending_timer_started_event_ids,json=pendingTimerStartedEventIds,proto3" json:"pending_timer_started_event_ids,omitempty"`
	PendingActivityScheduledEventIds  []int64                     `protobuf:"varint,15,rep,packed,name=pending_activity_scheduled_event_ids,json=pendingActivityScheduledEventIds,proto3" json:"pending_activity_scheduled_event_ids,omitempty"`
	PendingSignalInitiatedEventIds    []int64                     `protobuf:"varint,16,rep,packed,name=pending_signal_initiated_event_ids,json=pendingSignalInitiatedEventIds,proto3" json:"pending_signal_initiated_event_ids,omitempty"`
	PendingReqCancelInitiatedEventIds []int64                     `protobuf:"varint,17,rep,packed,name=pending_req_cancel_initiated_event_ids,json=pendingReqCancelInitiatedEventIds,proto3" json:"pending_req_cancel_initiated_event_ids,omitempty"`
	PendingChildInitiatedEventIds     []int64                     `protobuf:"varint,18,rep,packed,name=pending_child_initiated_event_ids,json=pendingChildInitiatedEventIds,proto3" json:"pending_child_initiated_event_ids,omitempty"`
	PendingChasmNodePaths             []string                    `protobuf:"bytes,26,rep,name=pending_chasm_node_paths,json=pendingChasmNodePaths,proto3" json:"pending_chasm_node_paths,omitempty"`
	StickyTaskQueueName               string                      `protobuf:"bytes,19,opt,name=sticky_task_queue_name,json=stickyTaskQueueName,proto3" json:"sticky_task_queue_name,omitempty"`
	VersionHistories                  *v12.VersionHistories       `protobuf:"bytes,20,opt,name=version_histories,json=versionHistories,proto3" json:"version_histories,omitempty"`
	unknownFields                     protoimpl.UnknownFields
	sizeCache                         protoimpl.SizeCache
}

func (x *MutableStateChecksumPayload) Reset() {
	*x = MutableStateChecksumPayload{}
	mi := &file_temporal_server_api_checksum_v1_message_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *MutableStateChecksumPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MutableStateChecksumPayload) ProtoMessage() {}

func (x *MutableStateChecksumPayload) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_checksum_v1_message_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MutableStateChecksumPayload.ProtoReflect.Descriptor instead.
func (*MutableStateChecksumPayload) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_checksum_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *MutableStateChecksumPayload) GetCancelRequested() bool {
	if x != nil {
		return x.CancelRequested
	}
	return false
}

func (x *MutableStateChecksumPayload) GetState() v1.WorkflowExecutionState {
	if x != nil {
		return x.State
	}
	return v1.WorkflowExecutionState(0)
}

func (x *MutableStateChecksumPayload) GetStatus() v11.WorkflowExecutionStatus {
	if x != nil {
		return x.Status
	}
	return v11.WorkflowExecutionStatus(0)
}

func (x *MutableStateChecksumPayload) GetLastWriteVersion() int64 {
	if x != nil {
		return x.LastWriteVersion
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetLastWriteEventId() int64 {
	if x != nil {
		return x.LastWriteEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetLastFirstEventId() int64 {
	if x != nil {
		return x.LastFirstEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetNextEventId() int64 {
	if x != nil {
		return x.NextEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetLastProcessedEventId() int64 {
	if x != nil {
		return x.LastProcessedEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetSignalCount() int64 {
	if x != nil {
		return x.SignalCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetActivityCount() int64 {
	if x != nil {
		return x.ActivityCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetChildExecutionCount() int64 {
	if x != nil {
		return x.ChildExecutionCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetUserTimerCount() int64 {
	if x != nil {
		return x.UserTimerCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetRequestCancelExternalCount() int64 {
	if x != nil {
		return x.RequestCancelExternalCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetSignalExternalCount() int64 {
	if x != nil {
		return x.SignalExternalCount
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetWorkflowTaskAttempt() int32 {
	if x != nil {
		return x.WorkflowTaskAttempt
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetWorkflowTaskVersion() int64 {
	if x != nil {
		return x.WorkflowTaskVersion
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetWorkflowTaskScheduledEventId() int64 {
	if x != nil {
		return x.WorkflowTaskScheduledEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetWorkflowTaskStartedEventId() int64 {
	if x != nil {
		return x.WorkflowTaskStartedEventId
	}
	return 0
}

func (x *MutableStateChecksumPayload) GetPendingTimerStartedEventIds() []int64 {
	if x != nil {
		return x.PendingTimerStartedEventIds
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetPendingActivityScheduledEventIds() []int64 {
	if x != nil {
		return x.PendingActivityScheduledEventIds
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetPendingSignalInitiatedEventIds() []int64 {
	if x != nil {
		return x.PendingSignalInitiatedEventIds
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetPendingReqCancelInitiatedEventIds() []int64 {
	if x != nil {
		return x.PendingReqCancelInitiatedEventIds
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetPendingChildInitiatedEventIds() []int64 {
	if x != nil {
		return x.PendingChildInitiatedEventIds
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetPendingChasmNodePaths() []string {
	if x != nil {
		return x.PendingChasmNodePaths
	}
	return nil
}

func (x *MutableStateChecksumPayload) GetStickyTaskQueueName() string {
	if x != nil {
		return x.StickyTaskQueueName
	}
	return ""
}

func (x *MutableStateChecksumPayload) GetVersionHistories() *v12.VersionHistories {
	if x != nil {
		return x.VersionHistories
	}
	return nil
}

var File_temporal_server_api_checksum_v1_message_proto protoreflect.FileDescriptor

const file_temporal_server_api_checksum_v1_message_proto_rawDesc = "" +
	"\n" +
	"-temporal/server/api/checksum/v1/message.proto\x12\x1ftemporal.server.api.checksum.v1\x1a$temporal/api/enums/v1/workflow.proto\x1a,temporal/server/api/history/v1/message.proto\x1a+temporal/server/api/enums/v1/workflow.proto\"\xa2\f\n" +
	"\x1bMutableStateChecksumPayload\x12)\n" +
	"\x10cancel_requested\x18\x01 \x01(\bR\x0fcancelRequested\x12J\n" +
	"\x05state\x18\x02 \x01(\x0e24.temporal.server.api.enums.v1.WorkflowExecutionStateR\x05state\x12F\n" +
	"\x06status\x18\x03 \x01(\x0e2..temporal.api.enums.v1.WorkflowExecutionStatusR\x06status\x12,\n" +
	"\x12last_write_version\x18\x04 \x01(\x03R\x10lastWriteVersion\x12-\n" +
	"\x13last_write_event_id\x18\x05 \x01(\x03R\x10lastWriteEventId\x12-\n" +
	"\x13last_first_event_id\x18\x06 \x01(\x03R\x10lastFirstEventId\x12\"\n" +
	"\rnext_event_id\x18\a \x01(\x03R\vnextEventId\x125\n" +
	"\x17last_processed_event_id\x18\b \x01(\x03R\x14lastProcessedEventId\x12!\n" +
	"\fsignal_count\x18\t \x01(\x03R\vsignalCount\x12%\n" +
	"\x0eactivity_count\x18\x15 \x01(\x03R\ractivityCount\x122\n" +
	"\x15child_execution_count\x18\x16 \x01(\x03R\x13childExecutionCount\x12(\n" +
	"\x10user_timer_count\x18\x17 \x01(\x03R\x0euserTimerCount\x12A\n" +
	"\x1drequest_cancel_external_count\x18\x18 \x01(\x03R\x1arequestCancelExternalCount\x122\n" +
	"\x15signal_external_count\x18\x19 \x01(\x03R\x13signalExternalCount\x122\n" +
	"\x15workflow_task_attempt\x18\n" +
	" \x01(\x05R\x13workflowTaskAttempt\x122\n" +
	"\x15workflow_task_version\x18\v \x01(\x03R\x13workflowTaskVersion\x12F\n" +
	" workflow_task_scheduled_event_id\x18\f \x01(\x03R\x1cworkflowTaskScheduledEventId\x12B\n" +
	"\x1eworkflow_task_started_event_id\x18\r \x01(\x03R\x1aworkflowTaskStartedEventId\x12D\n" +
	"\x1fpending_timer_started_event_ids\x18\x0e \x03(\x03R\x1bpendingTimerStartedEventIds\x12N\n" +
	"$pending_activity_scheduled_event_ids\x18\x0f \x03(\x03R pendingActivityScheduledEventIds\x12J\n" +
	"\"pending_signal_initiated_event_ids\x18\x10 \x03(\x03R\x1ependingSignalInitiatedEventIds\x12Q\n" +
	"&pending_req_cancel_initiated_event_ids\x18\x11 \x03(\x03R!pendingReqCancelInitiatedEventIds\x12H\n" +
	"!pending_child_initiated_event_ids\x18\x12 \x03(\x03R\x1dpendingChildInitiatedEventIds\x127\n" +
	"\x18pending_chasm_node_paths\x18\x1a \x03(\tR\x15pendingChasmNodePaths\x123\n" +
	"\x16sticky_task_queue_name\x18\x13 \x01(\tR\x13stickyTaskQueueName\x12]\n" +
	"\x11version_histories\x18\x14 \x01(\v20.temporal.server.api.history.v1.VersionHistoriesR\x10versionHistoriesB0Z.go.temporal.io/server/api/checksum/v1;checksumb\x06proto3"

var (
	file_temporal_server_api_checksum_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_checksum_v1_message_proto_rawDescData []byte
)

func file_temporal_server_api_checksum_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_checksum_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_checksum_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_checksum_v1_message_proto_rawDesc), len(file_temporal_server_api_checksum_v1_message_proto_rawDesc)))
	})
	return file_temporal_server_api_checksum_v1_message_proto_rawDescData
}

var file_temporal_server_api_checksum_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_temporal_server_api_checksum_v1_message_proto_goTypes = []any{
	(*MutableStateChecksumPayload)(nil), // 0: temporal.server.api.checksum.v1.MutableStateChecksumPayload
	(v1.WorkflowExecutionState)(0),      // 1: temporal.server.api.enums.v1.WorkflowExecutionState
	(v11.WorkflowExecutionStatus)(0),    // 2: temporal.api.enums.v1.WorkflowExecutionStatus
	(*v12.VersionHistories)(nil),        // 3: temporal.server.api.history.v1.VersionHistories
}
var file_temporal_server_api_checksum_v1_message_proto_depIdxs = []int32{
	1, // 0: temporal.server.api.checksum.v1.MutableStateChecksumPayload.state:type_name -> temporal.server.api.enums.v1.WorkflowExecutionState
	2, // 1: temporal.server.api.checksum.v1.MutableStateChecksumPayload.status:type_name -> temporal.api.enums.v1.WorkflowExecutionStatus
	3, // 2: temporal.server.api.checksum.v1.MutableStateChecksumPayload.version_histories:type_name -> temporal.server.api.history.v1.VersionHistories
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_temporal_server_api_checksum_v1_message_proto_init() }
func file_temporal_server_api_checksum_v1_message_proto_init() {
	if File_temporal_server_api_checksum_v1_message_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_checksum_v1_message_proto_rawDesc), len(file_temporal_server_api_checksum_v1_message_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_checksum_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_checksum_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_checksum_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_checksum_v1_message_proto = out.File
	file_temporal_server_api_checksum_v1_message_proto_goTypes = nil
	file_temporal_server_api_checksum_v1_message_proto_depIdxs = nil
}
