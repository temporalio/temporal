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
// source: temporal/server/api/persistence/v1/update.proto

package persistence

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// UpdateAdmissionInfo contains information about a durably admitted update. Note that updates in Admitted state are typically
// non-durable (i.e. do not have a corresponding event in history). Durably admitted updates arise as a result of
// workflow reset or history event replication conflict: in these cases a WorkflowExecutionUpdateAdmittedEvent event is
// created when an accepted update (on one branch of workflow history) is converted into an admitted update (on another
// branch).
type UpdateAdmissionInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Location:
	//
	//	*UpdateAdmissionInfo_HistoryPointer_
	Location      isUpdateAdmissionInfo_Location `protobuf_oneof:"location"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateAdmissionInfo) Reset() {
	*x = UpdateAdmissionInfo{}
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateAdmissionInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateAdmissionInfo) ProtoMessage() {}

func (x *UpdateAdmissionInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateAdmissionInfo.ProtoReflect.Descriptor instead.
func (*UpdateAdmissionInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP(), []int{0}
}

func (x *UpdateAdmissionInfo) GetLocation() isUpdateAdmissionInfo_Location {
	if x != nil {
		return x.Location
	}
	return nil
}

func (x *UpdateAdmissionInfo) GetHistoryPointer() *UpdateAdmissionInfo_HistoryPointer {
	if x != nil {
		if x, ok := x.Location.(*UpdateAdmissionInfo_HistoryPointer_); ok {
			return x.HistoryPointer
		}
	}
	return nil
}

type isUpdateAdmissionInfo_Location interface {
	isUpdateAdmissionInfo_Location()
}

type UpdateAdmissionInfo_HistoryPointer_ struct {
	HistoryPointer *UpdateAdmissionInfo_HistoryPointer `protobuf:"bytes,1,opt,name=history_pointer,json=historyPointer,proto3,oneof"`
}

func (*UpdateAdmissionInfo_HistoryPointer_) isUpdateAdmissionInfo_Location() {}

// UpdateAcceptanceInfo contains information about an accepted update
type UpdateAcceptanceInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// the event ID of the WorkflowExecutionUpdateAcceptedEvent
	EventId       int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateAcceptanceInfo) Reset() {
	*x = UpdateAcceptanceInfo{}
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateAcceptanceInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateAcceptanceInfo) ProtoMessage() {}

func (x *UpdateAcceptanceInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateAcceptanceInfo.ProtoReflect.Descriptor instead.
func (*UpdateAcceptanceInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP(), []int{1}
}

func (x *UpdateAcceptanceInfo) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

// UpdateCompletionInfo contains information about a completed update
type UpdateCompletionInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// the event ID of the WorkflowExecutionUpdateCompletedEvent
	EventId int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	// the ID of the event batch containing the event_id above
	EventBatchId  int64 `protobuf:"varint,2,opt,name=event_batch_id,json=eventBatchId,proto3" json:"event_batch_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateCompletionInfo) Reset() {
	*x = UpdateCompletionInfo{}
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateCompletionInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateCompletionInfo) ProtoMessage() {}

func (x *UpdateCompletionInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateCompletionInfo.ProtoReflect.Descriptor instead.
func (*UpdateCompletionInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP(), []int{2}
}

func (x *UpdateCompletionInfo) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *UpdateCompletionInfo) GetEventBatchId() int64 {
	if x != nil {
		return x.EventBatchId
	}
	return 0
}

// UpdateInfo is the persistent state of a single update
type UpdateInfo struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Value:
	//
	//	*UpdateInfo_Acceptance
	//	*UpdateInfo_Completion
	//	*UpdateInfo_Admission
	Value                         isUpdateInfo_Value   `protobuf_oneof:"value"`
	LastUpdateVersionedTransition *VersionedTransition `protobuf:"bytes,4,opt,name=last_update_versioned_transition,json=lastUpdateVersionedTransition,proto3" json:"last_update_versioned_transition,omitempty"`
	unknownFields                 protoimpl.UnknownFields
	sizeCache                     protoimpl.SizeCache
}

func (x *UpdateInfo) Reset() {
	*x = UpdateInfo{}
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateInfo) ProtoMessage() {}

func (x *UpdateInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateInfo.ProtoReflect.Descriptor instead.
func (*UpdateInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP(), []int{3}
}

func (x *UpdateInfo) GetValue() isUpdateInfo_Value {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *UpdateInfo) GetAcceptance() *UpdateAcceptanceInfo {
	if x != nil {
		if x, ok := x.Value.(*UpdateInfo_Acceptance); ok {
			return x.Acceptance
		}
	}
	return nil
}

func (x *UpdateInfo) GetCompletion() *UpdateCompletionInfo {
	if x != nil {
		if x, ok := x.Value.(*UpdateInfo_Completion); ok {
			return x.Completion
		}
	}
	return nil
}

func (x *UpdateInfo) GetAdmission() *UpdateAdmissionInfo {
	if x != nil {
		if x, ok := x.Value.(*UpdateInfo_Admission); ok {
			return x.Admission
		}
	}
	return nil
}

func (x *UpdateInfo) GetLastUpdateVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.LastUpdateVersionedTransition
	}
	return nil
}

type isUpdateInfo_Value interface {
	isUpdateInfo_Value()
}

type UpdateInfo_Acceptance struct {
	// update has been accepted and this is the acceptance metadata
	Acceptance *UpdateAcceptanceInfo `protobuf:"bytes,1,opt,name=acceptance,proto3,oneof"`
}

type UpdateInfo_Completion struct {
	// update has been completed and this is the completion metadata
	Completion *UpdateCompletionInfo `protobuf:"bytes,2,opt,name=completion,proto3,oneof"`
}

type UpdateInfo_Admission struct {
	// update has been admitted and this is the admission metadata
	Admission *UpdateAdmissionInfo `protobuf:"bytes,3,opt,name=admission,proto3,oneof"`
}

func (*UpdateInfo_Acceptance) isUpdateInfo_Value() {}

func (*UpdateInfo_Completion) isUpdateInfo_Value() {}

func (*UpdateInfo_Admission) isUpdateInfo_Value() {}

type UpdateAdmissionInfo_HistoryPointer struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// the event ID of the WorkflowExecutionUpdateAdmittedEvent
	EventId int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	// the ID of the event batch containing the event_id
	EventBatchId  int64 `protobuf:"varint,2,opt,name=event_batch_id,json=eventBatchId,proto3" json:"event_batch_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpdateAdmissionInfo_HistoryPointer) Reset() {
	*x = UpdateAdmissionInfo_HistoryPointer{}
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateAdmissionInfo_HistoryPointer) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateAdmissionInfo_HistoryPointer) ProtoMessage() {}

func (x *UpdateAdmissionInfo_HistoryPointer) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_update_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateAdmissionInfo_HistoryPointer.ProtoReflect.Descriptor instead.
func (*UpdateAdmissionInfo_HistoryPointer) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP(), []int{0, 0}
}

func (x *UpdateAdmissionInfo_HistoryPointer) GetEventId() int64 {
	if x != nil {
		return x.EventId
	}
	return 0
}

func (x *UpdateAdmissionInfo_HistoryPointer) GetEventBatchId() int64 {
	if x != nil {
		return x.EventBatchId
	}
	return 0
}

var File_temporal_server_api_persistence_v1_update_proto protoreflect.FileDescriptor

const file_temporal_server_api_persistence_v1_update_proto_rawDesc = "" +
	"\n" +
	"/temporal/server/api/persistence/v1/update.proto\x12\"temporal.server.api.persistence.v1\x1a,temporal/server/api/persistence/v1/hsm.proto\"\xe7\x01\n" +
	"\x13UpdateAdmissionInfo\x12q\n" +
	"\x0fhistory_pointer\x18\x01 \x01(\v2F.temporal.server.api.persistence.v1.UpdateAdmissionInfo.HistoryPointerH\x00R\x0ehistoryPointer\x1aQ\n" +
	"\x0eHistoryPointer\x12\x19\n" +
	"\bevent_id\x18\x01 \x01(\x03R\aeventId\x12$\n" +
	"\x0eevent_batch_id\x18\x02 \x01(\x03R\feventBatchIdB\n" +
	"\n" +
	"\blocation\"1\n" +
	"\x14UpdateAcceptanceInfo\x12\x19\n" +
	"\bevent_id\x18\x01 \x01(\x03R\aeventId\"W\n" +
	"\x14UpdateCompletionInfo\x12\x19\n" +
	"\bevent_id\x18\x01 \x01(\x03R\aeventId\x12$\n" +
	"\x0eevent_batch_id\x18\x02 \x01(\x03R\feventBatchId\"\xa9\x03\n" +
	"\n" +
	"UpdateInfo\x12Z\n" +
	"\n" +
	"acceptance\x18\x01 \x01(\v28.temporal.server.api.persistence.v1.UpdateAcceptanceInfoH\x00R\n" +
	"acceptance\x12Z\n" +
	"\n" +
	"completion\x18\x02 \x01(\v28.temporal.server.api.persistence.v1.UpdateCompletionInfoH\x00R\n" +
	"completion\x12W\n" +
	"\tadmission\x18\x03 \x01(\v27.temporal.server.api.persistence.v1.UpdateAdmissionInfoH\x00R\tadmission\x12\x80\x01\n" +
	" last_update_versioned_transition\x18\x04 \x01(\v27.temporal.server.api.persistence.v1.VersionedTransitionR\x1dlastUpdateVersionedTransitionB\a\n" +
	"\x05valueB6Z4go.temporal.io/server/api/persistence/v1;persistenceb\x06proto3"

var (
	file_temporal_server_api_persistence_v1_update_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_update_proto_rawDescData []byte
)

func file_temporal_server_api_persistence_v1_update_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_update_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_update_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_update_proto_rawDesc), len(file_temporal_server_api_persistence_v1_update_proto_rawDesc)))
	})
	return file_temporal_server_api_persistence_v1_update_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_update_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_temporal_server_api_persistence_v1_update_proto_goTypes = []any{
	(*UpdateAdmissionInfo)(nil),                // 0: temporal.server.api.persistence.v1.UpdateAdmissionInfo
	(*UpdateAcceptanceInfo)(nil),               // 1: temporal.server.api.persistence.v1.UpdateAcceptanceInfo
	(*UpdateCompletionInfo)(nil),               // 2: temporal.server.api.persistence.v1.UpdateCompletionInfo
	(*UpdateInfo)(nil),                         // 3: temporal.server.api.persistence.v1.UpdateInfo
	(*UpdateAdmissionInfo_HistoryPointer)(nil), // 4: temporal.server.api.persistence.v1.UpdateAdmissionInfo.HistoryPointer
	(*VersionedTransition)(nil),                // 5: temporal.server.api.persistence.v1.VersionedTransition
}
var file_temporal_server_api_persistence_v1_update_proto_depIdxs = []int32{
	4, // 0: temporal.server.api.persistence.v1.UpdateAdmissionInfo.history_pointer:type_name -> temporal.server.api.persistence.v1.UpdateAdmissionInfo.HistoryPointer
	1, // 1: temporal.server.api.persistence.v1.UpdateInfo.acceptance:type_name -> temporal.server.api.persistence.v1.UpdateAcceptanceInfo
	2, // 2: temporal.server.api.persistence.v1.UpdateInfo.completion:type_name -> temporal.server.api.persistence.v1.UpdateCompletionInfo
	0, // 3: temporal.server.api.persistence.v1.UpdateInfo.admission:type_name -> temporal.server.api.persistence.v1.UpdateAdmissionInfo
	5, // 4: temporal.server.api.persistence.v1.UpdateInfo.last_update_versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_update_proto_init() }
func file_temporal_server_api_persistence_v1_update_proto_init() {
	if File_temporal_server_api_persistence_v1_update_proto != nil {
		return
	}
	file_temporal_server_api_persistence_v1_hsm_proto_init()
	file_temporal_server_api_persistence_v1_update_proto_msgTypes[0].OneofWrappers = []any{
		(*UpdateAdmissionInfo_HistoryPointer_)(nil),
	}
	file_temporal_server_api_persistence_v1_update_proto_msgTypes[3].OneofWrappers = []any{
		(*UpdateInfo_Acceptance)(nil),
		(*UpdateInfo_Completion)(nil),
		(*UpdateInfo_Admission)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_update_proto_rawDesc), len(file_temporal_server_api_persistence_v1_update_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_update_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_update_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_update_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_update_proto = out.File
	file_temporal_server_api_persistence_v1_update_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_update_proto_depIdxs = nil
}
