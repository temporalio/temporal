// The MIT License
//
// Copyright (c) 2025 Temporal Technologies, Inc.
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
// source: temporal/server/api/persistence/v1/chasm.proto

package persistence

import (
	reflect "reflect"
	sync "sync"

	v1 "go.temporal.io/api/common/v1"
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

type ChasmNode struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Versioned transition when the node was instantiated.
	InitialVersionedTransition *VersionedTransition `protobuf:"bytes,1,opt,name=initial_versioned_transition,json=initialVersionedTransition,proto3" json:"initial_versioned_transition,omitempty"`
	// Versioned transition when the node was last updated.
	LastUpdateVersionedTransition *VersionedTransition `protobuf:"bytes,2,opt,name=last_update_versioned_transition,json=lastUpdateVersionedTransition,proto3" json:"last_update_versioned_transition,omitempty"`
	// Types that are assignable to Attributes:
	//
	//	*ChasmNode_ComponentAttributes
	//	*ChasmNode_DataAttributes
	//	*ChasmNode_CollectionAttributes
	//	*ChasmNode_PointerAttributes
	Attributes isChasmNode_Attributes `protobuf_oneof:"attributes"`
}

func (x *ChasmNode) Reset() {
	*x = ChasmNode{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmNode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmNode) ProtoMessage() {}

func (x *ChasmNode) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmNode.ProtoReflect.Descriptor instead.
func (*ChasmNode) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{0}
}

func (x *ChasmNode) GetInitialVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.InitialVersionedTransition
	}
	return nil
}

func (x *ChasmNode) GetLastUpdateVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.LastUpdateVersionedTransition
	}
	return nil
}

func (m *ChasmNode) GetAttributes() isChasmNode_Attributes {
	if m != nil {
		return m.Attributes
	}
	return nil
}

func (x *ChasmNode) GetComponentAttributes() *ChasmComponentAttributes {
	if x, ok := x.GetAttributes().(*ChasmNode_ComponentAttributes); ok {
		return x.ComponentAttributes
	}
	return nil
}

func (x *ChasmNode) GetDataAttributes() *ChasmDataAttributes {
	if x, ok := x.GetAttributes().(*ChasmNode_DataAttributes); ok {
		return x.DataAttributes
	}
	return nil
}

func (x *ChasmNode) GetCollectionAttributes() *ChasmCollectionAttributes {
	if x, ok := x.GetAttributes().(*ChasmNode_CollectionAttributes); ok {
		return x.CollectionAttributes
	}
	return nil
}

func (x *ChasmNode) GetPointerAttributes() *ChasmPointerAttributes {
	if x, ok := x.GetAttributes().(*ChasmNode_PointerAttributes); ok {
		return x.PointerAttributes
	}
	return nil
}

type isChasmNode_Attributes interface {
	isChasmNode_Attributes()
}

type ChasmNode_ComponentAttributes struct {
	ComponentAttributes *ChasmComponentAttributes `protobuf:"bytes,11,opt,name=component_attributes,json=componentAttributes,proto3,oneof"`
}

type ChasmNode_DataAttributes struct {
	DataAttributes *ChasmDataAttributes `protobuf:"bytes,12,opt,name=data_attributes,json=dataAttributes,proto3,oneof"`
}

type ChasmNode_CollectionAttributes struct {
	CollectionAttributes *ChasmCollectionAttributes `protobuf:"bytes,13,opt,name=collection_attributes,json=collectionAttributes,proto3,oneof"`
}

type ChasmNode_PointerAttributes struct {
	PointerAttributes *ChasmPointerAttributes `protobuf:"bytes,14,opt,name=pointer_attributes,json=pointerAttributes,proto3,oneof"`
}

func (*ChasmNode_ComponentAttributes) isChasmNode_Attributes() {}

func (*ChasmNode_DataAttributes) isChasmNode_Attributes() {}

func (*ChasmNode_CollectionAttributes) isChasmNode_Attributes() {}

func (*ChasmNode_PointerAttributes) isChasmNode_Attributes() {}

type ChasmComponentAttributes struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Fully qualified type name of a registered component.
	Type  string                           `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Data  *v1.DataBlob                     `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	Tasks []*ChasmComponentAttributes_Task `protobuf:"bytes,3,rep,name=tasks,proto3" json:"tasks,omitempty"`
}

func (x *ChasmComponentAttributes) Reset() {
	*x = ChasmComponentAttributes{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmComponentAttributes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmComponentAttributes) ProtoMessage() {}

func (x *ChasmComponentAttributes) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmComponentAttributes.ProtoReflect.Descriptor instead.
func (*ChasmComponentAttributes) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{1}
}

func (x *ChasmComponentAttributes) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *ChasmComponentAttributes) GetData() *v1.DataBlob {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *ChasmComponentAttributes) GetTasks() []*ChasmComponentAttributes_Task {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type ChasmDataAttributes struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data *v1.DataBlob `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *ChasmDataAttributes) Reset() {
	*x = ChasmDataAttributes{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmDataAttributes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmDataAttributes) ProtoMessage() {}

func (x *ChasmDataAttributes) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmDataAttributes.ProtoReflect.Descriptor instead.
func (*ChasmDataAttributes) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{2}
}

func (x *ChasmDataAttributes) GetData() *v1.DataBlob {
	if x != nil {
		return x.Data
	}
	return nil
}

type ChasmCollectionAttributes struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ChasmCollectionAttributes) Reset() {
	*x = ChasmCollectionAttributes{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmCollectionAttributes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmCollectionAttributes) ProtoMessage() {}

func (x *ChasmCollectionAttributes) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmCollectionAttributes.ProtoReflect.Descriptor instead.
func (*ChasmCollectionAttributes) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{3}
}

type ChasmPointerAttributes struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NodePath []string `protobuf:"bytes,1,rep,name=node_path,json=nodePath,proto3" json:"node_path,omitempty"`
}

func (x *ChasmPointerAttributes) Reset() {
	*x = ChasmPointerAttributes{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmPointerAttributes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmPointerAttributes) ProtoMessage() {}

func (x *ChasmPointerAttributes) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmPointerAttributes.ProtoReflect.Descriptor instead.
func (*ChasmPointerAttributes) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{4}
}

func (x *ChasmPointerAttributes) GetNodePath() []string {
	if x != nil {
		return x.NodePath
	}
	return nil
}

type ChasmComponentAttributes_Task struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Fully qualified type name of a registered task.
	Type          string                 `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Destination   string                 `protobuf:"bytes,2,opt,name=destination,proto3" json:"destination,omitempty"`
	ScheduledTime *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=scheduled_time,json=scheduledTime,proto3" json:"scheduled_time,omitempty"`
	Data          *v1.DataBlob           `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
	// Versioned transition of the Entity when the task was created.
	VersionedTransition *VersionedTransition `protobuf:"bytes,5,opt,name=versioned_transition,json=versionedTransition,proto3" json:"versioned_transition,omitempty"`
}

func (x *ChasmComponentAttributes_Task) Reset() {
	*x = ChasmComponentAttributes_Task{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChasmComponentAttributes_Task) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChasmComponentAttributes_Task) ProtoMessage() {}

func (x *ChasmComponentAttributes_Task) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChasmComponentAttributes_Task.ProtoReflect.Descriptor instead.
func (*ChasmComponentAttributes_Task) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP(), []int{1, 0}
}

func (x *ChasmComponentAttributes_Task) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *ChasmComponentAttributes_Task) GetDestination() string {
	if x != nil {
		return x.Destination
	}
	return ""
}

func (x *ChasmComponentAttributes_Task) GetScheduledTime() *timestamppb.Timestamp {
	if x != nil {
		return x.ScheduledTime
	}
	return nil
}

func (x *ChasmComponentAttributes_Task) GetData() *v1.DataBlob {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *ChasmComponentAttributes_Task) GetVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.VersionedTransition
	}
	return nil
}

var File_temporal_server_api_persistence_v1_chasm_proto protoreflect.FileDescriptor

var file_temporal_server_api_persistence_v1_chasm_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2f,
	0x76, 0x31, 0x2f, 0x63, 0x68, 0x61, 0x73, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x22,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x2c, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65,
	0x2f, 0x76, 0x31, 0x2f, 0x68, 0x73, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0xe9, 0x05, 0x0a, 0x09, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x7d,
	0x0a, 0x1c, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x65, 0x64, 0x5f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74,
	0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64,
	0x54, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x1a, 0x69, 0x6e, 0x69, 0x74,
	0x69, 0x61, 0x6c, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64, 0x54, 0x72, 0x61, 0x6e, 0x73,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x84, 0x01, 0x0a, 0x20, 0x6c, 0x61, 0x73,
	0x74, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x65, 0x64, 0x5f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65,
	0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64, 0x54,
	0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x1d, 0x6c, 0x61, 0x73, 0x74, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64, 0x54, 0x72, 0x61,
	0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x75, 0x0a, 0x14, 0x63, 0x6f,
	0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74,
	0x65, 0x73, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x3c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72,
	0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x73,
	0x6d, 0x43, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75,
	0x74, 0x65, 0x73, 0x48, 0x00, 0x52, 0x13, 0x63, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74,
	0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x12, 0x66, 0x0a,
	0x0f, 0x64, 0x61, 0x74, 0x61, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18,
	0x0c, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69,
	0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x44, 0x61,
	0x74, 0x61, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x48, 0x00, 0x52, 0x0e,
	0x64, 0x61, 0x74, 0x61, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x78, 0x0a, 0x15, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f,
	0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x3d, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65,
	0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x48, 0x00, 0x52, 0x14, 0x63,
	0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75,
	0x74, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x12, 0x6f, 0x0a, 0x12, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x65,
	0x72, 0x5f, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x18, 0x0e, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x3a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e,
	0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x50, 0x6f, 0x69, 0x6e, 0x74,
	0x65, 0x72, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x48, 0x00, 0x52, 0x11, 0x70,
	0x6f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x42,
	0x02, 0x68, 0x00, 0x42, 0x0c, 0x0a, 0x0a, 0x61, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65,
	0x73, 0x22, 0x81, 0x04, 0x0a, 0x18, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x43, 0x6f, 0x6d, 0x70, 0x6f, 0x6e,
	0x65, 0x6e, 0x74, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x16, 0x0a,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x38, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x42, 0x6c,
	0x6f, 0x62, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x02, 0x68, 0x00, 0x12, 0x5b, 0x0a, 0x05, 0x74,
	0x61, 0x73, 0x6b, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x41, 0x2e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68,
	0x61, 0x73, 0x6d, 0x43, 0x6f, 0x6d, 0x70, 0x6f, 0x6e, 0x65, 0x6e, 0x74, 0x41, 0x74, 0x74, 0x72,
	0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b,
	0x73, 0x42, 0x02, 0x68, 0x00, 0x1a, 0xb5, 0x02, 0x0a, 0x04, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x16,
	0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70,
	0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x24, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x74, 0x69,
	0x6e, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x45, 0x0a, 0x0e, 0x73, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0d, 0x73, 0x63, 0x68,
	0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x38,
	0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e,
	0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x42, 0x6c, 0x6f, 0x62, 0x52, 0x04, 0x64, 0x61,
	0x74, 0x61, 0x42, 0x02, 0x68, 0x00, 0x12, 0x6e, 0x0a, 0x14, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x65, 0x64, 0x5f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65,
	0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x65, 0x64,
	0x54, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x13, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x65, 0x64, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68,
	0x00, 0x22, 0x4f, 0x0a, 0x13, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x44, 0x61, 0x74, 0x61, 0x41, 0x74,
	0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x12, 0x38, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x61,
	0x74, 0x61, 0x42, 0x6c, 0x6f, 0x62, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x02, 0x68, 0x00, 0x22,
	0x1b, 0x0a, 0x19, 0x43, 0x68, 0x61, 0x73, 0x6d, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62, 0x75, 0x74, 0x65, 0x73, 0x22, 0x39, 0x0a, 0x16, 0x43,
	0x68, 0x61, 0x73, 0x6d, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x41, 0x74, 0x74, 0x72, 0x69, 0x62,
	0x75, 0x74, 0x65, 0x73, 0x12, 0x1f, 0x0a, 0x09, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x70, 0x61, 0x74,
	0x68, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x08, 0x6e, 0x6f, 0x64, 0x65, 0x50, 0x61, 0x74, 0x68,
	0x42, 0x02, 0x68, 0x00, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2f, 0x76, 0x31, 0x3b, 0x70,
	0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_temporal_server_api_persistence_v1_chasm_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_chasm_proto_rawDescData = file_temporal_server_api_persistence_v1_chasm_proto_rawDesc
)

func file_temporal_server_api_persistence_v1_chasm_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_chasm_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_chasm_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_persistence_v1_chasm_proto_rawDescData)
	})
	return file_temporal_server_api_persistence_v1_chasm_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_chasm_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_temporal_server_api_persistence_v1_chasm_proto_goTypes = []interface{}{
	(*ChasmNode)(nil),                     // 0: temporal.server.api.persistence.v1.ChasmNode
	(*ChasmComponentAttributes)(nil),      // 1: temporal.server.api.persistence.v1.ChasmComponentAttributes
	(*ChasmDataAttributes)(nil),           // 2: temporal.server.api.persistence.v1.ChasmDataAttributes
	(*ChasmCollectionAttributes)(nil),     // 3: temporal.server.api.persistence.v1.ChasmCollectionAttributes
	(*ChasmPointerAttributes)(nil),        // 4: temporal.server.api.persistence.v1.ChasmPointerAttributes
	(*ChasmComponentAttributes_Task)(nil), // 5: temporal.server.api.persistence.v1.ChasmComponentAttributes.Task
	(*VersionedTransition)(nil),           // 6: temporal.server.api.persistence.v1.VersionedTransition
	(*v1.DataBlob)(nil),                   // 7: temporal.api.common.v1.DataBlob
	(*timestamppb.Timestamp)(nil),         // 8: google.protobuf.Timestamp
}
var file_temporal_server_api_persistence_v1_chasm_proto_depIdxs = []int32{
	6,  // 0: temporal.server.api.persistence.v1.ChasmNode.initial_versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	6,  // 1: temporal.server.api.persistence.v1.ChasmNode.last_update_versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	1,  // 2: temporal.server.api.persistence.v1.ChasmNode.component_attributes:type_name -> temporal.server.api.persistence.v1.ChasmComponentAttributes
	2,  // 3: temporal.server.api.persistence.v1.ChasmNode.data_attributes:type_name -> temporal.server.api.persistence.v1.ChasmDataAttributes
	3,  // 4: temporal.server.api.persistence.v1.ChasmNode.collection_attributes:type_name -> temporal.server.api.persistence.v1.ChasmCollectionAttributes
	4,  // 5: temporal.server.api.persistence.v1.ChasmNode.pointer_attributes:type_name -> temporal.server.api.persistence.v1.ChasmPointerAttributes
	7,  // 6: temporal.server.api.persistence.v1.ChasmComponentAttributes.data:type_name -> temporal.api.common.v1.DataBlob
	5,  // 7: temporal.server.api.persistence.v1.ChasmComponentAttributes.tasks:type_name -> temporal.server.api.persistence.v1.ChasmComponentAttributes.Task
	7,  // 8: temporal.server.api.persistence.v1.ChasmDataAttributes.data:type_name -> temporal.api.common.v1.DataBlob
	8,  // 9: temporal.server.api.persistence.v1.ChasmComponentAttributes.Task.scheduled_time:type_name -> google.protobuf.Timestamp
	7,  // 10: temporal.server.api.persistence.v1.ChasmComponentAttributes.Task.data:type_name -> temporal.api.common.v1.DataBlob
	6,  // 11: temporal.server.api.persistence.v1.ChasmComponentAttributes.Task.versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	12, // [12:12] is the sub-list for method output_type
	12, // [12:12] is the sub-list for method input_type
	12, // [12:12] is the sub-list for extension type_name
	12, // [12:12] is the sub-list for extension extendee
	0,  // [0:12] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_chasm_proto_init() }
func file_temporal_server_api_persistence_v1_chasm_proto_init() {
	if File_temporal_server_api_persistence_v1_chasm_proto != nil {
		return
	}
	file_temporal_server_api_persistence_v1_hsm_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmNode); i {
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
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmComponentAttributes); i {
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
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmDataAttributes); i {
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
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmCollectionAttributes); i {
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
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmPointerAttributes); i {
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
		file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChasmComponentAttributes_Task); i {
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
	file_temporal_server_api_persistence_v1_chasm_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*ChasmNode_ComponentAttributes)(nil),
		(*ChasmNode_DataAttributes)(nil),
		(*ChasmNode_CollectionAttributes)(nil),
		(*ChasmNode_PointerAttributes)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_persistence_v1_chasm_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_chasm_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_chasm_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_chasm_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_chasm_proto = out.File
	file_temporal_server_api_persistence_v1_chasm_proto_rawDesc = nil
	file_temporal_server_api_persistence_v1_chasm_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_chasm_proto_depIdxs = nil
}
