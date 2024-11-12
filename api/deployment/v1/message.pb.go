// The MIT License
//
// Copyright (c) 2024 Temporal Technologies, Inc.
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
// source: temporal/server/api/deployment/v1/message.proto

package deployment

import (
	reflect "reflect"
	sync "sync"

	v1 "go.temporal.io/api/enums/v1"
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

type TaskQueue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name          string           `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	TaskQueueType v1.TaskQueueType `protobuf:"varint,2,opt,name=task_queue_type,json=taskQueueType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_queue_type,omitempty"`
}

func (x *TaskQueue) Reset() {
	*x = TaskQueue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueue) ProtoMessage() {}

func (x *TaskQueue) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskQueue.ProtoReflect.Descriptor instead.
func (*TaskQueue) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *TaskQueue) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TaskQueue) GetTaskQueueType() v1.TaskQueueType {
	if x != nil {
		return x.TaskQueueType
	}
	return v1.TaskQueueType(0)
}

type DeploymentTaskQueue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FirstPollerTime *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=first_poller_time,json=firstPollerTime,proto3" json:"first_poller_time,omitempty"`
	TaskQueue       *TaskQueue             `protobuf:"bytes,2,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
}

func (x *DeploymentTaskQueue) Reset() {
	*x = DeploymentTaskQueue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentTaskQueue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentTaskQueue) ProtoMessage() {}

func (x *DeploymentTaskQueue) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentTaskQueue.ProtoReflect.Descriptor instead.
func (*DeploymentTaskQueue) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *DeploymentTaskQueue) GetFirstPollerTime() *timestamppb.Timestamp {
	if x != nil {
		return x.FirstPollerTime
	}
	return nil
}

func (x *DeploymentTaskQueue) GetTaskQueue() *TaskQueue {
	if x != nil {
		return x.TaskQueue
	}
	return nil
}

type DeploymentWorkflowArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceName        string                 `protobuf:"bytes,1,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	NamespaceId          string                 `protobuf:"bytes,2,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	DeploymentTaskQueues []*DeploymentTaskQueue `protobuf:"bytes,3,rep,name=deployment_task_queues,json=deploymentTaskQueues,proto3" json:"deployment_task_queues,omitempty"`
}

func (x *DeploymentWorkflowArgs) Reset() {
	*x = DeploymentWorkflowArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentWorkflowArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentWorkflowArgs) ProtoMessage() {}

func (x *DeploymentWorkflowArgs) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentWorkflowArgs.ProtoReflect.Descriptor instead.
func (*DeploymentWorkflowArgs) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *DeploymentWorkflowArgs) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *DeploymentWorkflowArgs) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *DeploymentWorkflowArgs) GetDeploymentTaskQueues() []*DeploymentTaskQueue {
	if x != nil {
		return x.DeploymentTaskQueues
	}
	return nil
}

type DeploymentLocalState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceName        string                 `protobuf:"bytes,1,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	NamespaceId          string                 `protobuf:"bytes,2,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	DeploymentName       string                 `protobuf:"bytes,3,opt,name=deployment_name,json=deploymentName,proto3" json:"deployment_name,omitempty"`
	BuildId              string                 `protobuf:"bytes,4,opt,name=build_id,json=buildId,proto3" json:"build_id,omitempty"`
	DeploymentTaskQueues []*DeploymentTaskQueue `protobuf:"bytes,5,rep,name=deployment_task_queues,json=deploymentTaskQueues,proto3" json:"deployment_task_queues,omitempty"`
}

func (x *DeploymentLocalState) Reset() {
	*x = DeploymentLocalState{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentLocalState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentLocalState) ProtoMessage() {}

func (x *DeploymentLocalState) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentLocalState.ProtoReflect.Descriptor instead.
func (*DeploymentLocalState) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *DeploymentLocalState) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *DeploymentLocalState) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *DeploymentLocalState) GetDeploymentName() string {
	if x != nil {
		return x.DeploymentName
	}
	return ""
}

func (x *DeploymentLocalState) GetBuildId() string {
	if x != nil {
		return x.BuildId
	}
	return ""
}

func (x *DeploymentLocalState) GetDeploymentTaskQueues() []*DeploymentTaskQueue {
	if x != nil {
		return x.DeploymentTaskQueues
	}
	return nil
}

type DeploymentNameWorkflowArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceName  string `protobuf:"bytes,1,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	NamespaceId    string `protobuf:"bytes,2,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	DefaultBuildId string `protobuf:"bytes,3,opt,name=default_build_id,json=defaultBuildId,proto3" json:"default_build_id,omitempty"`
}

func (x *DeploymentNameWorkflowArgs) Reset() {
	*x = DeploymentNameWorkflowArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentNameWorkflowArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentNameWorkflowArgs) ProtoMessage() {}

func (x *DeploymentNameWorkflowArgs) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentNameWorkflowArgs.ProtoReflect.Descriptor instead.
func (*DeploymentNameWorkflowArgs) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{4}
}

func (x *DeploymentNameWorkflowArgs) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *DeploymentNameWorkflowArgs) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *DeploymentNameWorkflowArgs) GetDefaultBuildId() string {
	if x != nil {
		return x.DefaultBuildId
	}
	return ""
}

var File_temporal_server_api_deployment_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_deployment_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2f, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x21,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x1a,
	0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75,
	0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x75, 0x0a, 0x09, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x12, 0x16, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x50, 0x0a, 0x0f, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x24, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65,
	0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x54, 0x79,
	0x70, 0x65, 0x42, 0x02, 0x68, 0x00, 0x22, 0xb2, 0x01, 0x0a, 0x13, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79,
	0x6d, 0x65, 0x6e, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x4a, 0x0a, 0x11,
	0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x70, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x5f, 0x74, 0x69, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x0f, 0x66, 0x69, 0x72, 0x73, 0x74, 0x50, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x54, 0x69, 0x6d,
	0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4f, 0x0a, 0x0a, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70,
	0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x52, 0x09, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x42, 0x02,
	0x68, 0x00, 0x22, 0xdc, 0x01, 0x0a, 0x16, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74,
	0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x41, 0x72, 0x67, 0x73, 0x12, 0x29, 0x0a, 0x0e, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65,
	0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x70, 0x0a, 0x16, 0x64, 0x65, 0x70,
	0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x36, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70,
	0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79,
	0x6d, 0x65, 0x6e, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x14, 0x64,
	0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x22, 0xa6, 0x02, 0x0a, 0x14, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79,
	0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x29, 0x0a,
	0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x2b, 0x0a, 0x0f, 0x64,
	0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x1d, 0x0a, 0x08, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69,
	0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x70, 0x0a, 0x16, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e,
	0x74, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x18, 0x05, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x36, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e,
	0x74, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x54, 0x61,
	0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x14, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65,
	0x6e, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00,
	0x22, 0x9c, 0x01, 0x0a, 0x1a, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x41, 0x72, 0x67, 0x73, 0x12, 0x29, 0x0a,
	0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x2c, 0x0a, 0x10, 0x64,
	0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x5f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x42, 0x75, 0x69, 0x6c,
	0x64, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61,
	0x70, 0x69, 0x2f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2f, 0x76, 0x31, 0x3b,
	0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_temporal_server_api_deployment_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_deployment_v1_message_proto_rawDescData = file_temporal_server_api_deployment_v1_message_proto_rawDesc
)

func file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_deployment_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_deployment_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_deployment_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_deployment_v1_message_proto_rawDescData
}

var file_temporal_server_api_deployment_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_temporal_server_api_deployment_v1_message_proto_goTypes = []interface{}{
	(*TaskQueue)(nil),                  // 0: temporal.server.api.deployment.v1.TaskQueue
	(*DeploymentTaskQueue)(nil),        // 1: temporal.server.api.deployment.v1.DeploymentTaskQueue
	(*DeploymentWorkflowArgs)(nil),     // 2: temporal.server.api.deployment.v1.DeploymentWorkflowArgs
	(*DeploymentLocalState)(nil),       // 3: temporal.server.api.deployment.v1.DeploymentLocalState
	(*DeploymentNameWorkflowArgs)(nil), // 4: temporal.server.api.deployment.v1.DeploymentNameWorkflowArgs
	(v1.TaskQueueType)(0),              // 5: temporal.api.enums.v1.TaskQueueType
	(*timestamppb.Timestamp)(nil),      // 6: google.protobuf.Timestamp
}
var file_temporal_server_api_deployment_v1_message_proto_depIdxs = []int32{
	5, // 0: temporal.server.api.deployment.v1.TaskQueue.task_queue_type:type_name -> temporal.api.enums.v1.TaskQueueType
	6, // 1: temporal.server.api.deployment.v1.DeploymentTaskQueue.first_poller_time:type_name -> google.protobuf.Timestamp
	0, // 2: temporal.server.api.deployment.v1.DeploymentTaskQueue.task_queue:type_name -> temporal.server.api.deployment.v1.TaskQueue
	1, // 3: temporal.server.api.deployment.v1.DeploymentWorkflowArgs.deployment_task_queues:type_name -> temporal.server.api.deployment.v1.DeploymentTaskQueue
	1, // 4: temporal.server.api.deployment.v1.DeploymentLocalState.deployment_task_queues:type_name -> temporal.server.api.deployment.v1.DeploymentTaskQueue
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_temporal_server_api_deployment_v1_message_proto_init() }
func file_temporal_server_api_deployment_v1_message_proto_init() {
	if File_temporal_server_api_deployment_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskQueue); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentTaskQueue); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentWorkflowArgs); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentLocalState); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentNameWorkflowArgs); i {
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
			RawDescriptor: file_temporal_server_api_deployment_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_deployment_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_deployment_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_deployment_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_deployment_v1_message_proto = out.File
	file_temporal_server_api_deployment_v1_message_proto_rawDesc = nil
	file_temporal_server_api_deployment_v1_message_proto_goTypes = nil
	file_temporal_server_api_deployment_v1_message_proto_depIdxs = nil
}
