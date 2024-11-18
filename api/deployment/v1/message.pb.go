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

	v1 "go.temporal.io/api/common/v1"
	v11 "go.temporal.io/api/enums/v1"
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

type DeploymentLocalState struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	WorkerDeployment  *v1.WorkerDeployment                                 `protobuf:"bytes,1,opt,name=worker_deployment,json=workerDeployment,proto3" json:"worker_deployment,omitempty"`
	TaskQueueFamilies map[string]*DeploymentLocalState_TaskQueueFamilyInfo `protobuf:"bytes,2,rep,name=task_queue_families,json=taskQueueFamilies,proto3" json:"task_queue_families,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *DeploymentLocalState) Reset() {
	*x = DeploymentLocalState{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentLocalState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentLocalState) ProtoMessage() {}

func (x *DeploymentLocalState) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use DeploymentLocalState.ProtoReflect.Descriptor instead.
func (*DeploymentLocalState) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *DeploymentLocalState) GetWorkerDeployment() *v1.WorkerDeployment {
	if x != nil {
		return x.WorkerDeployment
	}
	return nil
}

func (x *DeploymentLocalState) GetTaskQueueFamilies() map[string]*DeploymentLocalState_TaskQueueFamilyInfo {
	if x != nil {
		return x.TaskQueueFamilies
	}
	return nil
}

type DeploymentWorkflowArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NamespaceName        string                `protobuf:"bytes,1,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	NamespaceId          string                `protobuf:"bytes,2,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	DeploymentLocalState *DeploymentLocalState `protobuf:"bytes,3,opt,name=deployment_local_state,json=deploymentLocalState,proto3" json:"deployment_local_state,omitempty"`
}

func (x *DeploymentWorkflowArgs) Reset() {
	*x = DeploymentWorkflowArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentWorkflowArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentWorkflowArgs) ProtoMessage() {}

func (x *DeploymentWorkflowArgs) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use DeploymentWorkflowArgs.ProtoReflect.Descriptor instead.
func (*DeploymentWorkflowArgs) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{1}
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

func (x *DeploymentWorkflowArgs) GetDeploymentLocalState() *DeploymentLocalState {
	if x != nil {
		return x.DeploymentLocalState
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
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentNameWorkflowArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentNameWorkflowArgs) ProtoMessage() {}

func (x *DeploymentNameWorkflowArgs) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use DeploymentNameWorkflowArgs.ProtoReflect.Descriptor instead.
func (*DeploymentNameWorkflowArgs) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{2}
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

type RegisterWorkerInDeploymentArgs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskQueueName   string                 `protobuf:"bytes,1,opt,name=task_queue_name,json=taskQueueName,proto3" json:"task_queue_name,omitempty"`
	TaskQueueType   v11.TaskQueueType      `protobuf:"varint,2,opt,name=task_queue_type,json=taskQueueType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_queue_type,omitempty"`
	FirstPollerTime *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=first_poller_time,json=firstPollerTime,proto3" json:"first_poller_time,omitempty"`
}

func (x *RegisterWorkerInDeploymentArgs) Reset() {
	*x = RegisterWorkerInDeploymentArgs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterWorkerInDeploymentArgs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterWorkerInDeploymentArgs) ProtoMessage() {}

func (x *RegisterWorkerInDeploymentArgs) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use RegisterWorkerInDeploymentArgs.ProtoReflect.Descriptor instead.
func (*RegisterWorkerInDeploymentArgs) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *RegisterWorkerInDeploymentArgs) GetTaskQueueName() string {
	if x != nil {
		return x.TaskQueueName
	}
	return ""
}

func (x *RegisterWorkerInDeploymentArgs) GetTaskQueueType() v11.TaskQueueType {
	if x != nil {
		return x.TaskQueueType
	}
	return v11.TaskQueueType(0)
}

func (x *RegisterWorkerInDeploymentArgs) GetFirstPollerTime() *timestamppb.Timestamp {
	if x != nil {
		return x.FirstPollerTime
	}
	return nil
}

// Used in internal APIs to pass versioning info about a versioned workflow.
type WorkflowVersioningInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Behavior   v11.VersioningBehavior `protobuf:"varint,1,opt,name=behavior,proto3,enum=temporal.api.enums.v1.VersioningBehavior" json:"behavior,omitempty"`
	Deployment *v1.WorkerDeployment   `protobuf:"bytes,2,opt,name=deployment,proto3" json:"deployment,omitempty"`
}

func (x *WorkflowVersioningInfo) Reset() {
	*x = WorkflowVersioningInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WorkflowVersioningInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkflowVersioningInfo) ProtoMessage() {}

func (x *WorkflowVersioningInfo) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use WorkflowVersioningInfo.ProtoReflect.Descriptor instead.
func (*WorkflowVersioningInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{4}
}

func (x *WorkflowVersioningInfo) GetBehavior() v11.VersioningBehavior {
	if x != nil {
		return x.Behavior
	}
	return v11.VersioningBehavior(0)
}

func (x *WorkflowVersioningInfo) GetDeployment() *v1.WorkerDeployment {
	if x != nil {
		return x.Deployment
	}
	return nil
}

// Information about task redirect done by Matching.
type TaskRedirectInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Deployment sent in WorkflowVersioningInfo when the task was scheduled.
	SourceDeployment *v1.WorkerDeployment `protobuf:"bytes,1,opt,name=source_deployment,json=sourceDeployment,proto3" json:"source_deployment,omitempty"`
	// Whether the source deployment contains the task's task queue. Used by history to determine
	// if the task redirect should affect the workflow.
	SourceDeploymentContainsTaskQueue *v1.WorkerDeployment `protobuf:"bytes,2,opt,name=source_deployment_contains_task_queue,json=sourceDeploymentContainsTaskQueue,proto3" json:"source_deployment_contains_task_queue,omitempty"`
}

func (x *TaskRedirectInfo) Reset() {
	*x = TaskRedirectInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskRedirectInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskRedirectInfo) ProtoMessage() {}

func (x *TaskRedirectInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskRedirectInfo.ProtoReflect.Descriptor instead.
func (*TaskRedirectInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{5}
}

func (x *TaskRedirectInfo) GetSourceDeployment() *v1.WorkerDeployment {
	if x != nil {
		return x.SourceDeployment
	}
	return nil
}

func (x *TaskRedirectInfo) GetSourceDeploymentContainsTaskQueue() *v1.WorkerDeployment {
	if x != nil {
		return x.SourceDeploymentContainsTaskQueue
	}
	return nil
}

type DeploymentLocalState_TaskQueueFamilyInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// key: taskQueueType, val: TaskQueueInfo
	TaskQueues map[int32]*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo `protobuf:"bytes,1,rep,name=task_queues,json=taskQueues,proto3" json:"task_queues,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo) Reset() {
	*x = DeploymentLocalState_TaskQueueFamilyInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentLocalState_TaskQueueFamilyInfo) ProtoMessage() {}

func (x *DeploymentLocalState_TaskQueueFamilyInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentLocalState_TaskQueueFamilyInfo.ProtoReflect.Descriptor instead.
func (*DeploymentLocalState_TaskQueueFamilyInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{0, 1}
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo) GetTaskQueues() map[int32]*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo {
	if x != nil {
		return x.TaskQueues
	}
	return nil
}

type DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskQueueType   v11.TaskQueueType      `protobuf:"varint,1,opt,name=task_queue_type,json=taskQueueType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_queue_type,omitempty"`
	FirstPollerTime *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=first_poller_time,json=firstPollerTime,proto3" json:"first_poller_time,omitempty"`
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) Reset() {
	*x = DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) ProtoMessage() {}

func (x *DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_deployment_v1_message_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo.ProtoReflect.Descriptor instead.
func (*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_deployment_v1_message_proto_rawDescGZIP(), []int{0, 1, 1}
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) GetTaskQueueType() v11.TaskQueueType {
	if x != nil {
		return x.TaskQueueType
	}
	return v11.TaskQueueType(0)
}

func (x *DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo) GetFirstPollerTime() *timestamppb.Timestamp {
	if x != nil {
		return x.FirstPollerTime
	}
	return nil
}

var File_temporal_server_api_deployment_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_deployment_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2f, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x21,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31,
	0x1a, 0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e,
	0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x77, 0x6f, 0x72,
	0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x24, 0x74, 0x65, 0x6d, 0x70,
	0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f,
	0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x80, 0x07, 0x0a, 0x14, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63,
	0x61, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x59, 0x0a, 0x11, 0x77, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x5f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x28, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72,
	0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x10, 0x77, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x12, 0x82,
	0x01, 0x0a, 0x13, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x66, 0x61,
	0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x4e, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x44,
	0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x46, 0x61, 0x6d, 0x69, 0x6c,
	0x69, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x11, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x69, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x1a, 0x99,
	0x01, 0x0a, 0x16, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x46, 0x61, 0x6d, 0x69, 0x6c,
	0x69, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x14, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x02, 0x68, 0x00, 0x12, 0x65, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x4b, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x44,
	0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x46, 0x61, 0x6d, 0x69,
	0x6c, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x02, 0x68, 0x00,
	0x3a, 0x02, 0x38, 0x01, 0x1a, 0xeb, 0x03, 0x0a, 0x13, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x80, 0x01, 0x0a, 0x0b,
	0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x5b, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74,
	0x2e, 0x76, 0x31, 0x2e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63,
	0x61, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0a, 0x74, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x73, 0x42, 0x02, 0x68, 0x00, 0x1a, 0xa0, 0x01, 0x0a, 0x0f, 0x54, 0x61, 0x73,
	0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x14, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x73, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x59, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e,
	0x76, 0x31, 0x2e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61,
	0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x46, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x49, 0x6e, 0x66, 0x6f, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x02, 0x68,
	0x00, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0xad, 0x01, 0x0a, 0x0d, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x50, 0x0a, 0x0f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71,
	0x75, 0x65, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x24,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e,
	0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x54,
	0x79, 0x70, 0x65, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x54, 0x79, 0x70,
	0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4a, 0x0a, 0x11, 0x66, 0x69, 0x72, 0x73, 0x74, 0x5f, 0x70, 0x6f,
	0x6c, 0x6c, 0x65, 0x72, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0f, 0x66, 0x69, 0x72, 0x73, 0x74,
	0x50, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x22, 0xdd,
	0x01, 0x0a, 0x16, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x57, 0x6f, 0x72, 0x6b,
	0x66, 0x6c, 0x6f, 0x77, 0x41, 0x72, 0x67, 0x73, 0x12, 0x29, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x25, 0x0a, 0x0c, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x71, 0x0a, 0x16, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79,
	0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x64, 0x65, 0x70, 0x6c, 0x6f,
	0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65,
	0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x14, 0x64, 0x65,
	0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x4c, 0x6f, 0x63, 0x61, 0x6c, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x42, 0x02, 0x68, 0x00, 0x22, 0x9c, 0x01, 0x0a, 0x1a, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d,
	0x65, 0x6e, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x41,
	0x72, 0x67, 0x73, 0x12, 0x29, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x25, 0x0a, 0x0c,
	0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x49, 0x64, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x2c, 0x0a, 0x10, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x5f, 0x62, 0x75, 0x69,
	0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0e, 0x64, 0x65, 0x66, 0x61,
	0x75, 0x6c, 0x74, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x22, 0xea, 0x01,
	0x0a, 0x1e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72,
	0x49, 0x6e, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x41, 0x72, 0x67, 0x73, 0x12,
	0x2a, 0x0a, 0x0f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x50, 0x0a, 0x0f, 0x74, 0x61, 0x73,
	0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x24, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x54, 0x79, 0x70, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4a, 0x0a, 0x11, 0x66, 0x69, 0x72, 0x73,
	0x74, 0x5f, 0x70, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0f, 0x66,
	0x69, 0x72, 0x73, 0x74, 0x50, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x54, 0x69, 0x6d, 0x65, 0x42, 0x02, 0x68,
	0x00, 0x22, 0xb1, 0x01, 0x0a, 0x16, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x49, 0x0a, 0x08, 0x62,
	0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x29, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73,
	0x2e, 0x76, 0x31, 0x2e, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x42, 0x65,
	0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x52, 0x08, 0x62, 0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x4c, 0x0a, 0x0a, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x57,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x0a,
	0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x22, 0xed,
	0x01, 0x0a, 0x10, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x49, 0x6e,
	0x66, 0x6f, 0x12, 0x59, 0x0a, 0x11, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x64, 0x65, 0x70, 0x6c,
	0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79,
	0x6d, 0x65, 0x6e, 0x74, 0x52, 0x10, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x44, 0x65, 0x70, 0x6c,
	0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x12, 0x7e, 0x0a, 0x25, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x5f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x63, 0x6f,
	0x6e, 0x74, 0x61, 0x69, 0x6e, 0x73, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x57,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x21,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74,
	0x43, 0x6f, 0x6e, 0x74, 0x61, 0x69, 0x6e, 0x73, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x42, 0x02, 0x68, 0x00, 0x42, 0x34, 0x5a, 0x32, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x2f, 0x76, 0x31, 0x3b, 0x64, 0x65,
	0x70, 0x6c, 0x6f, 0x79, 0x6d, 0x65, 0x6e, 0x74, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
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

var file_temporal_server_api_deployment_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_temporal_server_api_deployment_v1_message_proto_goTypes = []interface{}{
	(*DeploymentLocalState)(nil),                     // 0: temporal.server.api.deployment.v1.DeploymentLocalState
	(*DeploymentWorkflowArgs)(nil),                   // 1: temporal.server.api.deployment.v1.DeploymentWorkflowArgs
	(*DeploymentNameWorkflowArgs)(nil),               // 2: temporal.server.api.deployment.v1.DeploymentNameWorkflowArgs
	(*RegisterWorkerInDeploymentArgs)(nil),           // 3: temporal.server.api.deployment.v1.RegisterWorkerInDeploymentArgs
	(*WorkflowVersioningInfo)(nil),                   // 4: temporal.server.api.deployment.v1.WorkflowVersioningInfo
	(*TaskRedirectInfo)(nil),                         // 5: temporal.server.api.deployment.v1.TaskRedirectInfo
	nil,                                              // 6: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamiliesEntry
	(*DeploymentLocalState_TaskQueueFamilyInfo)(nil), // 7: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo
	nil, // 8: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueuesEntry
	(*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo)(nil), // 9: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueueInfo
	(*v1.WorkerDeployment)(nil),                                    // 10: temporal.api.common.v1.WorkerDeployment
	(v11.TaskQueueType)(0),                                         // 11: temporal.api.enums.v1.TaskQueueType
	(*timestamppb.Timestamp)(nil),                                  // 12: google.protobuf.Timestamp
	(v11.VersioningBehavior)(0),                                    // 13: temporal.api.enums.v1.VersioningBehavior
}
var file_temporal_server_api_deployment_v1_message_proto_depIdxs = []int32{
	10, // 0: temporal.server.api.deployment.v1.DeploymentLocalState.worker_deployment:type_name -> temporal.api.common.v1.WorkerDeployment
	6,  // 1: temporal.server.api.deployment.v1.DeploymentLocalState.task_queue_families:type_name -> temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamiliesEntry
	0,  // 2: temporal.server.api.deployment.v1.DeploymentWorkflowArgs.deployment_local_state:type_name -> temporal.server.api.deployment.v1.DeploymentLocalState
	11, // 3: temporal.server.api.deployment.v1.RegisterWorkerInDeploymentArgs.task_queue_type:type_name -> temporal.api.enums.v1.TaskQueueType
	12, // 4: temporal.server.api.deployment.v1.RegisterWorkerInDeploymentArgs.first_poller_time:type_name -> google.protobuf.Timestamp
	13, // 5: temporal.server.api.deployment.v1.WorkflowVersioningInfo.behavior:type_name -> temporal.api.enums.v1.VersioningBehavior
	10, // 6: temporal.server.api.deployment.v1.WorkflowVersioningInfo.deployment:type_name -> temporal.api.common.v1.WorkerDeployment
	10, // 7: temporal.server.api.deployment.v1.TaskRedirectInfo.source_deployment:type_name -> temporal.api.common.v1.WorkerDeployment
	10, // 8: temporal.server.api.deployment.v1.TaskRedirectInfo.source_deployment_contains_task_queue:type_name -> temporal.api.common.v1.WorkerDeployment
	7,  // 9: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamiliesEntry.value:type_name -> temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo
	8,  // 10: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.task_queues:type_name -> temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueuesEntry
	9,  // 11: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueuesEntry.value:type_name -> temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueueInfo
	11, // 12: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueueInfo.task_queue_type:type_name -> temporal.api.enums.v1.TaskQueueType
	12, // 13: temporal.server.api.deployment.v1.DeploymentLocalState.TaskQueueFamilyInfo.TaskQueueInfo.first_poller_time:type_name -> google.protobuf.Timestamp
	14, // [14:14] is the sub-list for method output_type
	14, // [14:14] is the sub-list for method input_type
	14, // [14:14] is the sub-list for extension type_name
	14, // [14:14] is the sub-list for extension extendee
	0,  // [0:14] is the sub-list for field type_name
}

func init() { file_temporal_server_api_deployment_v1_message_proto_init() }
func file_temporal_server_api_deployment_v1_message_proto_init() {
	if File_temporal_server_api_deployment_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterWorkerInDeploymentArgs); i {
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
			switch v := v.(*WorkflowVersioningInfo); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskRedirectInfo); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentLocalState_TaskQueueFamilyInfo); i {
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
		file_temporal_server_api_deployment_v1_message_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeploymentLocalState_TaskQueueFamilyInfo_TaskQueueInfo); i {
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
			NumMessages:   10,
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
