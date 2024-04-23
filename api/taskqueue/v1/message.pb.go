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
// source: temporal/server/api/taskqueue/v1/message.proto

package taskqueue

import (
	reflect "reflect"
	sync "sync"

	v11 "go.temporal.io/api/enums/v1"
	v1 "go.temporal.io/api/taskqueue/v1"
	v12 "go.temporal.io/server/api/enums/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// TaskVersionDirective controls how matching should direct a task.
type TaskVersionDirective struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Default (if build_id is not present) is "unversioned":
	// Use the unversioned task queue, even if the task queue has versioning data.
	// Absent value means the task is the non-starting task of an unversioned execution so it should remain unversioned.
	//
	// Types that are assignable to BuildId:
	//
	//	*TaskVersionDirective_UseAssignmentRules
	//	*TaskVersionDirective_AssignedBuildId
	BuildId isTaskVersionDirective_BuildId `protobuf_oneof:"build_id"`
}

func (x *TaskVersionDirective) Reset() {
	*x = TaskVersionDirective{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskVersionDirective) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskVersionDirective) ProtoMessage() {}

func (x *TaskVersionDirective) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskVersionDirective.ProtoReflect.Descriptor instead.
func (*TaskVersionDirective) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{0}
}

func (m *TaskVersionDirective) GetBuildId() isTaskVersionDirective_BuildId {
	if m != nil {
		return m.BuildId
	}
	return nil
}

func (x *TaskVersionDirective) GetUseAssignmentRules() *emptypb.Empty {
	if x, ok := x.GetBuildId().(*TaskVersionDirective_UseAssignmentRules); ok {
		return x.UseAssignmentRules
	}
	return nil
}

func (x *TaskVersionDirective) GetAssignedBuildId() string {
	if x, ok := x.GetBuildId().(*TaskVersionDirective_AssignedBuildId); ok {
		return x.AssignedBuildId
	}
	return ""
}

type isTaskVersionDirective_BuildId interface {
	isTaskVersionDirective_BuildId()
}

type TaskVersionDirective_UseAssignmentRules struct {
	// If use_assignment_rules is present, matching should use the assignment rules
	// to determine the build ID.
	// WV1: the task should be assigned the default version for the task queue. [cleanup-old-wv]
	UseAssignmentRules *emptypb.Empty `protobuf:"bytes,1,opt,name=use_assignment_rules,json=useAssignmentRules,proto3,oneof"`
}

type TaskVersionDirective_AssignedBuildId struct {
	// This means the task is already assigned to `build_id`
	// WV1: If assigned_build_id is present, use the default version in the compatible set
	// containing this build ID. [cleanup-old-wv]
	AssignedBuildId string `protobuf:"bytes,2,opt,name=assigned_build_id,json=assignedBuildId,proto3,oneof"`
}

func (*TaskVersionDirective_UseAssignmentRules) isTaskVersionDirective_BuildId() {}

func (*TaskVersionDirective_AssignedBuildId) isTaskVersionDirective_BuildId() {}

type TaskQueueVersionInfoInternal struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PhysicalTaskQueueInfo *PhysicalTaskQueueInfo `protobuf:"bytes,2,opt,name=physical_task_queue_info,json=physicalTaskQueueInfo,proto3" json:"physical_task_queue_info,omitempty"`
}

func (x *TaskQueueVersionInfoInternal) Reset() {
	*x = TaskQueueVersionInfoInternal{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueueVersionInfoInternal) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueueVersionInfoInternal) ProtoMessage() {}

func (x *TaskQueueVersionInfoInternal) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskQueueVersionInfoInternal.ProtoReflect.Descriptor instead.
func (*TaskQueueVersionInfoInternal) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *TaskQueueVersionInfoInternal) GetPhysicalTaskQueueInfo() *PhysicalTaskQueueInfo {
	if x != nil {
		return x.PhysicalTaskQueueInfo
	}
	return nil
}

type PhysicalTaskQueueInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Unversioned workers (with `useVersioning=false`) are reported in unversioned result even if they set a Build ID.
	Pollers     []*v1.PollerInfo `protobuf:"bytes,1,rep,name=pollers,proto3" json:"pollers,omitempty"`
	BacklogInfo *v1.BacklogInfo  `protobuf:"bytes,2,opt,name=backlog_info,json=backlogInfo,proto3" json:"backlog_info,omitempty"`
}

func (x *PhysicalTaskQueueInfo) Reset() {
	*x = PhysicalTaskQueueInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PhysicalTaskQueueInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PhysicalTaskQueueInfo) ProtoMessage() {}

func (x *PhysicalTaskQueueInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PhysicalTaskQueueInfo.ProtoReflect.Descriptor instead.
func (*PhysicalTaskQueueInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *PhysicalTaskQueueInfo) GetPollers() []*v1.PollerInfo {
	if x != nil {
		return x.Pollers
	}
	return nil
}

func (x *PhysicalTaskQueueInfo) GetBacklogInfo() *v1.BacklogInfo {
	if x != nil {
		return x.BacklogInfo
	}
	return nil
}

// Represents a normal or sticky partition of a task queue.
type TaskQueuePartition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// This is the user-facing name for this task queue
	TaskQueue     string            `protobuf:"bytes,1,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
	TaskQueueType v11.TaskQueueType `protobuf:"varint,2,opt,name=task_queue_type,json=taskQueueType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_queue_type,omitempty"`
	// Absent means normal root partition (normal_partition_id=0)
	//
	// Types that are assignable to PartitionId:
	//
	//	*TaskQueuePartition_NormalPartitionId
	//	*TaskQueuePartition_StickyName
	PartitionId isTaskQueuePartition_PartitionId `protobuf_oneof:"partition_id"`
}

func (x *TaskQueuePartition) Reset() {
	*x = TaskQueuePartition{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueuePartition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueuePartition) ProtoMessage() {}

func (x *TaskQueuePartition) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskQueuePartition.ProtoReflect.Descriptor instead.
func (*TaskQueuePartition) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *TaskQueuePartition) GetTaskQueue() string {
	if x != nil {
		return x.TaskQueue
	}
	return ""
}

func (x *TaskQueuePartition) GetTaskQueueType() v11.TaskQueueType {
	if x != nil {
		return x.TaskQueueType
	}
	return v11.TaskQueueType(0)
}

func (m *TaskQueuePartition) GetPartitionId() isTaskQueuePartition_PartitionId {
	if m != nil {
		return m.PartitionId
	}
	return nil
}

func (x *TaskQueuePartition) GetNormalPartitionId() int32 {
	if x, ok := x.GetPartitionId().(*TaskQueuePartition_NormalPartitionId); ok {
		return x.NormalPartitionId
	}
	return 0
}

func (x *TaskQueuePartition) GetStickyName() string {
	if x, ok := x.GetPartitionId().(*TaskQueuePartition_StickyName); ok {
		return x.StickyName
	}
	return ""
}

type isTaskQueuePartition_PartitionId interface {
	isTaskQueuePartition_PartitionId()
}

type TaskQueuePartition_NormalPartitionId struct {
	NormalPartitionId int32 `protobuf:"varint,3,opt,name=normal_partition_id,json=normalPartitionId,proto3,oneof"`
}

type TaskQueuePartition_StickyName struct {
	StickyName string `protobuf:"bytes,4,opt,name=sticky_name,json=stickyName,proto3,oneof"`
}

func (*TaskQueuePartition_NormalPartitionId) isTaskQueuePartition_PartitionId() {}

func (*TaskQueuePartition_StickyName) isTaskQueuePartition_PartitionId() {}

// Information about redirect intention sent by Matching to History in Record*TaskStarted calls
type BuildIdRedirectInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// build ID asked by History in the directive or the one calculated based on the assignment rules.
	// this is the source of the redirect rule chain applied. (the target of the redirect rule chain is
	// the poller's build ID reported in WorkerVersionCapabilities)
	AssignedBuildId string `protobuf:"bytes,1,opt,name=assigned_build_id,json=assignedBuildId,proto3" json:"assigned_build_id,omitempty"`
}

func (x *BuildIdRedirectInfo) Reset() {
	*x = BuildIdRedirectInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BuildIdRedirectInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BuildIdRedirectInfo) ProtoMessage() {}

func (x *BuildIdRedirectInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BuildIdRedirectInfo.ProtoReflect.Descriptor instead.
func (*BuildIdRedirectInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{4}
}

func (x *BuildIdRedirectInfo) GetAssignedBuildId() string {
	if x != nil {
		return x.AssignedBuildId
	}
	return ""
}

// Information about task forwarding from one partition to its parent. Versioning decisions are made
// at the source partition and sent to the parent partition in this message.
type TaskForwardInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// RPC name of the partition forwarded the task.
	// In case of multiple hops, this is the source partition of the last hop.
	SourcePartition string         `protobuf:"bytes,1,opt,name=source_partition,json=sourcePartition,proto3" json:"source_partition,omitempty"`
	TaskSource      v12.TaskSource `protobuf:"varint,2,opt,name=task_source,json=taskSource,proto3,enum=temporal.server.api.enums.v1.TaskSource" json:"task_source,omitempty"`
	// Redirect info is not present for Query and Nexus tasks.
	RedirectInfo *BuildIdRedirectInfo `protobuf:"bytes,3,opt,name=redirect_info,json=redirectInfo,proto3" json:"redirect_info,omitempty"`
	// Build ID that should be used to dispatch the task to.
	DispatchBuildId string `protobuf:"bytes,4,opt,name=dispatch_build_id,json=dispatchBuildId,proto3" json:"dispatch_build_id,omitempty"`
	// Only used for old versioning. [cleanup-old-wv]
	DispatchVersionSet string `protobuf:"bytes,5,opt,name=dispatch_version_set,json=dispatchVersionSet,proto3" json:"dispatch_version_set,omitempty"`
}

func (x *TaskForwardInfo) Reset() {
	*x = TaskForwardInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskForwardInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskForwardInfo) ProtoMessage() {}

func (x *TaskForwardInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TaskForwardInfo.ProtoReflect.Descriptor instead.
func (*TaskForwardInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{5}
}

func (x *TaskForwardInfo) GetSourcePartition() string {
	if x != nil {
		return x.SourcePartition
	}
	return ""
}

func (x *TaskForwardInfo) GetTaskSource() v12.TaskSource {
	if x != nil {
		return x.TaskSource
	}
	return v12.TaskSource(0)
}

func (x *TaskForwardInfo) GetRedirectInfo() *BuildIdRedirectInfo {
	if x != nil {
		return x.RedirectInfo
	}
	return nil
}

func (x *TaskForwardInfo) GetDispatchBuildId() string {
	if x != nil {
		return x.DispatchBuildId
	}
	return ""
}

func (x *TaskForwardInfo) GetDispatchVersionSet() string {
	if x != nil {
		return x.DispatchVersionSet
	}
	return ""
}

var File_temporal_server_api_taskqueue_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_taskqueue_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x76, 0x31,
	0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x20, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x1a, 0x1b,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65,
	0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x27, 0x74, 0x65, 0x6d, 0x70,
	0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65,
	0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x27, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75,
	0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0xa4, 0x01, 0x0a, 0x14, 0x54, 0x61, 0x73, 0x6b, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x44, 0x69,
	0x72, 0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x12, 0x4e, 0x0a, 0x14, 0x75, 0x73, 0x65, 0x5f, 0x61, 0x73,
	0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x75, 0x6c, 0x65, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48, 0x00, 0x52, 0x12, 0x75, 0x73,
	0x65, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x52, 0x75, 0x6c, 0x65, 0x73,
	0x42, 0x02, 0x68, 0x00, 0x12, 0x30, 0x0a, 0x11, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x5f,
	0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52,
	0x0f, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64,
	0x42, 0x02, 0x68, 0x00, 0x42, 0x0a, 0x0a, 0x08, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x22,
	0x94, 0x01, 0x0a, 0x1c, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x12,
	0x74, 0x0a, 0x18, 0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x37, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x15, 0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54,
	0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x42, 0x02, 0x68, 0x00,
	0x22, 0xab, 0x01, 0x0a, 0x15, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x73, 0x6b,
	0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x43, 0x0a, 0x07, 0x70, 0x6f, 0x6c, 0x6c,
	0x65, 0x72, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x74, 0x65, 0x6d, 0x70,
	0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x07,
	0x70, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x73, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4d, 0x0a, 0x0c, 0x62,
	0x61, 0x63, 0x6b, 0x6c, 0x6f, 0x67, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x26, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74,
	0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x6c,
	0x6f, 0x67, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x0b, 0x62, 0x61, 0x63, 0x6b, 0x6c, 0x6f, 0x67, 0x49,
	0x6e, 0x66, 0x6f, 0x42, 0x02, 0x68, 0x00, 0x22, 0xf6, 0x01, 0x0a, 0x12, 0x54, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a, 0x0a,
	0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x50,
	0x0a, 0x0f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x24, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61,
	0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b,
	0x51, 0x75, 0x65, 0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x34, 0x0a,
	0x13, 0x6e, 0x6f, 0x72, 0x6d, 0x61, 0x6c, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x48, 0x00, 0x52, 0x11, 0x6e, 0x6f, 0x72, 0x6d,
	0x61, 0x6c, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00,
	0x12, 0x25, 0x0a, 0x0b, 0x73, 0x74, 0x69, 0x63, 0x6b, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0a, 0x73, 0x74, 0x69, 0x63, 0x6b, 0x79, 0x4e, 0x61,
	0x6d, 0x65, 0x42, 0x02, 0x68, 0x00, 0x42, 0x0e, 0x0a, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x22, 0x45, 0x0a, 0x13, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64,
	0x52, 0x65, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x2e, 0x0a, 0x11, 0x61,
	0x73, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x5f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x42,
	0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x22, 0xd5, 0x02, 0x0a, 0x0f, 0x54, 0x61,
	0x73, 0x6b, 0x46, 0x6f, 0x72, 0x77, 0x61, 0x72, 0x64, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x2d, 0x0a, 0x10,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x4d, 0x0a, 0x0b, 0x74, 0x61, 0x73,
	0x6b, 0x5f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x28, 0x2e,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b,
	0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x0a, 0x74, 0x61, 0x73, 0x6b, 0x53, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x42, 0x02, 0x68, 0x00, 0x12, 0x5e, 0x0a, 0x0d, 0x72, 0x65, 0x64, 0x69, 0x72, 0x65, 0x63,
	0x74, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x35, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x75,
	0x69, 0x6c, 0x64, 0x49, 0x64, 0x52, 0x65, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x49, 0x6e, 0x66, 0x6f,
	0x52, 0x0c, 0x72, 0x65, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x42, 0x02, 0x68,
	0x00, 0x12, 0x2e, 0x0a, 0x11, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x64, 0x69, 0x73,
	0x70, 0x61, 0x74, 0x63, 0x68, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12,
	0x34, 0x0a, 0x14, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x5f, 0x73, 0x65, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12, 0x64, 0x69,
	0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x74, 0x42,
	0x02, 0x68, 0x00, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x76, 0x31, 0x3b, 0x74, 0x61, 0x73, 0x6b,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_taskqueue_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_taskqueue_v1_message_proto_rawDescData = file_temporal_server_api_taskqueue_v1_message_proto_rawDesc
)

func file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_taskqueue_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_taskqueue_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_taskqueue_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescData
}

var file_temporal_server_api_taskqueue_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_temporal_server_api_taskqueue_v1_message_proto_goTypes = []interface{}{
	(*TaskVersionDirective)(nil),         // 0: temporal.server.api.taskqueue.v1.TaskVersionDirective
	(*TaskQueueVersionInfoInternal)(nil), // 1: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal
	(*PhysicalTaskQueueInfo)(nil),        // 2: temporal.server.api.taskqueue.v1.PhysicalTaskQueueInfo
	(*TaskQueuePartition)(nil),           // 3: temporal.server.api.taskqueue.v1.TaskQueuePartition
	(*BuildIdRedirectInfo)(nil),          // 4: temporal.server.api.taskqueue.v1.BuildIdRedirectInfo
	(*TaskForwardInfo)(nil),              // 5: temporal.server.api.taskqueue.v1.TaskForwardInfo
	(*emptypb.Empty)(nil),                // 6: google.protobuf.Empty
	(*v1.PollerInfo)(nil),                // 7: temporal.api.taskqueue.v1.PollerInfo
	(*v1.BacklogInfo)(nil),               // 8: temporal.api.taskqueue.v1.BacklogInfo
	(v11.TaskQueueType)(0),               // 9: temporal.api.enums.v1.TaskQueueType
	(v12.TaskSource)(0),                  // 10: temporal.server.api.enums.v1.TaskSource
}
var file_temporal_server_api_taskqueue_v1_message_proto_depIdxs = []int32{
	6,  // 0: temporal.server.api.taskqueue.v1.TaskVersionDirective.use_assignment_rules:type_name -> google.protobuf.Empty
	2,  // 1: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.physical_task_queue_info:type_name -> temporal.server.api.taskqueue.v1.PhysicalTaskQueueInfo
	7,  // 2: temporal.server.api.taskqueue.v1.PhysicalTaskQueueInfo.pollers:type_name -> temporal.api.taskqueue.v1.PollerInfo
	8,  // 3: temporal.server.api.taskqueue.v1.PhysicalTaskQueueInfo.backlog_info:type_name -> temporal.api.taskqueue.v1.BacklogInfo
	9,  // 4: temporal.server.api.taskqueue.v1.TaskQueuePartition.task_queue_type:type_name -> temporal.api.enums.v1.TaskQueueType
	10, // 5: temporal.server.api.taskqueue.v1.TaskForwardInfo.task_source:type_name -> temporal.server.api.enums.v1.TaskSource
	4,  // 6: temporal.server.api.taskqueue.v1.TaskForwardInfo.redirect_info:type_name -> temporal.server.api.taskqueue.v1.BuildIdRedirectInfo
	7,  // [7:7] is the sub-list for method output_type
	7,  // [7:7] is the sub-list for method input_type
	7,  // [7:7] is the sub-list for extension type_name
	7,  // [7:7] is the sub-list for extension extendee
	0,  // [0:7] is the sub-list for field type_name
}

func init() { file_temporal_server_api_taskqueue_v1_message_proto_init() }
func file_temporal_server_api_taskqueue_v1_message_proto_init() {
	if File_temporal_server_api_taskqueue_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskVersionDirective); i {
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskQueueVersionInfoInternal); i {
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PhysicalTaskQueueInfo); i {
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskQueuePartition); i {
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BuildIdRedirectInfo); i {
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskForwardInfo); i {
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
	file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*TaskVersionDirective_UseAssignmentRules)(nil),
		(*TaskVersionDirective_AssignedBuildId)(nil),
	}
	file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*TaskQueuePartition_NormalPartitionId)(nil),
		(*TaskQueuePartition_StickyName)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_taskqueue_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_taskqueue_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_taskqueue_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_taskqueue_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_taskqueue_v1_message_proto = out.File
	file_temporal_server_api_taskqueue_v1_message_proto_rawDesc = nil
	file_temporal_server_api_taskqueue_v1_message_proto_goTypes = nil
	file_temporal_server_api_taskqueue_v1_message_proto_depIdxs = nil
}
