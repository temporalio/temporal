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

	v1 "go.temporal.io/api/enums/v1"
	v11 "go.temporal.io/api/taskqueue/v1"
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
	// containing this build id. [cleanup-old-wv]
	AssignedBuildId string `protobuf:"bytes,2,opt,name=assigned_build_id,json=assignedBuildId,proto3,oneof"`
}

func (*TaskVersionDirective_UseAssignmentRules) isTaskVersionDirective_BuildId() {}

func (*TaskVersionDirective_AssignedBuildId) isTaskVersionDirective_BuildId() {}

type TaskQueueVersionInfoInternal struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Empty means unversioned.
	BuildId               string                                              `protobuf:"bytes,1,opt,name=build_id,json=buildId,proto3" json:"build_id,omitempty"`
	PhysicalTaskQueueInfo *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo `protobuf:"bytes,2,opt,name=physical_task_queue_info,json=physicalTaskQueueInfo,proto3" json:"physical_task_queue_info,omitempty"`
	// Only applicable to versioned workers (workers with `useVersioning=true`).
	TaskReachability v1.BuildIdTaskReachability `protobuf:"varint,3,opt,name=task_reachability,json=taskReachability,proto3,enum=temporal.api.enums.v1.BuildIdTaskReachability" json:"task_reachability,omitempty"`
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

func (x *TaskQueueVersionInfoInternal) GetBuildId() string {
	if x != nil {
		return x.BuildId
	}
	return ""
}

func (x *TaskQueueVersionInfoInternal) GetPhysicalTaskQueueInfo() *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo {
	if x != nil {
		return x.PhysicalTaskQueueInfo
	}
	return nil
}

func (x *TaskQueueVersionInfoInternal) GetTaskReachability() v1.BuildIdTaskReachability {
	if x != nil {
		return x.TaskReachability
	}
	return v1.BuildIdTaskReachability(0)
}

// Represents a normal or sticky partition of a task queue.
type TaskQueuePartition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// This is the user-facing name for this task queue
	TaskQueue     string           `protobuf:"bytes,1,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
	TaskQueueType v1.TaskQueueType `protobuf:"varint,2,opt,name=task_queue_type,json=taskQueueType,proto3,enum=temporal.api.enums.v1.TaskQueueType" json:"task_queue_type,omitempty"`
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
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueuePartition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueuePartition) ProtoMessage() {}

func (x *TaskQueuePartition) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use TaskQueuePartition.ProtoReflect.Descriptor instead.
func (*TaskQueuePartition) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{2}
}

func (x *TaskQueuePartition) GetTaskQueue() string {
	if x != nil {
		return x.TaskQueue
	}
	return ""
}

func (x *TaskQueuePartition) GetTaskQueueType() v1.TaskQueueType {
	if x != nil {
		return x.TaskQueueType
	}
	return v1.TaskQueueType(0)
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

func (x *TaskQueuePartition) GetStickyName() int32 {
	if x, ok := x.GetPartitionId().(*TaskQueuePartition_StickyName); ok {
		return x.StickyName
	}
	return 0
}

type isTaskQueuePartition_PartitionId interface {
	isTaskQueuePartition_PartitionId()
}

type TaskQueuePartition_NormalPartitionId struct {
	NormalPartitionId int32 `protobuf:"varint,3,opt,name=normal_partition_id,json=normalPartitionId,proto3,oneof"`
}

type TaskQueuePartition_StickyName struct {
	StickyName int32 `protobuf:"varint,4,opt,name=sticky_name,json=stickyName,proto3,oneof"`
}

func (*TaskQueuePartition_NormalPartitionId) isTaskQueuePartition_PartitionId() {}

func (*TaskQueuePartition_StickyName) isTaskQueuePartition_PartitionId() {}

type TaskQueuePartitionStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BacklogCountHint int64            `protobuf:"varint,1,opt,name=backlog_count_hint,json=backlogCountHint,proto3" json:"backlog_count_hint,omitempty"`
	ReadLevel        int64            `protobuf:"varint,2,opt,name=read_level,json=readLevel,proto3" json:"read_level,omitempty"`
	AckLevel         int64            `protobuf:"varint,3,opt,name=ack_level,json=ackLevel,proto3" json:"ack_level,omitempty"`
	RatePerSecond    float64          `protobuf:"fixed64,4,opt,name=rate_per_second,json=ratePerSecond,proto3" json:"rate_per_second,omitempty"`
	TaskIdBlock      *v11.TaskIdBlock `protobuf:"bytes,5,opt,name=task_id_block,json=taskIdBlock,proto3" json:"task_id_block,omitempty"`
}

func (x *TaskQueuePartitionStatus) Reset() {
	*x = TaskQueuePartitionStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueuePartitionStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueuePartitionStatus) ProtoMessage() {}

func (x *TaskQueuePartitionStatus) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use TaskQueuePartitionStatus.ProtoReflect.Descriptor instead.
func (*TaskQueuePartitionStatus) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{3}
}

func (x *TaskQueuePartitionStatus) GetBacklogCountHint() int64 {
	if x != nil {
		return x.BacklogCountHint
	}
	return 0
}

func (x *TaskQueuePartitionStatus) GetReadLevel() int64 {
	if x != nil {
		return x.ReadLevel
	}
	return 0
}

func (x *TaskQueuePartitionStatus) GetAckLevel() int64 {
	if x != nil {
		return x.AckLevel
	}
	return 0
}

func (x *TaskQueuePartitionStatus) GetRatePerSecond() float64 {
	if x != nil {
		return x.RatePerSecond
	}
	return 0
}

func (x *TaskQueuePartitionStatus) GetTaskIdBlock() *v11.TaskIdBlock {
	if x != nil {
		return x.TaskIdBlock
	}
	return nil
}

type TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Unversioned workers (with `useVersioning=false`) are reported in unversioned result even if they set a Build ID.
	Pollers     []*v11.PollerInfo         `protobuf:"bytes,1,rep,name=pollers,proto3" json:"pollers,omitempty"`
	Status      *TaskQueuePartitionStatus `protobuf:"bytes,2,opt,name=status,proto3" json:"status,omitempty"`
	BacklogInfo *v11.BacklogInfo          `protobuf:"bytes,3,opt,name=backlog_info,json=backlogInfo,proto3" json:"backlog_info,omitempty"`
}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) Reset() {
	*x = TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) ProtoMessage() {}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) ProtoReflect() protoreflect.Message {
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

// Deprecated: Use TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo.ProtoReflect.Descriptor instead.
func (*TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_taskqueue_v1_message_proto_rawDescGZIP(), []int{1, 0}
}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) GetPollers() []*v11.PollerInfo {
	if x != nil {
		return x.Pollers
	}
	return nil
}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) GetStatus() *TaskQueuePartitionStatus {
	if x != nil {
		return x.Status
	}
	return nil
}

func (x *TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo) GetBacklogInfo() *v11.BacklogInfo {
	if x != nil {
		return x.BacklogInfo
	}
	return nil
}

var File_temporal_server_api_taskqueue_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_taskqueue_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f,
	0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x20, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e,
	0x76, 0x31, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x27, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x74, 0x61,
	0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f,
	0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x9c, 0x01, 0x0a, 0x14, 0x54, 0x61, 0x73, 0x6b, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x69, 0x76, 0x65, 0x12, 0x4a, 0x0a, 0x14, 0x75, 0x73, 0x65,
	0x5f, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x72, 0x75, 0x6c, 0x65,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48,
	0x00, 0x52, 0x12, 0x75, 0x73, 0x65, 0x41, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x6d, 0x65, 0x6e, 0x74,
	0x52, 0x75, 0x6c, 0x65, 0x73, 0x12, 0x2c, 0x0a, 0x11, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x65,
	0x64, 0x5f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x48, 0x00, 0x52, 0x0f, 0x61, 0x73, 0x73, 0x69, 0x67, 0x6e, 0x65, 0x64, 0x42, 0x75, 0x69, 0x6c,
	0x64, 0x49, 0x64, 0x42, 0x0a, 0x0a, 0x08, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x22,
	0xa0, 0x04, 0x0a, 0x1c, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c,
	0x12, 0x19, 0x0a, 0x08, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x12, 0x8d, 0x01, 0x0a, 0x18,
	0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x54,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x56, 0x65, 0x72, 0x73, 0x69,
	0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2e, 0x50,
	0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x15, 0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61,
	0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x5b, 0x0a, 0x11, 0x74,
	0x61, 0x73, 0x6b, 0x5f, 0x72, 0x65, 0x61, 0x63, 0x68, 0x61, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2e, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x42,
	0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x61, 0x63, 0x68, 0x61,
	0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x10, 0x74, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x61, 0x63,
	0x68, 0x61, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x1a, 0xf7, 0x01, 0x0a, 0x15, 0x50, 0x68, 0x79,
	0x73, 0x69, 0x63, 0x61, 0x6c, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x6e,
	0x66, 0x6f, 0x12, 0x3f, 0x0a, 0x07, 0x70, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x50, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x07, 0x70, 0x6f, 0x6c, 0x6c,
	0x65, 0x72, 0x73, 0x12, 0x52, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x3a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52,
	0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x49, 0x0a, 0x0c, 0x62, 0x61, 0x63, 0x6b, 0x6c,
	0x6f, 0x67, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x26, 0x2e,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x74, 0x61, 0x73,
	0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x6c, 0x6f,
	0x67, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x0b, 0x62, 0x61, 0x63, 0x6b, 0x6c, 0x6f, 0x67, 0x49, 0x6e,
	0x66, 0x6f, 0x22, 0xe6, 0x01, 0x0a, 0x12, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x73,
	0x6b, 0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74,
	0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x4c, 0x0a, 0x0f, 0x74, 0x61, 0x73, 0x6b,
	0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x24, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0d, 0x74, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x30, 0x0a, 0x13, 0x6e, 0x6f, 0x72, 0x6d, 0x61, 0x6c,
	0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x05, 0x48, 0x00, 0x52, 0x11, 0x6e, 0x6f, 0x72, 0x6d, 0x61, 0x6c, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x21, 0x0a, 0x0b, 0x73, 0x74, 0x69, 0x63,
	0x6b, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x48, 0x00, 0x52,
	0x0a, 0x73, 0x74, 0x69, 0x63, 0x6b, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x42, 0x0e, 0x0a, 0x0c, 0x70,
	0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x22, 0xf8, 0x01, 0x0a, 0x18,
	0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x2c, 0x0a, 0x12, 0x62, 0x61, 0x63, 0x6b,
	0x6c, 0x6f, 0x67, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x68, 0x69, 0x6e, 0x74, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x10, 0x62, 0x61, 0x63, 0x6b, 0x6c, 0x6f, 0x67, 0x43, 0x6f, 0x75,
	0x6e, 0x74, 0x48, 0x69, 0x6e, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x61, 0x64, 0x5f, 0x6c,
	0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x72, 0x65, 0x61, 0x64,
	0x4c, 0x65, 0x76, 0x65, 0x6c, 0x12, 0x1b, 0x0a, 0x09, 0x61, 0x63, 0x6b, 0x5f, 0x6c, 0x65, 0x76,
	0x65, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x61, 0x63, 0x6b, 0x4c, 0x65, 0x76,
	0x65, 0x6c, 0x12, 0x26, 0x0a, 0x0f, 0x72, 0x61, 0x74, 0x65, 0x5f, 0x70, 0x65, 0x72, 0x5f, 0x73,
	0x65, 0x63, 0x6f, 0x6e, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x01, 0x52, 0x0d, 0x72, 0x61, 0x74,
	0x65, 0x50, 0x65, 0x72, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x12, 0x4a, 0x0a, 0x0d, 0x74, 0x61,
	0x73, 0x6b, 0x5f, 0x69, 0x64, 0x5f, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x26, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61,
	0x73, 0x6b, 0x49, 0x64, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x0b, 0x74, 0x61, 0x73, 0x6b, 0x49,
	0x64, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f,
	0x61, 0x70, 0x69, 0x2f, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x76, 0x31,
	0x3b, 0x74, 0x61, 0x73, 0x6b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
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

var file_temporal_server_api_taskqueue_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_temporal_server_api_taskqueue_v1_message_proto_goTypes = []interface{}{
	(*TaskVersionDirective)(nil),                               // 0: temporal.server.api.taskqueue.v1.TaskVersionDirective
	(*TaskQueueVersionInfoInternal)(nil),                       // 1: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal
	(*TaskQueuePartition)(nil),                                 // 2: temporal.server.api.taskqueue.v1.TaskQueuePartition
	(*TaskQueuePartitionStatus)(nil),                           // 3: temporal.server.api.taskqueue.v1.TaskQueuePartitionStatus
	(*TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo)(nil), // 4: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.PhysicalTaskQueueInfo
	(*emptypb.Empty)(nil),                                      // 5: google.protobuf.Empty
	(v1.BuildIdTaskReachability)(0),                            // 6: temporal.api.enums.v1.BuildIdTaskReachability
	(v1.TaskQueueType)(0),                                      // 7: temporal.api.enums.v1.TaskQueueType
	(*v11.TaskIdBlock)(nil),                                    // 8: temporal.api.taskqueue.v1.TaskIdBlock
	(*v11.PollerInfo)(nil),                                     // 9: temporal.api.taskqueue.v1.PollerInfo
	(*v11.BacklogInfo)(nil),                                    // 10: temporal.api.taskqueue.v1.BacklogInfo
}
var file_temporal_server_api_taskqueue_v1_message_proto_depIdxs = []int32{
	5,  // 0: temporal.server.api.taskqueue.v1.TaskVersionDirective.use_assignment_rules:type_name -> google.protobuf.Empty
	4,  // 1: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.physical_task_queue_info:type_name -> temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.PhysicalTaskQueueInfo
	6,  // 2: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.task_reachability:type_name -> temporal.api.enums.v1.BuildIdTaskReachability
	7,  // 3: temporal.server.api.taskqueue.v1.TaskQueuePartition.task_queue_type:type_name -> temporal.api.enums.v1.TaskQueueType
	8,  // 4: temporal.server.api.taskqueue.v1.TaskQueuePartitionStatus.task_id_block:type_name -> temporal.api.taskqueue.v1.TaskIdBlock
	9,  // 5: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.PhysicalTaskQueueInfo.pollers:type_name -> temporal.api.taskqueue.v1.PollerInfo
	3,  // 6: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.PhysicalTaskQueueInfo.status:type_name -> temporal.server.api.taskqueue.v1.TaskQueuePartitionStatus
	10, // 7: temporal.server.api.taskqueue.v1.TaskQueueVersionInfoInternal.PhysicalTaskQueueInfo.backlog_info:type_name -> temporal.api.taskqueue.v1.BacklogInfo
	8,  // [8:8] is the sub-list for method output_type
	8,  // [8:8] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
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
		file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TaskQueuePartitionStatus); i {
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
			switch v := v.(*TaskQueueVersionInfoInternal_PhysicalTaskQueueInfo); i {
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
	file_temporal_server_api_taskqueue_v1_message_proto_msgTypes[2].OneofWrappers = []interface{}{
		(*TaskQueuePartition_NormalPartitionId)(nil),
		(*TaskQueuePartition_StickyName)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_taskqueue_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
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
