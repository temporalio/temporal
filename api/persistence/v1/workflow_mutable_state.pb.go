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
// source: temporal/server/api/persistence/v1/workflow_mutable_state.proto

package persistence

import (
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"

	v1 "go.temporal.io/api/history/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type WorkflowMutableState struct {
	state               protoimpl.MessageState        `protogen:"open.v1"`
	ActivityInfos       map[int64]*ActivityInfo       `protobuf:"bytes,1,rep,name=activity_infos,json=activityInfos,proto3" json:"activity_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	TimerInfos          map[string]*TimerInfo         `protobuf:"bytes,2,rep,name=timer_infos,json=timerInfos,proto3" json:"timer_infos,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	ChildExecutionInfos map[int64]*ChildExecutionInfo `protobuf:"bytes,3,rep,name=child_execution_infos,json=childExecutionInfos,proto3" json:"child_execution_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	RequestCancelInfos  map[int64]*RequestCancelInfo  `protobuf:"bytes,4,rep,name=request_cancel_infos,json=requestCancelInfos,proto3" json:"request_cancel_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	SignalInfos         map[int64]*SignalInfo         `protobuf:"bytes,5,rep,name=signal_infos,json=signalInfos,proto3" json:"signal_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	ChasmNodes          map[string]*ChasmNode         `protobuf:"bytes,12,rep,name=chasm_nodes,json=chasmNodes,proto3" json:"chasm_nodes,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	SignalRequestedIds  []string                      `protobuf:"bytes,6,rep,name=signal_requested_ids,json=signalRequestedIds,proto3" json:"signal_requested_ids,omitempty"`
	ExecutionInfo       *WorkflowExecutionInfo        `protobuf:"bytes,7,opt,name=execution_info,json=executionInfo,proto3" json:"execution_info,omitempty"`
	ExecutionState      *WorkflowExecutionState       `protobuf:"bytes,8,opt,name=execution_state,json=executionState,proto3" json:"execution_state,omitempty"`
	NextEventId         int64                         `protobuf:"varint,9,opt,name=next_event_id,json=nextEventId,proto3" json:"next_event_id,omitempty"`
	BufferedEvents      []*v1.HistoryEvent            `protobuf:"bytes,10,rep,name=buffered_events,json=bufferedEvents,proto3" json:"buffered_events,omitempty"`
	Checksum            *Checksum                     `protobuf:"bytes,11,opt,name=checksum,proto3" json:"checksum,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *WorkflowMutableState) Reset() {
	*x = WorkflowMutableState{}
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkflowMutableState) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkflowMutableState) ProtoMessage() {}

func (x *WorkflowMutableState) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkflowMutableState.ProtoReflect.Descriptor instead.
func (*WorkflowMutableState) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescGZIP(), []int{0}
}

func (x *WorkflowMutableState) GetActivityInfos() map[int64]*ActivityInfo {
	if x != nil {
		return x.ActivityInfos
	}
	return nil
}

func (x *WorkflowMutableState) GetTimerInfos() map[string]*TimerInfo {
	if x != nil {
		return x.TimerInfos
	}
	return nil
}

func (x *WorkflowMutableState) GetChildExecutionInfos() map[int64]*ChildExecutionInfo {
	if x != nil {
		return x.ChildExecutionInfos
	}
	return nil
}

func (x *WorkflowMutableState) GetRequestCancelInfos() map[int64]*RequestCancelInfo {
	if x != nil {
		return x.RequestCancelInfos
	}
	return nil
}

func (x *WorkflowMutableState) GetSignalInfos() map[int64]*SignalInfo {
	if x != nil {
		return x.SignalInfos
	}
	return nil
}

func (x *WorkflowMutableState) GetChasmNodes() map[string]*ChasmNode {
	if x != nil {
		return x.ChasmNodes
	}
	return nil
}

func (x *WorkflowMutableState) GetSignalRequestedIds() []string {
	if x != nil {
		return x.SignalRequestedIds
	}
	return nil
}

func (x *WorkflowMutableState) GetExecutionInfo() *WorkflowExecutionInfo {
	if x != nil {
		return x.ExecutionInfo
	}
	return nil
}

func (x *WorkflowMutableState) GetExecutionState() *WorkflowExecutionState {
	if x != nil {
		return x.ExecutionState
	}
	return nil
}

func (x *WorkflowMutableState) GetNextEventId() int64 {
	if x != nil {
		return x.NextEventId
	}
	return 0
}

func (x *WorkflowMutableState) GetBufferedEvents() []*v1.HistoryEvent {
	if x != nil {
		return x.BufferedEvents
	}
	return nil
}

func (x *WorkflowMutableState) GetChecksum() *Checksum {
	if x != nil {
		return x.Checksum
	}
	return nil
}

type WorkflowMutableStateMutation struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The following updated_* fields are computed based on the
	// lastUpdateVersionedTransition field of each sub state machine.
	UpdatedActivityInfos       map[int64]*ActivityInfo                                  `protobuf:"bytes,1,rep,name=updated_activity_infos,json=updatedActivityInfos,proto3" json:"updated_activity_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedTimerInfos          map[string]*TimerInfo                                    `protobuf:"bytes,2,rep,name=updated_timer_infos,json=updatedTimerInfos,proto3" json:"updated_timer_infos,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedChildExecutionInfos map[int64]*ChildExecutionInfo                            `protobuf:"bytes,3,rep,name=updated_child_execution_infos,json=updatedChildExecutionInfos,proto3" json:"updated_child_execution_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedRequestCancelInfos  map[int64]*RequestCancelInfo                             `protobuf:"bytes,4,rep,name=updated_request_cancel_infos,json=updatedRequestCancelInfos,proto3" json:"updated_request_cancel_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedSignalInfos         map[int64]*SignalInfo                                    `protobuf:"bytes,5,rep,name=updated_signal_infos,json=updatedSignalInfos,proto3" json:"updated_signal_infos,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedUpdateInfos         map[string]*UpdateInfo                                   `protobuf:"bytes,6,rep,name=updated_update_infos,json=updatedUpdateInfos,proto3" json:"updated_update_infos,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	UpdatedSubStateMachines    []*WorkflowMutableStateMutation_StateMachineNodeMutation `protobuf:"bytes,7,rep,name=updated_sub_state_machines,json=updatedSubStateMachines,proto3" json:"updated_sub_state_machines,omitempty"`
	UpdatedChasmNodes          map[string]*ChasmNode                                    `protobuf:"bytes,19,rep,name=updated_chasm_nodes,json=updatedChasmNodes,proto3" json:"updated_chasm_nodes,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	SignalRequestedIds         []string                                                 `protobuf:"bytes,15,rep,name=signal_requested_ids,json=signalRequestedIds,proto3" json:"signal_requested_ids,omitempty"`
	// Partial WorkflowExecutionInfo. Some fields, such as
	// update_infos and sub_state_machines_by_type, are not populated here.
	// Instead, only diffs are synced in the deleted_* and updated_* fields above.
	ExecutionInfo                   *WorkflowExecutionInfo        `protobuf:"bytes,16,opt,name=execution_info,json=executionInfo,proto3" json:"execution_info,omitempty"`
	ExecutionState                  *WorkflowExecutionState       `protobuf:"bytes,17,opt,name=execution_state,json=executionState,proto3" json:"execution_state,omitempty"`
	SubStateMachineTombstoneBatches []*StateMachineTombstoneBatch `protobuf:"bytes,18,rep,name=sub_state_machine_tombstone_batches,json=subStateMachineTombstoneBatches,proto3" json:"sub_state_machine_tombstone_batches,omitempty"`
	unknownFields                   protoimpl.UnknownFields
	sizeCache                       protoimpl.SizeCache
}

func (x *WorkflowMutableStateMutation) Reset() {
	*x = WorkflowMutableStateMutation{}
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkflowMutableStateMutation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkflowMutableStateMutation) ProtoMessage() {}

func (x *WorkflowMutableStateMutation) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkflowMutableStateMutation.ProtoReflect.Descriptor instead.
func (*WorkflowMutableStateMutation) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescGZIP(), []int{1}
}

func (x *WorkflowMutableStateMutation) GetUpdatedActivityInfos() map[int64]*ActivityInfo {
	if x != nil {
		return x.UpdatedActivityInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedTimerInfos() map[string]*TimerInfo {
	if x != nil {
		return x.UpdatedTimerInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedChildExecutionInfos() map[int64]*ChildExecutionInfo {
	if x != nil {
		return x.UpdatedChildExecutionInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedRequestCancelInfos() map[int64]*RequestCancelInfo {
	if x != nil {
		return x.UpdatedRequestCancelInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedSignalInfos() map[int64]*SignalInfo {
	if x != nil {
		return x.UpdatedSignalInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedUpdateInfos() map[string]*UpdateInfo {
	if x != nil {
		return x.UpdatedUpdateInfos
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedSubStateMachines() []*WorkflowMutableStateMutation_StateMachineNodeMutation {
	if x != nil {
		return x.UpdatedSubStateMachines
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetUpdatedChasmNodes() map[string]*ChasmNode {
	if x != nil {
		return x.UpdatedChasmNodes
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetSignalRequestedIds() []string {
	if x != nil {
		return x.SignalRequestedIds
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetExecutionInfo() *WorkflowExecutionInfo {
	if x != nil {
		return x.ExecutionInfo
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetExecutionState() *WorkflowExecutionState {
	if x != nil {
		return x.ExecutionState
	}
	return nil
}

func (x *WorkflowMutableStateMutation) GetSubStateMachineTombstoneBatches() []*StateMachineTombstoneBatch {
	if x != nil {
		return x.SubStateMachineTombstoneBatches
	}
	return nil
}

type WorkflowMutableStateMutation_StateMachineNodeMutation struct {
	state                         protoimpl.MessageState `protogen:"open.v1"`
	Path                          *StateMachinePath      `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	Data                          []byte                 `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	InitialVersionedTransition    *VersionedTransition   `protobuf:"bytes,3,opt,name=initial_versioned_transition,json=initialVersionedTransition,proto3" json:"initial_versioned_transition,omitempty"`
	LastUpdateVersionedTransition *VersionedTransition   `protobuf:"bytes,4,opt,name=last_update_versioned_transition,json=lastUpdateVersionedTransition,proto3" json:"last_update_versioned_transition,omitempty"`
	unknownFields                 protoimpl.UnknownFields
	sizeCache                     protoimpl.SizeCache
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) Reset() {
	*x = WorkflowMutableStateMutation_StateMachineNodeMutation{}
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WorkflowMutableStateMutation_StateMachineNodeMutation) ProtoMessage() {}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WorkflowMutableStateMutation_StateMachineNodeMutation.ProtoReflect.Descriptor instead.
func (*WorkflowMutableStateMutation_StateMachineNodeMutation) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescGZIP(), []int{1, 0}
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) GetPath() *StateMachinePath {
	if x != nil {
		return x.Path
	}
	return nil
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) GetInitialVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.InitialVersionedTransition
	}
	return nil
}

func (x *WorkflowMutableStateMutation_StateMachineNodeMutation) GetLastUpdateVersionedTransition() *VersionedTransition {
	if x != nil {
		return x.LastUpdateVersionedTransition
	}
	return nil
}

var File_temporal_server_api_persistence_v1_workflow_mutable_state_proto protoreflect.FileDescriptor

const file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDesc = "" +
	"\n" +
	"?temporal/server/api/persistence/v1/workflow_mutable_state.proto\x12\"temporal.server.api.persistence.v1\x1a%temporal/api/history/v1/message.proto\x1a3temporal/server/api/persistence/v1/executions.proto\x1a,temporal/server/api/persistence/v1/hsm.proto\x1a.temporal/server/api/persistence/v1/chasm.proto\x1a/temporal/server/api/persistence/v1/update.proto\"\xd0\x0e\n" +
	"\x14WorkflowMutableState\x12r\n" +
	"\x0eactivity_infos\x18\x01 \x03(\v2K.temporal.server.api.persistence.v1.WorkflowMutableState.ActivityInfosEntryR\ractivityInfos\x12i\n" +
	"\vtimer_infos\x18\x02 \x03(\v2H.temporal.server.api.persistence.v1.WorkflowMutableState.TimerInfosEntryR\n" +
	"timerInfos\x12\x85\x01\n" +
	"\x15child_execution_infos\x18\x03 \x03(\v2Q.temporal.server.api.persistence.v1.WorkflowMutableState.ChildExecutionInfosEntryR\x13childExecutionInfos\x12\x82\x01\n" +
	"\x14request_cancel_infos\x18\x04 \x03(\v2P.temporal.server.api.persistence.v1.WorkflowMutableState.RequestCancelInfosEntryR\x12requestCancelInfos\x12l\n" +
	"\fsignal_infos\x18\x05 \x03(\v2I.temporal.server.api.persistence.v1.WorkflowMutableState.SignalInfosEntryR\vsignalInfos\x12i\n" +
	"\vchasm_nodes\x18\f \x03(\v2H.temporal.server.api.persistence.v1.WorkflowMutableState.ChasmNodesEntryR\n" +
	"chasmNodes\x120\n" +
	"\x14signal_requested_ids\x18\x06 \x03(\tR\x12signalRequestedIds\x12`\n" +
	"\x0eexecution_info\x18\a \x01(\v29.temporal.server.api.persistence.v1.WorkflowExecutionInfoR\rexecutionInfo\x12c\n" +
	"\x0fexecution_state\x18\b \x01(\v2:.temporal.server.api.persistence.v1.WorkflowExecutionStateR\x0eexecutionState\x12\"\n" +
	"\rnext_event_id\x18\t \x01(\x03R\vnextEventId\x12N\n" +
	"\x0fbuffered_events\x18\n" +
	" \x03(\v2%.temporal.api.history.v1.HistoryEventR\x0ebufferedEvents\x12H\n" +
	"\bchecksum\x18\v \x01(\v2,.temporal.server.api.persistence.v1.ChecksumR\bchecksum\x1ar\n" +
	"\x12ActivityInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12F\n" +
	"\x05value\x18\x02 \x01(\v20.temporal.server.api.persistence.v1.ActivityInfoR\x05value:\x028\x01\x1al\n" +
	"\x0fTimerInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12C\n" +
	"\x05value\x18\x02 \x01(\v2-.temporal.server.api.persistence.v1.TimerInfoR\x05value:\x028\x01\x1a~\n" +
	"\x18ChildExecutionInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12L\n" +
	"\x05value\x18\x02 \x01(\v26.temporal.server.api.persistence.v1.ChildExecutionInfoR\x05value:\x028\x01\x1a|\n" +
	"\x17RequestCancelInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12K\n" +
	"\x05value\x18\x02 \x01(\v25.temporal.server.api.persistence.v1.RequestCancelInfoR\x05value:\x028\x01\x1an\n" +
	"\x10SignalInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12D\n" +
	"\x05value\x18\x02 \x01(\v2..temporal.server.api.persistence.v1.SignalInfoR\x05value:\x028\x01\x1al\n" +
	"\x0fChasmNodesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12C\n" +
	"\x05value\x18\x02 \x01(\v2-.temporal.server.api.persistence.v1.ChasmNodeR\x05value:\x028\x01\"\xcd\x16\n" +
	"\x1cWorkflowMutableStateMutation\x12\x90\x01\n" +
	"\x16updated_activity_infos\x18\x01 \x03(\v2Z.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedActivityInfosEntryR\x14updatedActivityInfos\x12\x87\x01\n" +
	"\x13updated_timer_infos\x18\x02 \x03(\v2W.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedTimerInfosEntryR\x11updatedTimerInfos\x12\xa3\x01\n" +
	"\x1dupdated_child_execution_infos\x18\x03 \x03(\v2`.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChildExecutionInfosEntryR\x1aupdatedChildExecutionInfos\x12\xa0\x01\n" +
	"\x1cupdated_request_cancel_infos\x18\x04 \x03(\v2_.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedRequestCancelInfosEntryR\x19updatedRequestCancelInfos\x12\x8a\x01\n" +
	"\x14updated_signal_infos\x18\x05 \x03(\v2X.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedSignalInfosEntryR\x12updatedSignalInfos\x12\x8a\x01\n" +
	"\x14updated_update_infos\x18\x06 \x03(\v2X.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedUpdateInfosEntryR\x12updatedUpdateInfos\x12\x96\x01\n" +
	"\x1aupdated_sub_state_machines\x18\a \x03(\v2Y.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutationR\x17updatedSubStateMachines\x12\x87\x01\n" +
	"\x13updated_chasm_nodes\x18\x13 \x03(\v2W.temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChasmNodesEntryR\x11updatedChasmNodes\x120\n" +
	"\x14signal_requested_ids\x18\x0f \x03(\tR\x12signalRequestedIds\x12`\n" +
	"\x0eexecution_info\x18\x10 \x01(\v29.temporal.server.api.persistence.v1.WorkflowExecutionInfoR\rexecutionInfo\x12c\n" +
	"\x0fexecution_state\x18\x11 \x01(\v2:.temporal.server.api.persistence.v1.WorkflowExecutionStateR\x0eexecutionState\x12\x8c\x01\n" +
	"#sub_state_machine_tombstone_batches\x18\x12 \x03(\v2>.temporal.server.api.persistence.v1.StateMachineTombstoneBatchR\x1fsubStateMachineTombstoneBatches\x1a\xf6\x02\n" +
	"\x18StateMachineNodeMutation\x12H\n" +
	"\x04path\x18\x01 \x01(\v24.temporal.server.api.persistence.v1.StateMachinePathR\x04path\x12\x12\n" +
	"\x04data\x18\x02 \x01(\fR\x04data\x12y\n" +
	"\x1cinitial_versioned_transition\x18\x03 \x01(\v27.temporal.server.api.persistence.v1.VersionedTransitionR\x1ainitialVersionedTransition\x12\x80\x01\n" +
	" last_update_versioned_transition\x18\x04 \x01(\v27.temporal.server.api.persistence.v1.VersionedTransitionR\x1dlastUpdateVersionedTransition\x1ay\n" +
	"\x19UpdatedActivityInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12F\n" +
	"\x05value\x18\x02 \x01(\v20.temporal.server.api.persistence.v1.ActivityInfoR\x05value:\x028\x01\x1as\n" +
	"\x16UpdatedTimerInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12C\n" +
	"\x05value\x18\x02 \x01(\v2-.temporal.server.api.persistence.v1.TimerInfoR\x05value:\x028\x01\x1a\x85\x01\n" +
	"\x1fUpdatedChildExecutionInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12L\n" +
	"\x05value\x18\x02 \x01(\v26.temporal.server.api.persistence.v1.ChildExecutionInfoR\x05value:\x028\x01\x1a\x83\x01\n" +
	"\x1eUpdatedRequestCancelInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12K\n" +
	"\x05value\x18\x02 \x01(\v25.temporal.server.api.persistence.v1.RequestCancelInfoR\x05value:\x028\x01\x1au\n" +
	"\x17UpdatedSignalInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12D\n" +
	"\x05value\x18\x02 \x01(\v2..temporal.server.api.persistence.v1.SignalInfoR\x05value:\x028\x01\x1au\n" +
	"\x17UpdatedUpdateInfosEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12D\n" +
	"\x05value\x18\x02 \x01(\v2..temporal.server.api.persistence.v1.UpdateInfoR\x05value:\x028\x01\x1as\n" +
	"\x16UpdatedChasmNodesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12C\n" +
	"\x05value\x18\x02 \x01(\v2-.temporal.server.api.persistence.v1.ChasmNodeR\x05value:\x028\x01J\x04\b\b\x10\tJ\x04\b\t\x10\n" +
	"J\x04\b\n" +
	"\x10\vJ\x04\b\v\x10\fJ\x04\b\f\x10\rJ\x04\b\r\x10\x0eJ\x04\b\x0e\x10\x0fB6Z4go.temporal.io/server/api/persistence/v1;persistenceb\x06proto3"

var (
	file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescData []byte
)

func file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDesc), len(file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDesc)))
	})
	return file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes = make([]protoimpl.MessageInfo, 16)
var file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_goTypes = []any{
	(*WorkflowMutableState)(nil),         // 0: temporal.server.api.persistence.v1.WorkflowMutableState
	(*WorkflowMutableStateMutation)(nil), // 1: temporal.server.api.persistence.v1.WorkflowMutableStateMutation
	nil,                                  // 2: temporal.server.api.persistence.v1.WorkflowMutableState.ActivityInfosEntry
	nil,                                  // 3: temporal.server.api.persistence.v1.WorkflowMutableState.TimerInfosEntry
	nil,                                  // 4: temporal.server.api.persistence.v1.WorkflowMutableState.ChildExecutionInfosEntry
	nil,                                  // 5: temporal.server.api.persistence.v1.WorkflowMutableState.RequestCancelInfosEntry
	nil,                                  // 6: temporal.server.api.persistence.v1.WorkflowMutableState.SignalInfosEntry
	nil,                                  // 7: temporal.server.api.persistence.v1.WorkflowMutableState.ChasmNodesEntry
	(*WorkflowMutableStateMutation_StateMachineNodeMutation)(nil), // 8: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutation
	nil,                                // 9: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedActivityInfosEntry
	nil,                                // 10: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedTimerInfosEntry
	nil,                                // 11: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChildExecutionInfosEntry
	nil,                                // 12: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedRequestCancelInfosEntry
	nil,                                // 13: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedSignalInfosEntry
	nil,                                // 14: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedUpdateInfosEntry
	nil,                                // 15: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChasmNodesEntry
	(*WorkflowExecutionInfo)(nil),      // 16: temporal.server.api.persistence.v1.WorkflowExecutionInfo
	(*WorkflowExecutionState)(nil),     // 17: temporal.server.api.persistence.v1.WorkflowExecutionState
	(*v1.HistoryEvent)(nil),            // 18: temporal.api.history.v1.HistoryEvent
	(*Checksum)(nil),                   // 19: temporal.server.api.persistence.v1.Checksum
	(*StateMachineTombstoneBatch)(nil), // 20: temporal.server.api.persistence.v1.StateMachineTombstoneBatch
	(*ActivityInfo)(nil),               // 21: temporal.server.api.persistence.v1.ActivityInfo
	(*TimerInfo)(nil),                  // 22: temporal.server.api.persistence.v1.TimerInfo
	(*ChildExecutionInfo)(nil),         // 23: temporal.server.api.persistence.v1.ChildExecutionInfo
	(*RequestCancelInfo)(nil),          // 24: temporal.server.api.persistence.v1.RequestCancelInfo
	(*SignalInfo)(nil),                 // 25: temporal.server.api.persistence.v1.SignalInfo
	(*ChasmNode)(nil),                  // 26: temporal.server.api.persistence.v1.ChasmNode
	(*StateMachinePath)(nil),           // 27: temporal.server.api.persistence.v1.StateMachinePath
	(*VersionedTransition)(nil),        // 28: temporal.server.api.persistence.v1.VersionedTransition
	(*UpdateInfo)(nil),                 // 29: temporal.server.api.persistence.v1.UpdateInfo
}
var file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_depIdxs = []int32{
	2,  // 0: temporal.server.api.persistence.v1.WorkflowMutableState.activity_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.ActivityInfosEntry
	3,  // 1: temporal.server.api.persistence.v1.WorkflowMutableState.timer_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.TimerInfosEntry
	4,  // 2: temporal.server.api.persistence.v1.WorkflowMutableState.child_execution_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.ChildExecutionInfosEntry
	5,  // 3: temporal.server.api.persistence.v1.WorkflowMutableState.request_cancel_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.RequestCancelInfosEntry
	6,  // 4: temporal.server.api.persistence.v1.WorkflowMutableState.signal_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.SignalInfosEntry
	7,  // 5: temporal.server.api.persistence.v1.WorkflowMutableState.chasm_nodes:type_name -> temporal.server.api.persistence.v1.WorkflowMutableState.ChasmNodesEntry
	16, // 6: temporal.server.api.persistence.v1.WorkflowMutableState.execution_info:type_name -> temporal.server.api.persistence.v1.WorkflowExecutionInfo
	17, // 7: temporal.server.api.persistence.v1.WorkflowMutableState.execution_state:type_name -> temporal.server.api.persistence.v1.WorkflowExecutionState
	18, // 8: temporal.server.api.persistence.v1.WorkflowMutableState.buffered_events:type_name -> temporal.api.history.v1.HistoryEvent
	19, // 9: temporal.server.api.persistence.v1.WorkflowMutableState.checksum:type_name -> temporal.server.api.persistence.v1.Checksum
	9,  // 10: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_activity_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedActivityInfosEntry
	10, // 11: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_timer_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedTimerInfosEntry
	11, // 12: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_child_execution_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChildExecutionInfosEntry
	12, // 13: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_request_cancel_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedRequestCancelInfosEntry
	13, // 14: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_signal_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedSignalInfosEntry
	14, // 15: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_update_infos:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedUpdateInfosEntry
	8,  // 16: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_sub_state_machines:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutation
	15, // 17: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.updated_chasm_nodes:type_name -> temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChasmNodesEntry
	16, // 18: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.execution_info:type_name -> temporal.server.api.persistence.v1.WorkflowExecutionInfo
	17, // 19: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.execution_state:type_name -> temporal.server.api.persistence.v1.WorkflowExecutionState
	20, // 20: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.sub_state_machine_tombstone_batches:type_name -> temporal.server.api.persistence.v1.StateMachineTombstoneBatch
	21, // 21: temporal.server.api.persistence.v1.WorkflowMutableState.ActivityInfosEntry.value:type_name -> temporal.server.api.persistence.v1.ActivityInfo
	22, // 22: temporal.server.api.persistence.v1.WorkflowMutableState.TimerInfosEntry.value:type_name -> temporal.server.api.persistence.v1.TimerInfo
	23, // 23: temporal.server.api.persistence.v1.WorkflowMutableState.ChildExecutionInfosEntry.value:type_name -> temporal.server.api.persistence.v1.ChildExecutionInfo
	24, // 24: temporal.server.api.persistence.v1.WorkflowMutableState.RequestCancelInfosEntry.value:type_name -> temporal.server.api.persistence.v1.RequestCancelInfo
	25, // 25: temporal.server.api.persistence.v1.WorkflowMutableState.SignalInfosEntry.value:type_name -> temporal.server.api.persistence.v1.SignalInfo
	26, // 26: temporal.server.api.persistence.v1.WorkflowMutableState.ChasmNodesEntry.value:type_name -> temporal.server.api.persistence.v1.ChasmNode
	27, // 27: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutation.path:type_name -> temporal.server.api.persistence.v1.StateMachinePath
	28, // 28: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutation.initial_versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	28, // 29: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.StateMachineNodeMutation.last_update_versioned_transition:type_name -> temporal.server.api.persistence.v1.VersionedTransition
	21, // 30: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedActivityInfosEntry.value:type_name -> temporal.server.api.persistence.v1.ActivityInfo
	22, // 31: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedTimerInfosEntry.value:type_name -> temporal.server.api.persistence.v1.TimerInfo
	23, // 32: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChildExecutionInfosEntry.value:type_name -> temporal.server.api.persistence.v1.ChildExecutionInfo
	24, // 33: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedRequestCancelInfosEntry.value:type_name -> temporal.server.api.persistence.v1.RequestCancelInfo
	25, // 34: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedSignalInfosEntry.value:type_name -> temporal.server.api.persistence.v1.SignalInfo
	29, // 35: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedUpdateInfosEntry.value:type_name -> temporal.server.api.persistence.v1.UpdateInfo
	26, // 36: temporal.server.api.persistence.v1.WorkflowMutableStateMutation.UpdatedChasmNodesEntry.value:type_name -> temporal.server.api.persistence.v1.ChasmNode
	37, // [37:37] is the sub-list for method output_type
	37, // [37:37] is the sub-list for method input_type
	37, // [37:37] is the sub-list for extension type_name
	37, // [37:37] is the sub-list for extension extendee
	0,  // [0:37] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_init() }
func file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_init() {
	if File_temporal_server_api_persistence_v1_workflow_mutable_state_proto != nil {
		return
	}
	file_temporal_server_api_persistence_v1_executions_proto_init()
	file_temporal_server_api_persistence_v1_hsm_proto_init()
	file_temporal_server_api_persistence_v1_chasm_proto_init()
	file_temporal_server_api_persistence_v1_update_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDesc), len(file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   16,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_workflow_mutable_state_proto = out.File
	file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_workflow_mutable_state_proto_depIdxs = nil
}
