// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/dynamicconfig"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	// executionManagerImpl implements ExecutionManager based on ExecutionStore, statsComputer and Serializer
	executionManagerImpl struct {
		serializer            serialization.Serializer
		persistence           ExecutionStore
		logger                log.Logger
		statsComputer         statsComputer
		pagingTokenSerializer *jsonHistoryTokenSerializer
		transactionSizeLimit  dynamicconfig.IntPropertyFn
	}
)

var _ ExecutionManager = (*executionManagerImpl)(nil)

// NewExecutionManager returns new ExecutionManager
func NewExecutionManager(
	persistence ExecutionStore,
	logger log.Logger,
	transactionSizeLimit dynamicconfig.IntPropertyFn,
) ExecutionManager {

	return &executionManagerImpl{
		serializer:            serialization.NewSerializer(),
		persistence:           persistence,
		logger:                logger,
		statsComputer:         statsComputer{},
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
		transactionSizeLimit:  transactionSizeLimit,
	}
}

func (m *executionManagerImpl) GetName() string {
	return m.persistence.GetName()
}

// The below three APIs are related to serialization/deserialization
func (m *executionManagerImpl) GetWorkflowExecution(
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {
	response, err := m.persistence.GetWorkflowExecution(request)
	if err != nil {
		return nil, err
	}
	state, err := m.toWorkflowMutableState(response.State)
	if err != nil {
		return nil, err
	}

	newResponse := &GetWorkflowExecutionResponse{
		State:             state,
		DBRecordVersion:   response.DBRecordVersion,
		MutableStateStats: m.statsComputer.computeMutableStateStats(state),
	}
	return newResponse, nil
}

func (m *executionManagerImpl) DeserializeBufferedEvents(
	blobs []*commonpb.DataBlob,
) ([]*historypb.HistoryEvent, error) {

	events := make([]*historypb.HistoryEvent, 0)
	for _, b := range blobs {
		if b == nil {
			// Should not happen, log and discard to prevent callers from consuming
			m.logger.Warn("discarding nil buffered event")
			continue
		}

		history, err := m.serializer.DeserializeEvents(b)
		if err != nil {
			return nil, err
		}
		events = append(events, history...)
	}
	return events, nil
}

func (m *executionManagerImpl) UpdateWorkflowExecution(
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {

	mutation := request.UpdateWorkflowMutation
	newSnapshot := request.NewWorkflowSnapshot
	if err := ValidateUpdateWorkflowModeState(
		request.Mode,
		mutation,
		newSnapshot,
	); err != nil {
		return nil, err
	}
	if err := ValidateUpdateWorkflowStateStatus(
		mutation.ExecutionState.State,
		mutation.ExecutionState.Status,
	); err != nil {
		return nil, err
	}

	serializedWorkflowMutation, err := m.SerializeWorkflowMutation(&mutation)
	if err != nil {
		return nil, err
	}
	var serializedNewWorkflowSnapshot *InternalWorkflowSnapshot
	if request.NewWorkflowSnapshot != nil {
		serializedNewWorkflowSnapshot, err = m.SerializeWorkflowSnapshot(newSnapshot)
		if err != nil {
			return nil, err
		}
	}

	newRequest := &InternalUpdateWorkflowExecutionRequest{
		ShardID: request.ShardID,
		RangeID: request.RangeID,

		Mode: request.Mode,

		UpdateWorkflowMutation: *serializedWorkflowMutation,
		NewWorkflowSnapshot:    serializedNewWorkflowSnapshot,
	}

	if err := m.persistence.UpdateWorkflowExecution(newRequest); err != nil {
		return nil, err
	}
	msuss := m.statsComputer.computeMutableStateUpdateStats(request)
	return &UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: msuss}, nil
}

func (m *executionManagerImpl) ConflictResolveWorkflowExecution(
	request *ConflictResolveWorkflowExecutionRequest,
) error {

	if err := ValidateConflictResolveWorkflowModeState(
		request.Mode,
		request.ResetWorkflowSnapshot,
		request.NewWorkflowSnapshot,
		request.CurrentWorkflowMutation,
	); err != nil {
		return err
	}

	serializedResetWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&request.ResetWorkflowSnapshot)
	if err != nil {
		return err
	}
	var serializedCurrentWorkflowMutation *InternalWorkflowMutation
	if request.CurrentWorkflowMutation != nil {
		serializedCurrentWorkflowMutation, err = m.SerializeWorkflowMutation(request.CurrentWorkflowMutation)
		if err != nil {
			return err
		}
	}
	var serializedNewWorkflowMutation *InternalWorkflowSnapshot
	if request.NewWorkflowSnapshot != nil {
		serializedNewWorkflowMutation, err = m.SerializeWorkflowSnapshot(request.NewWorkflowSnapshot)
		if err != nil {
			return err
		}
	}

	newRequest := &InternalConflictResolveWorkflowExecutionRequest{
		ShardID: request.ShardID,
		RangeID: request.RangeID,

		Mode: request.Mode,

		ResetWorkflowSnapshot: *serializedResetWorkflowSnapshot,

		NewWorkflowSnapshot: serializedNewWorkflowMutation,

		CurrentWorkflowMutation: serializedCurrentWorkflowMutation,
	}
	return m.persistence.ConflictResolveWorkflowExecution(newRequest)
}

func (m *executionManagerImpl) CreateWorkflowExecution(
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {
	snapshot := request.NewWorkflowSnapshot
	if err := ValidateCreateWorkflowModeState(
		request.Mode,
		snapshot,
	); err != nil {
		return nil, err
	}
	if err := ValidateCreateWorkflowStateStatus(
		snapshot.ExecutionState.State,
		snapshot.ExecutionState.Status,
	); err != nil {
		return nil, err
	}

	snapshot.ExecutionInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	serializedNewWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&snapshot)
	if err != nil {
		return nil, err
	}

	newRequest := &InternalCreateWorkflowExecutionRequest{
		ShardID:                  request.ShardID,
		RangeID:                  request.RangeID,
		Mode:                     request.Mode,
		PreviousRunID:            request.PreviousRunID,
		PreviousLastWriteVersion: request.PreviousLastWriteVersion,
		NewWorkflowSnapshot:      *serializedNewWorkflowSnapshot,
	}

	return m.persistence.CreateWorkflowExecution(newRequest)
}

func (m *executionManagerImpl) SerializeWorkflowMutation(
	input *WorkflowMutation,
) (*InternalWorkflowMutation, error) {

	var err error
	result := &InternalWorkflowMutation{
		NamespaceID: input.ExecutionInfo.GetNamespaceId(),
		WorkflowID:  input.ExecutionInfo.GetWorkflowId(),
		RunID:       input.ExecutionState.GetRunId(),

		UpsertActivityInfos:       make(map[int64]*commonpb.DataBlob),
		UpsertTimerInfos:          make(map[string]*commonpb.DataBlob),
		UpsertChildExecutionInfos: make(map[int64]*commonpb.DataBlob),
		UpsertRequestCancelInfos:  make(map[int64]*commonpb.DataBlob),
		UpsertSignalInfos:         make(map[int64]*commonpb.DataBlob),

		ExecutionState: input.ExecutionState,

		DeleteActivityInfos:       input.DeleteActivityInfos,
		DeleteTimerInfos:          input.DeleteTimerInfos,
		DeleteChildExecutionInfos: input.DeleteChildExecutionInfos,
		DeleteRequestCancelInfos:  input.DeleteRequestCancelInfos,
		DeleteSignalInfos:         input.DeleteSignalInfos,
		DeleteSignalRequestedIDs:  input.DeleteSignalRequestedIDs,

		UpsertSignalRequestedIDs: input.UpsertSignalRequestedIDs,
		ClearBufferedEvents:      input.ClearBufferedEvents,

		TransferTasks:    input.TransferTasks,
		ReplicationTasks: input.ReplicationTasks,
		TimerTasks:       input.TimerTasks,
		VisibilityTasks:  input.VisibilityTasks,

		Condition:       input.Condition,
		DBRecordVersion: input.DBRecordVersion,
		NextEventID:     input.NextEventID,
		StartVersion:    input.ExecutionInfo.StartVersion,
	}

	result.ExecutionInfo, err = m.serializer.WorkflowExecutionInfoToBlob(input.ExecutionInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	result.ExecutionStateBlob, err = m.serializer.WorkflowExecutionStateToBlob(input.ExecutionState, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	for key, info := range input.UpsertActivityInfos {
		blob, err := m.serializer.ActivityInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.UpsertActivityInfos[key] = blob
	}
	for key, info := range input.UpsertTimerInfos {
		blob, err := m.serializer.TimerInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.UpsertTimerInfos[key] = blob
	}
	for key, info := range input.UpsertChildExecutionInfos {
		blob, err := m.serializer.ChildExecutionInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.UpsertChildExecutionInfos[key] = blob
	}
	for key, info := range input.UpsertRequestCancelInfos {
		blob, err := m.serializer.RequestCancelInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.UpsertRequestCancelInfos[key] = blob
	}
	for key, info := range input.UpsertSignalInfos {
		blob, err := m.serializer.SignalInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.UpsertSignalInfos[key] = blob
	}

	if len(input.NewBufferedEvents) > 0 {
		result.NewBufferedEvents, err = m.serializer.SerializeEvents(input.NewBufferedEvents, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
	}
	result.LastWriteVersion, err = getLastWriteVersion(input.ExecutionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	result.Checksum, err = m.serializer.ChecksumToBlob(input.Checksum, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (m *executionManagerImpl) SerializeWorkflowSnapshot(
	input *WorkflowSnapshot,
) (*InternalWorkflowSnapshot, error) {

	var err error
	result := &InternalWorkflowSnapshot{
		NamespaceID: input.ExecutionInfo.GetNamespaceId(),
		WorkflowID:  input.ExecutionInfo.GetWorkflowId(),
		RunID:       input.ExecutionState.GetRunId(),

		ActivityInfos:       make(map[int64]*commonpb.DataBlob),
		TimerInfos:          make(map[string]*commonpb.DataBlob),
		ChildExecutionInfos: make(map[int64]*commonpb.DataBlob),
		RequestCancelInfos:  make(map[int64]*commonpb.DataBlob),
		SignalInfos:         make(map[int64]*commonpb.DataBlob),

		ExecutionState:     input.ExecutionState,
		SignalRequestedIDs: input.SignalRequestedIDs,
		TransferTasks:      input.TransferTasks,
		ReplicationTasks:   input.ReplicationTasks,
		TimerTasks:         input.TimerTasks,
		VisibilityTasks:    input.VisibilityTasks,

		Condition:       input.Condition,
		DBRecordVersion: input.DBRecordVersion,
		NextEventID:     input.NextEventID,

		StartVersion: input.ExecutionInfo.StartVersion,
	}

	result.ExecutionInfo, err = m.serializer.WorkflowExecutionInfoToBlob(input.ExecutionInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	result.ExecutionStateBlob, err = m.serializer.WorkflowExecutionStateToBlob(input.ExecutionState, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	result.LastWriteVersion, err = getLastWriteVersion(input.ExecutionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}

	for key, info := range input.ActivityInfos {
		blob, err := m.serializer.ActivityInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.ActivityInfos[key] = blob
	}
	for key, info := range input.TimerInfos {
		blob, err := m.serializer.TimerInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.TimerInfos[key] = blob
	}
	for key, info := range input.ChildExecutionInfos {
		blob, err := m.serializer.ChildExecutionInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.ChildExecutionInfos[key] = blob
	}
	for key, info := range input.RequestCancelInfos {
		blob, err := m.serializer.RequestCancelInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.RequestCancelInfos[key] = blob
	}
	for key, info := range input.SignalInfos {
		blob, err := m.serializer.SignalInfoToBlob(info, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		result.SignalInfos[key] = blob
	}
	result.Checksum, err = m.serializer.ChecksumToBlob(input.Checksum, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (m *executionManagerImpl) DeleteWorkflowExecution(
	request *DeleteWorkflowExecutionRequest,
) error {
	return m.persistence.DeleteWorkflowExecution(request)
}

func (m *executionManagerImpl) DeleteCurrentWorkflowExecution(
	request *DeleteCurrentWorkflowExecutionRequest,
) error {
	return m.persistence.DeleteCurrentWorkflowExecution(request)
}

func (m *executionManagerImpl) GetCurrentExecution(
	request *GetCurrentExecutionRequest,
) (*GetCurrentExecutionResponse, error) {
	internalResp, err := m.persistence.GetCurrentExecution(request)
	if err != nil {
		return nil, err
	}

	return &GetCurrentExecutionResponse{
		RunID:            internalResp.RunID,
		LastWriteVersion: internalResp.LastWriteVersion,
		StartRequestID:   internalResp.ExecutionState.CreateRequestId,
		State:            internalResp.ExecutionState.State,
		Status:           internalResp.ExecutionState.Status,
	}, nil
}

func (m *executionManagerImpl) ListConcreteExecutions(
	request *ListConcreteExecutionsRequest,
) (*ListConcreteExecutionsResponse, error) {
	response, err := m.persistence.ListConcreteExecutions(request)
	if err != nil {
		return nil, err
	}
	newResponse := &ListConcreteExecutionsResponse{
		States:    make([]*persistencespb.WorkflowMutableState, len(response.States)),
		PageToken: response.NextPageToken,
	}
	for i, s := range response.States {
		state, err := m.toWorkflowMutableState(s)
		if err != nil {
			return nil, err
		}
		newResponse.States[i] = state
	}
	return newResponse, nil
}

func (m *executionManagerImpl) AddTasks(
	request *AddTasksRequest,
) error {
	return m.persistence.AddTasks(request)
}

// Transfer task related methods
func (m *executionManagerImpl) GetTransferTask(
	request *GetTransferTaskRequest,
) (*GetTransferTaskResponse, error) {
	return m.persistence.GetTransferTask(request)
}

func (m *executionManagerImpl) GetTransferTasks(
	request *GetTransferTasksRequest,
) (*GetTransferTasksResponse, error) {
	return m.persistence.GetTransferTasks(request)
}

func (m *executionManagerImpl) CompleteTransferTask(
	request *CompleteTransferTaskRequest,
) error {
	return m.persistence.CompleteTransferTask(request)
}

func (m *executionManagerImpl) RangeCompleteTransferTask(
	request *RangeCompleteTransferTaskRequest,
) error {
	return m.persistence.RangeCompleteTransferTask(request)
}

// Visibility task related methods
func (m *executionManagerImpl) GetVisibilityTask(
	request *GetVisibilityTaskRequest,
) (*GetVisibilityTaskResponse, error) {
	return m.persistence.GetVisibilityTask(request)
}

func (m *executionManagerImpl) GetVisibilityTasks(
	request *GetVisibilityTasksRequest,
) (*GetVisibilityTasksResponse, error) {
	return m.persistence.GetVisibilityTasks(request)
}

func (m *executionManagerImpl) CompleteVisibilityTask(
	request *CompleteVisibilityTaskRequest,
) error {
	return m.persistence.CompleteVisibilityTask(request)
}

func (m *executionManagerImpl) RangeCompleteVisibilityTask(
	request *RangeCompleteVisibilityTaskRequest,
) error {
	return m.persistence.RangeCompleteVisibilityTask(request)
}

// Replication task related methods
func (m *executionManagerImpl) GetReplicationTask(
	request *GetReplicationTaskRequest,
) (*GetReplicationTaskResponse, error) {
	return m.persistence.GetReplicationTask(request)
}

func (m *executionManagerImpl) GetReplicationTasks(
	request *GetReplicationTasksRequest,
) (*GetReplicationTasksResponse, error) {
	return m.persistence.GetReplicationTasks(request)
}

func (m *executionManagerImpl) CompleteReplicationTask(
	request *CompleteReplicationTaskRequest,
) error {
	return m.persistence.CompleteReplicationTask(request)
}

func (m *executionManagerImpl) RangeCompleteReplicationTask(
	request *RangeCompleteReplicationTaskRequest,
) error {
	return m.persistence.RangeCompleteReplicationTask(request)
}

func (m *executionManagerImpl) PutReplicationTaskToDLQ(
	request *PutReplicationTaskToDLQRequest,
) error {
	return m.persistence.PutReplicationTaskToDLQ(request)
}

func (m *executionManagerImpl) GetReplicationTasksFromDLQ(
	request *GetReplicationTasksFromDLQRequest,
) (*GetReplicationTasksFromDLQResponse, error) {
	return m.persistence.GetReplicationTasksFromDLQ(request)
}

func (m *executionManagerImpl) DeleteReplicationTaskFromDLQ(
	request *DeleteReplicationTaskFromDLQRequest,
) error {
	return m.persistence.DeleteReplicationTaskFromDLQ(request)
}

func (m *executionManagerImpl) RangeDeleteReplicationTaskFromDLQ(
	request *RangeDeleteReplicationTaskFromDLQRequest,
) error {
	return m.persistence.RangeDeleteReplicationTaskFromDLQ(request)
}

// Timer related methods.
func (m *executionManagerImpl) GetTimerTask(
	request *GetTimerTaskRequest,
) (*GetTimerTaskResponse, error) {
	return m.persistence.GetTimerTask(request)
}

func (m *executionManagerImpl) GetTimerIndexTasks(
	request *GetTimerIndexTasksRequest,
) (*GetTimerIndexTasksResponse, error) {
	return m.persistence.GetTimerIndexTasks(request)
}

func (m *executionManagerImpl) CompleteTimerTask(
	request *CompleteTimerTaskRequest,
) error {
	return m.persistence.CompleteTimerTask(request)
}

func (m *executionManagerImpl) RangeCompleteTimerTask(
	request *RangeCompleteTimerTaskRequest,
) error {
	return m.persistence.RangeCompleteTimerTask(request)
}

func (m *executionManagerImpl) Close() {
	m.persistence.Close()
}

func (m *executionManagerImpl) toWorkflowMutableState(internState *InternalWorkflowMutableState) (*persistencespb.WorkflowMutableState, error) {
	state := &persistencespb.WorkflowMutableState{
		ActivityInfos:       make(map[int64]*persistencespb.ActivityInfo),
		TimerInfos:          make(map[string]*persistencespb.TimerInfo),
		ChildExecutionInfos: make(map[int64]*persistencespb.ChildExecutionInfo),
		RequestCancelInfos:  make(map[int64]*persistencespb.RequestCancelInfo),
		SignalInfos:         make(map[int64]*persistencespb.SignalInfo),
		SignalRequestedIds:  internState.SignalRequestedIDs,
		NextEventId:         internState.NextEventID,
		BufferedEvents:      make([]*historypb.HistoryEvent, len(internState.BufferedEvents)),
	}
	for key, blob := range internState.ActivityInfos {
		info, err := m.serializer.ActivityInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}
		state.ActivityInfos[key] = info
	}
	for key, blob := range internState.TimerInfos {
		info, err := m.serializer.TimerInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}
		state.TimerInfos[key] = info
	}
	for key, blob := range internState.ChildExecutionInfos {
		info, err := m.serializer.ChildExecutionInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}
		state.ChildExecutionInfos[key] = info
	}
	for key, blob := range internState.RequestCancelInfos {
		info, err := m.serializer.RequestCancelInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}
		state.RequestCancelInfos[key] = info
	}
	for key, blob := range internState.SignalInfos {
		info, err := m.serializer.SignalInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}
		state.SignalInfos[key] = info
	}
	var err error
	state.ExecutionInfo, err = m.serializer.WorkflowExecutionInfoFromBlob(internState.ExecutionInfo)
	if err != nil {
		return nil, err
	}
	if state.ExecutionInfo.AutoResetPoints == nil {
		// TODO: check if we need this?
		state.ExecutionInfo.AutoResetPoints = &workflowpb.ResetPoints{}
	}
	state.ExecutionState, err = m.serializer.WorkflowExecutionStateFromBlob(internState.ExecutionState)
	if err != nil {
		return nil, err
	}
	state.BufferedEvents, err = m.DeserializeBufferedEvents(internState.BufferedEvents)
	if err != nil {
		return nil, err
	}
	if internState.Checksum != nil {
		state.Checksum, err = m.serializer.ChecksumFromBlob(internState.Checksum)
	}
	if err != nil {
		return nil, err
	}

	return state, nil
}

func getLastWriteVersion(
	versionHistories *historyspb.VersionHistories,
) (int64, error) {

	if versionHistories == nil {
		return common.EmptyVersion, nil
	}

	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return 0, err
	}
	versionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(versionHistory)
	if err != nil {
		return 0, err
	}
	return versionHistoryItem.GetVersion(), nil
}
