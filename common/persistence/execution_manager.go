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
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// executionManagerImpl implements ExecutionManager based on ExecutionStore, statsComputer and Serializer
	executionManagerImpl struct {
		serializer            serialization.Serializer
		persistence           ExecutionStore
		logger                log.Logger
		pagingTokenSerializer *jsonHistoryTokenSerializer
		transactionSizeLimit  dynamicconfig.IntPropertyFn
	}
)

var _ ExecutionManager = (*executionManagerImpl)(nil)

// NewExecutionManager returns new ExecutionManager
func NewExecutionManager(
	persistence ExecutionStore,
	serializer serialization.Serializer,
	logger log.Logger,
	transactionSizeLimit dynamicconfig.IntPropertyFn,
) ExecutionManager {

	return &executionManagerImpl{
		serializer:            serializer,
		persistence:           persistence,
		logger:                logger,
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
		transactionSizeLimit:  transactionSizeLimit,
	}
}

func (m *executionManagerImpl) GetName() string {
	return m.persistence.GetName()
}

// The below three APIs are related to serialization/deserialization

func (m *executionManagerImpl) CreateWorkflowExecution(
	request *CreateWorkflowExecutionRequest,
) (*CreateWorkflowExecutionResponse, error) {

	newSnapshot := request.NewWorkflowSnapshot
	newWorkflowNewEvents, newHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.NewWorkflowEvents)
	if err != nil {
		return nil, err
	}
	newSnapshot.ExecutionInfo.ExecutionStats.HistorySize += int64(newHistoryDiff.SizeDiff)

	if err := ValidateCreateWorkflowModeState(
		request.Mode,
		newSnapshot,
	); err != nil {
		return nil, err
	}
	if err := ValidateCreateWorkflowStateStatus(
		newSnapshot.ExecutionState.State,
		newSnapshot.ExecutionState.Status,
	); err != nil {
		return nil, err
	}

	serializedNewWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&newSnapshot)
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
		NewWorkflowNewEvents:     newWorkflowNewEvents,
	}

	if _, err := m.persistence.CreateWorkflowExecution(newRequest); err != nil {
		return nil, err
	}
	return &CreateWorkflowExecutionResponse{
		NewMutableStateStats: *statusOfInternalWorkflowSnapshot(
			serializedNewWorkflowSnapshot,
			newHistoryDiff,
		),
	}, nil
}

func (m *executionManagerImpl) UpdateWorkflowExecution(
	request *UpdateWorkflowExecutionRequest,
) (*UpdateWorkflowExecutionResponse, error) {

	updateMutation := request.UpdateWorkflowMutation
	newSnapshot := request.NewWorkflowSnapshot

	updateWorkflowNewEvents, updateWorkflowHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.UpdateWorkflowEvents)
	if err != nil {
		return nil, err
	}
	updateMutation.ExecutionInfo.ExecutionStats.HistorySize += int64(updateWorkflowHistoryDiff.SizeDiff)

	newWorkflowNewEvents, newWorkflowHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.NewWorkflowEvents)
	if err != nil {
		return nil, err
	}
	if newSnapshot != nil {
		newSnapshot.ExecutionInfo.ExecutionStats.HistorySize += int64(newWorkflowHistoryDiff.SizeDiff)
	}

	if err := ValidateUpdateWorkflowModeState(
		request.Mode,
		updateMutation,
		newSnapshot,
	); err != nil {
		return nil, err
	}
	if err := ValidateUpdateWorkflowStateStatus(
		updateMutation.ExecutionState.State,
		updateMutation.ExecutionState.Status,
	); err != nil {
		return nil, err
	}

	serializedWorkflowMutation, err := m.SerializeWorkflowMutation(&updateMutation)
	if err != nil {
		return nil, err
	}
	var serializedNewWorkflowSnapshot *InternalWorkflowSnapshot
	if newSnapshot != nil {
		serializedNewWorkflowSnapshot, err = m.SerializeWorkflowSnapshot(newSnapshot)
		if err != nil {
			return nil, err
		}
	}

	newRequest := &InternalUpdateWorkflowExecutionRequest{
		ShardID: request.ShardID,
		RangeID: request.RangeID,

		Mode: request.Mode,

		UpdateWorkflowMutation:  *serializedWorkflowMutation,
		UpdateWorkflowNewEvents: updateWorkflowNewEvents,
		NewWorkflowSnapshot:     serializedNewWorkflowSnapshot,
		NewWorkflowNewEvents:    newWorkflowNewEvents,
	}

	err = m.persistence.UpdateWorkflowExecution(newRequest)
	switch err.(type) {
	case nil:
		return &UpdateWorkflowExecutionResponse{
			UpdateMutableStateStats: *statusOfInternalWorkflowMutation(
				&newRequest.UpdateWorkflowMutation,
				updateWorkflowHistoryDiff,
			),
			NewMutableStateStats: statusOfInternalWorkflowSnapshot(
				newRequest.NewWorkflowSnapshot,
				newWorkflowHistoryDiff,
			),
		}, nil
	case *CurrentWorkflowConditionFailedError,
		*WorkflowConditionFailedError,
		*ConditionFailedError:
		m.trimHistoryNode(
			request.ShardID,
			updateMutation.ExecutionInfo.NamespaceId,
			updateMutation.ExecutionInfo.WorkflowId,
			updateMutation.ExecutionState.RunId,
		)
		return nil, err
	default:
		return nil, err
	}
}

func (m *executionManagerImpl) ConflictResolveWorkflowExecution(
	request *ConflictResolveWorkflowExecutionRequest,
) (*ConflictResolveWorkflowExecutionResponse, error) {

	resetSnapshot := request.ResetWorkflowSnapshot
	newSnapshot := request.NewWorkflowSnapshot
	currentMutation := request.CurrentWorkflowMutation

	resetWorkflowEventsNewEvents, resetWorkflowHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.ResetWorkflowEvents)
	if err != nil {
		return nil, err
	}
	resetSnapshot.ExecutionInfo.ExecutionStats.HistorySize += int64(resetWorkflowHistoryDiff.SizeDiff)

	newWorkflowEventsNewEvents, newWorkflowHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.NewWorkflowEvents)
	if err != nil {
		return nil, err
	}
	if newSnapshot != nil {
		newSnapshot.ExecutionInfo.ExecutionStats.HistorySize += int64(newWorkflowHistoryDiff.SizeDiff)
	}

	currentWorkflowEventsNewEvents, currentWorkflowHistoryDiff, err := m.serializeWorkflowEventBatches(request.ShardID, request.CurrentWorkflowEvents)
	if err != nil {
		return nil, err
	}
	if currentMutation != nil {
		currentMutation.ExecutionInfo.ExecutionStats.HistorySize += int64(currentWorkflowHistoryDiff.SizeDiff)
	}

	if err := ValidateConflictResolveWorkflowModeState(
		request.Mode,
		resetSnapshot,
		newSnapshot,
		currentMutation,
	); err != nil {
		return nil, err
	}

	serializedResetWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&resetSnapshot)
	if err != nil {
		return nil, err
	}
	var serializedCurrentWorkflowMutation *InternalWorkflowMutation
	if currentMutation != nil {
		serializedCurrentWorkflowMutation, err = m.SerializeWorkflowMutation(currentMutation)
		if err != nil {
			return nil, err
		}
	}
	var serializedNewWorkflowMutation *InternalWorkflowSnapshot
	if newSnapshot != nil {
		serializedNewWorkflowMutation, err = m.SerializeWorkflowSnapshot(newSnapshot)
		if err != nil {
			return nil, err
		}
	}

	newRequest := &InternalConflictResolveWorkflowExecutionRequest{
		ShardID: request.ShardID,
		RangeID: request.RangeID,

		Mode: request.Mode,

		ResetWorkflowSnapshot:        *serializedResetWorkflowSnapshot,
		ResetWorkflowEventsNewEvents: resetWorkflowEventsNewEvents,

		NewWorkflowSnapshot:        serializedNewWorkflowMutation,
		NewWorkflowEventsNewEvents: newWorkflowEventsNewEvents,

		CurrentWorkflowMutation:        serializedCurrentWorkflowMutation,
		CurrentWorkflowEventsNewEvents: currentWorkflowEventsNewEvents,
	}

	err = m.persistence.ConflictResolveWorkflowExecution(newRequest)
	switch err.(type) {
	case nil:
		return &ConflictResolveWorkflowExecutionResponse{
			ResetMutableStateStats: *statusOfInternalWorkflowSnapshot(
				&newRequest.ResetWorkflowSnapshot,
				resetWorkflowHistoryDiff,
			),
			NewMutableStateStats: statusOfInternalWorkflowSnapshot(
				newRequest.NewWorkflowSnapshot,
				newWorkflowHistoryDiff,
			),
			CurrentMutableStateStats: statusOfInternalWorkflowMutation(
				newRequest.CurrentWorkflowMutation,
				currentWorkflowHistoryDiff,
			),
		}, nil
	case *CurrentWorkflowConditionFailedError,
		*WorkflowConditionFailedError,
		*ConditionFailedError:
		m.trimHistoryNode(
			request.ShardID,
			resetSnapshot.ExecutionInfo.NamespaceId,
			resetSnapshot.ExecutionInfo.WorkflowId,
			resetSnapshot.ExecutionState.RunId,
		)
		if currentMutation != nil {
			m.trimHistoryNode(
				request.ShardID,
				currentMutation.ExecutionInfo.NamespaceId,
				currentMutation.ExecutionInfo.WorkflowId,
				currentMutation.ExecutionState.RunId,
			)
		}
		return nil, err
	default:
		return nil, err
	}
}

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
	if state.ExecutionInfo.ExecutionStats == nil {
		state.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
			HistorySize: 0,
		}
	}

	newResponse := &GetWorkflowExecutionResponse{
		State:             state,
		DBRecordVersion:   response.DBRecordVersion,
		MutableStateStats: *statusOfInternalWorkflow(response.State, nil),
	}
	return newResponse, nil
}

func (m *executionManagerImpl) SetWorkflowExecution(
	request *SetWorkflowExecutionRequest,
) (*SetWorkflowExecutionResponse, error) {
	serializedWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&request.SetWorkflowSnapshot)
	if err != nil {
		return nil, err
	}

	newRequest := &InternalSetWorkflowExecutionRequest{
		ShardID: request.ShardID,
		RangeID: request.RangeID,

		SetWorkflowSnapshot: *serializedWorkflowSnapshot,
	}

	err = m.persistence.SetWorkflowExecution(newRequest)
	if err != nil {
		return nil, err
	}
	return &SetWorkflowExecutionResponse{}, nil
}

func (m *executionManagerImpl) serializeWorkflowEventBatches(
	shardID int32,
	eventBatches []*WorkflowEvents,
) ([]*InternalAppendHistoryNodesRequest, *HistoryStatistics, error) {
	var historyStatistics HistoryStatistics
	if len(eventBatches) == 0 {
		return nil, &historyStatistics, nil
	}

	workflowNewEvents := make([]*InternalAppendHistoryNodesRequest, 0, len(eventBatches))
	for _, workflowEvents := range eventBatches {
		newEvents, err := m.serializeWorkflowEvents(shardID, workflowEvents)
		if err != nil {
			return nil, nil, err
		}
		newEvents.ShardID = shardID
		workflowNewEvents = append(workflowNewEvents, newEvents)
		historyStatistics.SizeDiff += len(newEvents.Node.Events.Data)
		historyStatistics.CountDiff += len(workflowEvents.Events)
	}
	return workflowNewEvents, &historyStatistics, nil
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

func (m *executionManagerImpl) serializeWorkflowEvents(
	shardID int32,
	workflowEvents *WorkflowEvents,
) (*InternalAppendHistoryNodesRequest, error) {
	if len(workflowEvents.Events) == 0 {
		return nil, nil // allow update workflow without events
	}

	request := &AppendHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       workflowEvents.BranchToken,
		Events:            workflowEvents.Events,
		PrevTransactionID: workflowEvents.PrevTxnID,
		TransactionID:     workflowEvents.TxnID,
	}

	if workflowEvents.Events[0].EventId == common.FirstEventID {
		request.IsNewBranch = true
		request.Info = BuildHistoryGarbageCleanupInfo(workflowEvents.NamespaceID, workflowEvents.WorkflowID, workflowEvents.RunID)
	}

	return m.serializeAppendHistoryNodesRequest(request)
}

func (m *executionManagerImpl) SerializeWorkflowMutation(
	input *WorkflowMutation,
) (*InternalWorkflowMutation, error) {

	tasks, err := serializeTasks(m.serializer, input.Tasks)
	if err != nil {
		return nil, err
	}

	result := &InternalWorkflowMutation{
		NamespaceID: input.ExecutionInfo.GetNamespaceId(),
		WorkflowID:  input.ExecutionInfo.GetWorkflowId(),
		RunID:       input.ExecutionState.GetRunId(),

		UpsertActivityInfos:       make(map[int64]*commonpb.DataBlob),
		UpsertTimerInfos:          make(map[string]*commonpb.DataBlob),
		UpsertChildExecutionInfos: make(map[int64]*commonpb.DataBlob),
		UpsertRequestCancelInfos:  make(map[int64]*commonpb.DataBlob),
		UpsertSignalInfos:         make(map[int64]*commonpb.DataBlob),

		ExecutionInfo:  input.ExecutionInfo,
		ExecutionState: input.ExecutionState,

		DeleteActivityInfos:       input.DeleteActivityInfos,
		DeleteTimerInfos:          input.DeleteTimerInfos,
		DeleteChildExecutionInfos: input.DeleteChildExecutionInfos,
		DeleteRequestCancelInfos:  input.DeleteRequestCancelInfos,
		DeleteSignalInfos:         input.DeleteSignalInfos,
		DeleteSignalRequestedIDs:  input.DeleteSignalRequestedIDs,

		UpsertSignalRequestedIDs: input.UpsertSignalRequestedIDs,
		ClearBufferedEvents:      input.ClearBufferedEvents,

		Tasks: tasks,

		Condition:       input.Condition,
		DBRecordVersion: input.DBRecordVersion,
		NextEventID:     input.NextEventID,
	}

	result.ExecutionInfoBlob, err = m.serializer.WorkflowExecutionInfoToBlob(input.ExecutionInfo, enumspb.ENCODING_TYPE_PROTO3)
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
	result.LastWriteVersion, err = getCurrentBranchLastWriteVersion(input.ExecutionInfo.VersionHistories)
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

	tasks, err := serializeTasks(m.serializer, input.Tasks)
	if err != nil {
		return nil, err
	}

	result := &InternalWorkflowSnapshot{
		NamespaceID: input.ExecutionInfo.GetNamespaceId(),
		WorkflowID:  input.ExecutionInfo.GetWorkflowId(),
		RunID:       input.ExecutionState.GetRunId(),

		ActivityInfos:       make(map[int64]*commonpb.DataBlob),
		TimerInfos:          make(map[string]*commonpb.DataBlob),
		ChildExecutionInfos: make(map[int64]*commonpb.DataBlob),
		RequestCancelInfos:  make(map[int64]*commonpb.DataBlob),
		SignalInfos:         make(map[int64]*commonpb.DataBlob),

		ExecutionInfo:      input.ExecutionInfo,
		ExecutionState:     input.ExecutionState,
		SignalRequestedIDs: make(map[string]struct{}),

		Tasks: tasks,

		Condition:       input.Condition,
		DBRecordVersion: input.DBRecordVersion,
		NextEventID:     input.NextEventID,
	}

	result.ExecutionInfoBlob, err = m.serializer.WorkflowExecutionInfoToBlob(input.ExecutionInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	result.ExecutionStateBlob, err = m.serializer.WorkflowExecutionStateToBlob(input.ExecutionState, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	result.LastWriteVersion, err = getCurrentBranchLastWriteVersion(input.ExecutionInfo.VersionHistories)
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
	for key := range input.SignalRequestedIDs {
		result.SignalRequestedIDs[key] = struct{}{}
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
		RunID:          internalResp.RunID,
		StartRequestID: internalResp.ExecutionState.CreateRequestId,
		State:          internalResp.ExecutionState.State,
		Status:         internalResp.ExecutionState.Status,
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

func (m *executionManagerImpl) AddHistoryTasks(
	input *AddHistoryTasksRequest,
) error {
	tasks, err := serializeTasks(m.serializer, input.Tasks)
	if err != nil {
		return err
	}

	return m.persistence.AddHistoryTasks(&InternalAddHistoryTasksRequest{
		ShardID: input.ShardID,
		RangeID: input.RangeID,

		NamespaceID: input.NamespaceID,
		WorkflowID:  input.WorkflowID,
		RunID:       input.RunID,

		Tasks: tasks,
	})
}

func (m *executionManagerImpl) GetHistoryTask(
	request *GetHistoryTaskRequest,
) (*GetHistoryTaskResponse, error) {
	resp, err := m.persistence.GetHistoryTask(request)
	if err != nil {
		return nil, err
	}

	task, err := m.serializer.DeserializeTask(request.TaskCategory, resp.Task)
	if err != nil {
		return nil, err
	}
	return &GetHistoryTaskResponse{
		Task: task,
	}, nil
}

func (m *executionManagerImpl) GetHistoryTasks(
	request *GetHistoryTasksRequest,
) (*GetHistoryTasksResponse, error) {
	if err := validateTaskRange(
		request.TaskCategory.Type(),
		request.MinTaskKey,
		request.MaxTaskKey,
	); err != nil {
		return nil, err
	}

	resp, err := m.persistence.GetHistoryTasks(request)
	if err != nil {
		return nil, err
	}

	tasks := make([]tasks.Task, 0, len(resp.Tasks))
	for _, blob := range resp.Tasks {
		task, err := m.serializer.DeserializeTask(request.TaskCategory, blob)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return &GetHistoryTasksResponse{
		Tasks:         tasks,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (m *executionManagerImpl) CompleteHistoryTask(
	request *CompleteHistoryTaskRequest,
) error {
	return m.persistence.CompleteHistoryTask(request)
}

func (m *executionManagerImpl) RangeCompleteHistoryTasks(
	request *RangeCompleteHistoryTasksRequest,
) error {
	if err := validateTaskRange(
		request.TaskCategory.Type(),
		request.MinTaskKey,
		request.MaxTaskKey,
	); err != nil {
		return err
	}

	return m.persistence.RangeCompleteHistoryTasks(request)
}

func (m *executionManagerImpl) PutReplicationTaskToDLQ(
	request *PutReplicationTaskToDLQRequest,
) error {
	return m.persistence.PutReplicationTaskToDLQ(request)
}

func (m *executionManagerImpl) GetReplicationTasksFromDLQ(
	request *GetReplicationTasksFromDLQRequest,
) (*GetReplicationTasksFromDLQResponse, error) {
	resp, err := m.persistence.GetReplicationTasksFromDLQ(request)
	if err != nil {
		return nil, err
	}

	category := tasks.CategoryReplication
	tasks := make([]tasks.Task, 0, len(resp.Tasks))
	for _, blob := range resp.Tasks {
		task, err := m.serializer.DeserializeTask(category, blob)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return &GetReplicationTasksFromDLQResponse{
		Tasks:         tasks,
		NextPageToken: resp.NextPageToken,
	}, nil
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

func (m *executionManagerImpl) Close() {
	m.persistence.Close()
}

func (m *executionManagerImpl) trimHistoryNode(
	shardID int32,
	namespaceID string,
	workflowID string,
	runID string,
) {
	response, err := m.GetWorkflowExecution(&GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	if err != nil {
		m.logger.Error("ExecutionManager unable to get mutable state for trimming history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
		return // best effort trim
	}

	executionInfo := response.State.ExecutionInfo
	branchToken, err := getCurrentBranchToken(executionInfo.VersionHistories)
	if err != nil {
		return
	}
	mutableStateLastNodeID := executionInfo.LastFirstEventId
	mutableStateLastNodeTransactionID := executionInfo.LastFirstEventTxnId
	if _, err := m.TrimHistoryBranch(&TrimHistoryBranchRequest{
		ShardID:       shardID,
		BranchToken:   branchToken,
		NodeID:        mutableStateLastNodeID,
		TransactionID: mutableStateLastNodeTransactionID,
	}); err != nil {
		// best effort trim
		m.logger.Error("ExecutionManager unable to trim history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
		return
	}
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

func getCurrentBranchToken(
	versionHistories *historyspb.VersionHistories,
) ([]byte, error) {
	// TODO remove this if check once legacy execution tests are removed
	if versionHistories == nil {
		return nil, serviceerror.NewInternal("version history is empty")
	}
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	return versionHistory.BranchToken, nil
}

func getCurrentBranchLastWriteVersion(
	versionHistories *historyspb.VersionHistories,
) (int64, error) {
	// TODO remove this if check once legacy execution tests are removed
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

func serializeTasks(
	serializer serialization.Serializer,
	inputTasks map[tasks.Category][]tasks.Task,
) (map[tasks.Category][]InternalHistoryTask, error) {
	outputTasks := make(map[tasks.Category][]InternalHistoryTask)
	for category, tasks := range inputTasks {
		serializedTasks := make([]InternalHistoryTask, 0, len(tasks))
		for _, task := range tasks {
			blob, err := serializer.SerializeTask(task)
			if err != nil {
				return nil, err
			}
			serializedTasks = append(serializedTasks, InternalHistoryTask{
				Key:  task.GetKey(),
				Blob: blob,
			})
		}
		outputTasks[category] = serializedTasks
	}
	return outputTasks, nil
}

func validateTaskRange(
	taskCategoryType tasks.CategoryType,
	minTaskKey tasks.Key,
	maxTaskKey tasks.Key,
) error {
	minTaskIDSpecified := minTaskKey.TaskID != 0
	minFireTimeSpecified := !minTaskKey.FireTime.IsZero()
	maxTaskIDSpecified := maxTaskKey.TaskID != 0
	maxFireTimeSpecified := !maxTaskKey.FireTime.IsZero()

	switch taskCategoryType {
	case tasks.CategoryTypeImmediate:
		if !maxTaskIDSpecified {
			serviceerror.NewInvalidArgument("invalid task range, max taskID must be specified for immediate task category")
		}
		if minFireTimeSpecified || maxFireTimeSpecified {
			return serviceerror.NewInvalidArgument("invalid task range, fireTime must be empty for immediate task category")
		}
	case tasks.CategoryTypeScheduled:
		if !maxFireTimeSpecified {
			serviceerror.NewInvalidArgument("invalid task range, max fire time must be specified for scheduled task category")
		}
		if minTaskIDSpecified || maxTaskIDSpecified {
			return serviceerror.NewInvalidArgument("invalid task range, taskID must be empty for scheduled task category")
		}
	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid task category type: %v", taskCategoryType))
	}

	return nil
}
