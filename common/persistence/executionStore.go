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

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
)

type (
	// executionManagerImpl implements ExecutionManager based on ExecutionStore, statsComputer and PayloadSerializer
	executionManagerImpl struct {
		serializer    PayloadSerializer
		persistence   ExecutionStore
		statsComputer statsComputer
		logger        log.Logger
	}
)

var _ ExecutionManager = (*executionManagerImpl)(nil)

// NewExecutionManagerImpl returns new ExecutionManager
func NewExecutionManagerImpl(
	persistence ExecutionStore,
	logger log.Logger,
) ExecutionManager {

	return &executionManagerImpl{
		serializer:    NewPayloadSerializer(),
		persistence:   persistence,
		statsComputer: statsComputer{},
		logger:        logger,
	}
}

func (m *executionManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *executionManagerImpl) GetShardID() int32 {
	return m.persistence.GetShardID()
}

// The below three APIs are related to serialization/deserialization
func (m *executionManagerImpl) GetWorkflowExecution(
	request *GetWorkflowExecutionRequest,
) (*GetWorkflowExecutionResponse, error) {

	response, err := m.persistence.GetWorkflowExecution(request)
	if err != nil {
		return nil, err
	}
	newResponse := &GetWorkflowExecutionResponse{
		State: &persistencespb.WorkflowMutableState{
			ActivityInfos:       response.State.ActivityInfos,
			TimerInfos:          response.State.TimerInfos,
			RequestCancelInfos:  response.State.RequestCancelInfos,
			SignalInfos:         response.State.SignalInfos,
			SignalRequestedIds:  response.State.SignalRequestedIDs,
			Checksum:            response.State.Checksum,
			ChildExecutionInfos: response.State.ChildExecutionInfos,
			ExecutionState:      response.State.ExecutionState,
			NextEventId:         response.State.NextEventID,
		},
	}

	newResponse.State.BufferedEvents, err = m.DeserializeBufferedEvents(response.State.BufferedEvents)
	if err != nil {
		return nil, err
	}
	newResponse.State.ExecutionInfo, err = m.DeserializeExecutionInfo(response.State.ExecutionInfo)
	if err != nil {
		return nil, err
	}

	newResponse.MutableStateStats = m.statsComputer.computeMutableStateStats(response)

	return newResponse, nil
}

func (m *executionManagerImpl) DeserializeExecutionInfo(
	// TODO: info should be a blob
	info *persistencespb.WorkflowExecutionInfo,
) (*persistencespb.WorkflowExecutionInfo, error) {
	newInfo := &persistencespb.WorkflowExecutionInfo{
		CompletionEvent:                   info.CompletionEvent,
		NamespaceId:                       info.NamespaceId,
		WorkflowId:                        info.WorkflowId,
		FirstExecutionRunId:               info.FirstExecutionRunId,
		ParentNamespaceId:                 info.ParentNamespaceId,
		ParentWorkflowId:                  info.ParentWorkflowId,
		ParentRunId:                       info.ParentRunId,
		InitiatedId:                       info.InitiatedId,
		CompletionEventBatchId:            info.CompletionEventBatchId,
		TaskQueue:                         info.TaskQueue,
		WorkflowTypeName:                  info.WorkflowTypeName,
		WorkflowRunTimeout:                info.WorkflowRunTimeout,
		WorkflowExecutionTimeout:          info.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:        info.DefaultWorkflowTaskTimeout,
		LastFirstEventId:                  info.LastFirstEventId,
		LastEventTaskId:                   info.LastEventTaskId,
		LastProcessedEvent:                info.LastProcessedEvent,
		StartTime:                         info.StartTime,
		LastUpdateTime:                    info.LastUpdateTime,
		SignalCount:                       info.SignalCount,
		WorkflowTaskVersion:               info.WorkflowTaskVersion,
		WorkflowTaskScheduleId:            info.WorkflowTaskScheduleId,
		WorkflowTaskStartedId:             info.WorkflowTaskStartedId,
		WorkflowTaskRequestId:             info.WorkflowTaskRequestId,
		WorkflowTaskTimeout:               info.WorkflowTaskTimeout,
		WorkflowTaskAttempt:               info.WorkflowTaskAttempt,
		WorkflowTaskStartedTime:           info.WorkflowTaskStartedTime,
		WorkflowTaskScheduledTime:         info.WorkflowTaskScheduledTime,
		WorkflowTaskOriginalScheduledTime: info.WorkflowTaskOriginalScheduledTime,
		CancelRequested:                   info.CancelRequested,
		StickyTaskQueue:                   info.StickyTaskQueue,
		StickyScheduleToStartTimeout:      info.StickyScheduleToStartTimeout,
		Attempt:                           info.Attempt,
		HasRetryPolicy:                    info.HasRetryPolicy,
		RetryInitialInterval:              info.RetryInitialInterval,
		RetryBackoffCoefficient:           info.RetryBackoffCoefficient,
		RetryMaximumInterval:              info.RetryMaximumInterval,
		RetryMaximumAttempts:              info.RetryMaximumAttempts,
		RetryNonRetryableErrorTypes:       info.RetryNonRetryableErrorTypes,
		EventBranchToken:                  info.EventBranchToken,
		CronSchedule:                      info.CronSchedule,
		AutoResetPoints:                   info.AutoResetPoints,
		SearchAttributes:                  info.SearchAttributes,
		Memo:                              info.Memo,
		ExecutionStats:                    info.ExecutionStats,
		StartVersion:                      info.StartVersion,
		HistorySize:                       info.HistorySize,
		CancelRequestId:                   info.CancelRequestId,
		VersionHistories:                  info.VersionHistories,
		WorkflowRunExpirationTime:         info.WorkflowRunExpirationTime,
		WorkflowExecutionExpirationTime:   info.WorkflowExecutionExpirationTime,
	}

	if newInfo.AutoResetPoints == nil {
		newInfo.AutoResetPoints = &workflowpb.ResetPoints{}
	}

	return newInfo, nil
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

	serializedWorkflowMutation, err := m.SerializeWorkflowMutation(&request.UpdateWorkflowMutation)
	if err != nil {
		return nil, err
	}
	var serializedNewWorkflowSnapshot *InternalWorkflowSnapshot
	if request.NewWorkflowSnapshot != nil {
		serializedNewWorkflowSnapshot, err = m.SerializeWorkflowSnapshot(request.NewWorkflowSnapshot)
		if err != nil {
			return nil, err
		}
	}

	newRequest := &InternalUpdateWorkflowExecutionRequest{
		RangeID: request.RangeID,

		Mode: request.Mode,

		UpdateWorkflowMutation: *serializedWorkflowMutation,
		NewWorkflowSnapshot:    serializedNewWorkflowSnapshot,
	}

	msuss := m.statsComputer.computeMutableStateUpdateStats(newRequest)
	err1 := m.persistence.UpdateWorkflowExecution(newRequest)
	return &UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: msuss}, err1
}

func (m *executionManagerImpl) SerializeExecutionInfo(
	info *persistencespb.WorkflowExecutionInfo,
	// TODO: return blob here
) (*persistencespb.WorkflowExecutionInfo, error) {

	if info == nil {
		return &persistencespb.WorkflowExecutionInfo{}, nil
	}

	return &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                       info.NamespaceId,
		WorkflowId:                        info.WorkflowId,
		FirstExecutionRunId:               info.FirstExecutionRunId,
		ParentNamespaceId:                 info.ParentNamespaceId,
		ParentWorkflowId:                  info.ParentWorkflowId,
		ParentRunId:                       info.ParentRunId,
		InitiatedId:                       info.InitiatedId,
		CompletionEventBatchId:            info.CompletionEventBatchId,
		CompletionEvent:                   info.CompletionEvent,
		TaskQueue:                         info.TaskQueue,
		WorkflowTypeName:                  info.WorkflowTypeName,
		WorkflowRunTimeout:                info.WorkflowRunTimeout,
		WorkflowExecutionTimeout:          info.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:        info.DefaultWorkflowTaskTimeout,
		LastFirstEventId:                  info.LastFirstEventId,
		LastEventTaskId:                   info.LastEventTaskId,
		LastProcessedEvent:                info.LastProcessedEvent,
		StartTime:                         info.StartTime,
		LastUpdateTime:                    info.LastUpdateTime,
		SignalCount:                       info.SignalCount,
		WorkflowTaskVersion:               info.WorkflowTaskVersion,
		WorkflowTaskScheduleId:            info.WorkflowTaskScheduleId,
		WorkflowTaskStartedId:             info.WorkflowTaskStartedId,
		WorkflowTaskRequestId:             info.WorkflowTaskRequestId,
		WorkflowTaskTimeout:               info.WorkflowTaskTimeout,
		WorkflowTaskAttempt:               info.WorkflowTaskAttempt,
		WorkflowTaskStartedTime:           info.WorkflowTaskStartedTime,
		WorkflowTaskScheduledTime:         info.WorkflowTaskScheduledTime,
		WorkflowTaskOriginalScheduledTime: info.WorkflowTaskOriginalScheduledTime,
		CancelRequested:                   info.CancelRequested,
		StickyTaskQueue:                   info.StickyTaskQueue,
		StickyScheduleToStartTimeout:      info.StickyScheduleToStartTimeout,
		AutoResetPoints:                   info.AutoResetPoints,
		Attempt:                           info.Attempt,
		HasRetryPolicy:                    info.HasRetryPolicy,
		RetryInitialInterval:              info.RetryInitialInterval,
		RetryBackoffCoefficient:           info.RetryBackoffCoefficient,
		RetryMaximumInterval:              info.RetryMaximumInterval,
		RetryMaximumAttempts:              info.RetryMaximumAttempts,
		RetryNonRetryableErrorTypes:       info.RetryNonRetryableErrorTypes,
		EventBranchToken:                  info.EventBranchToken,
		CronSchedule:                      info.CronSchedule,
		Memo:                              info.Memo,
		SearchAttributes:                  info.SearchAttributes,
		WorkflowRunExpirationTime:         info.WorkflowRunExpirationTime,
		WorkflowExecutionExpirationTime:   info.WorkflowExecutionExpirationTime,

		ExecutionStats:   info.ExecutionStats,
		VersionHistories: info.VersionHistories,
		CancelRequestId:  info.CancelRequestId,
		HistorySize:      info.HistorySize,
		StartVersion:     info.StartVersion,
	}, nil
}

func (m *executionManagerImpl) ConflictResolveWorkflowExecution(
	request *ConflictResolveWorkflowExecutionRequest,
) error {

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

	serializedNewWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&request.NewWorkflowSnapshot)
	if err != nil {
		return nil, err
	}

	newRequest := &InternalCreateWorkflowExecutionRequest{
		RangeID: request.RangeID,

		Mode: request.Mode,

		PreviousRunID:            request.PreviousRunID,
		PreviousLastWriteVersion: request.PreviousLastWriteVersion,

		NewWorkflowSnapshot: *serializedNewWorkflowSnapshot,
	}

	return m.persistence.CreateWorkflowExecution(newRequest)
}

func (m *executionManagerImpl) SerializeWorkflowMutation(
	input *WorkflowMutation,
) (*InternalWorkflowMutation, error) {

	serializedExecutionInfo, err := m.SerializeExecutionInfo(input.ExecutionInfo)
	if err != nil {
		return nil, err
	}

	var serializedNewBufferedEvents *commonpb.DataBlob
	if len(input.NewBufferedEvents) > 0 {
		serializedNewBufferedEvents, err = m.serializer.SerializeEvents(input.NewBufferedEvents, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
	}

	lastWriteVersion, err := getLastWriteVersion(input.ExecutionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}

	return &InternalWorkflowMutation{
		ExecutionInfo:    serializedExecutionInfo,
		ExecutionState:   input.ExecutionState,
		NextEventID:      input.NextEventID,
		LastWriteVersion: lastWriteVersion,

		UpsertActivityInfos:       input.UpsertActivityInfos,
		DeleteActivityInfos:       input.DeleteActivityInfos,
		UpsertTimerInfos:          input.UpsertTimerInfos,
		DeleteTimerInfos:          input.DeleteTimerInfos,
		UpsertChildExecutionInfos: input.UpsertChildExecutionInfos,
		DeleteChildExecutionInfos: input.DeleteChildExecutionInfos,
		UpsertRequestCancelInfos:  input.UpsertRequestCancelInfos,
		DeleteRequestCancelInfos:  input.DeleteRequestCancelInfos,
		UpsertSignalInfos:         input.UpsertSignalInfos,
		DeleteSignalInfos:         input.DeleteSignalInfos,
		UpsertSignalRequestedIDs:  input.UpsertSignalRequestedIDs,
		DeleteSignalRequestedIDs:  input.DeleteSignalRequestedIDs,
		NewBufferedEvents:         serializedNewBufferedEvents,
		ClearBufferedEvents:       input.ClearBufferedEvents,

		TransferTasks:    input.TransferTasks,
		ReplicationTasks: input.ReplicationTasks,
		TimerTasks:       input.TimerTasks,
		VisibilityTasks:  input.VisibilityTasks,

		Condition: input.Condition,
		Checksum:  input.Checksum,
	}, nil
}

func (m *executionManagerImpl) SerializeWorkflowSnapshot(
	input *WorkflowSnapshot,
) (*InternalWorkflowSnapshot, error) {

	serializedExecutionInfo, err := m.SerializeExecutionInfo(input.ExecutionInfo)
	if err != nil {
		return nil, err
	}

	lastWriteVersion, err := getLastWriteVersion(input.ExecutionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}

	return &InternalWorkflowSnapshot{
		ExecutionInfo:    serializedExecutionInfo,
		ExecutionState:   input.ExecutionState,
		NextEventID:      input.NextEventID,
		LastWriteVersion: lastWriteVersion,

		ActivityInfos:       input.ActivityInfos,
		TimerInfos:          input.TimerInfos,
		ChildExecutionInfos: input.ChildExecutionInfos,
		RequestCancelInfos:  input.RequestCancelInfos,
		SignalInfos:         input.SignalInfos,
		SignalRequestedIDs:  input.SignalRequestedIDs,

		TransferTasks:    input.TransferTasks,
		ReplicationTasks: input.ReplicationTasks,
		TimerTasks:       input.TimerTasks,
		VisibilityTasks:  input.VisibilityTasks,

		Condition: input.Condition,
		Checksum:  input.Checksum,
	}, nil
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
	return m.persistence.GetCurrentExecution(request)
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
		state := &persistencespb.WorkflowMutableState{
			ActivityInfos:       s.ActivityInfos,
			TimerInfos:          s.TimerInfos,
			RequestCancelInfos:  s.RequestCancelInfos,
			SignalInfos:         s.SignalInfos,
			SignalRequestedIds:  s.SignalRequestedIDs,
			Checksum:            s.Checksum,
			ChildExecutionInfos: s.ChildExecutionInfos,
			ExecutionState:      s.ExecutionState,
			NextEventId:         s.NextEventID,
		}

		state.BufferedEvents, err = m.DeserializeBufferedEvents(s.BufferedEvents)
		if err != nil {
			return nil, err
		}
		state.ExecutionInfo, err = m.DeserializeExecutionInfo(s.ExecutionInfo)
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
