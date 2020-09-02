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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
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

func (m *executionManagerImpl) GetShardID() int {
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
		State: &WorkflowMutableState{
			ActivityInfos:       response.State.ActivityInfos,
			TimerInfos:          response.State.TimerInfos,
			RequestCancelInfos:  response.State.RequestCancelInfos,
			SignalInfos:         response.State.SignalInfos,
			SignalRequestedIDs:  response.State.SignalRequestedIDs,
			ReplicationState:    response.State.ReplicationState,
			Checksum:            response.State.Checksum,
			ChildExecutionInfos: response.State.ChildExecutionInfos,
			VersionHistories:    NewVersionHistoriesFromProto(response.State.VersionHistories),
		},
	}

	newResponse.State.BufferedEvents, err = m.DeserializeBufferedEvents(response.State.BufferedEvents)
	if err != nil {
		return nil, err
	}
	newResponse.State.ExecutionInfo, newResponse.State.ExecutionStats, err = m.DeserializeExecutionInfo(response.State.ExecutionInfo)
	if err != nil {
		return nil, err
	}

	newResponse.MutableStateStats = m.statsComputer.computeMutableStateStats(response)

	return newResponse, nil
}

func (m *executionManagerImpl) DeserializeExecutionInfo(
	info *InternalWorkflowExecutionInfo,
) (*WorkflowExecutionInfo, *persistenceblobs.ExecutionStats, error) {
	newInfo := &WorkflowExecutionInfo{
		CompletionEvent:                        info.CompletionEvent,
		NamespaceID:                            info.NamespaceID,
		WorkflowID:                             info.WorkflowID,
		RunID:                                  info.RunID,
		FirstExecutionRunID:                    info.FirstExecutionRunID,
		ParentNamespaceID:                      info.ParentNamespaceID,
		ParentWorkflowID:                       info.ParentWorkflowID,
		ParentRunID:                            info.ParentRunID,
		InitiatedID:                            info.InitiatedID,
		CompletionEventBatchID:                 info.CompletionEventBatchID,
		TaskQueue:                              info.TaskQueue,
		WorkflowTypeName:                       info.WorkflowTypeName,
		WorkflowRunTimeout:                     info.WorkflowRunTimeout,
		WorkflowExecutionTimeout:               info.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:             info.DefaultWorkflowTaskTimeout,
		State:                                  info.State,
		Status:                                 info.Status,
		LastFirstEventID:                       info.LastFirstEventID,
		LastEventTaskID:                        info.LastEventTaskID,
		NextEventID:                            info.NextEventID,
		LastProcessedEvent:                     info.LastProcessedEvent,
		StartTimestamp:                         info.StartTimestamp,
		LastUpdatedTimestamp:                   info.LastUpdateTimestamp,
		CreateRequestID:                        info.CreateRequestID,
		SignalCount:                            info.SignalCount,
		WorkflowTaskVersion:                    info.WorkflowTaskVersion,
		WorkflowTaskScheduleID:                 info.WorkflowTaskScheduleID,
		WorkflowTaskStartedID:                  info.WorkflowTaskStartedID,
		WorkflowTaskRequestID:                  info.WorkflowTaskRequestID,
		WorkflowTaskTimeout:                    info.WorkflowTaskTimeout,
		WorkflowTaskAttempt:                    info.WorkflowTaskAttempt,
		WorkflowTaskStartedTimestamp:           info.WorkflowTaskStartedTimestamp,
		WorkflowTaskScheduledTimestamp:         info.WorkflowTaskScheduledTimestamp,
		WorkflowTaskOriginalScheduledTimestamp: info.WorkflowTaskOriginalScheduledTimestamp,
		CancelRequested:                        info.CancelRequested,
		CancelRequestID:                        info.CancelRequestID,
		StickyTaskQueue:                        info.StickyTaskQueue,
		StickyScheduleToStartTimeout:           info.StickyScheduleToStartTimeout,
		ClientLibraryVersion:                   info.ClientLibraryVersion,
		ClientFeatureVersion:                   info.ClientFeatureVersion,
		ClientImpl:                             info.ClientImpl,
		Attempt:                                info.Attempt,
		HasRetryPolicy:                         info.HasRetryPolicy,
		InitialInterval:                        info.InitialInterval,
		BackoffCoefficient:                     info.BackoffCoefficient,
		MaximumInterval:                        info.MaximumInterval,
		WorkflowExpirationTime:                 info.ExpirationTime,
		MaximumAttempts:                        info.MaximumAttempts,
		NonRetryableErrorTypes:                 info.NonRetryableErrorTypes,
		BranchToken:                            info.BranchToken,
		CronSchedule:                           info.CronSchedule,
		AutoResetPoints:                        info.AutoResetPoints,
		SearchAttributes:                       info.SearchAttributes,
		Memo:                                   info.Memo,
	}

	if newInfo.AutoResetPoints == nil {
		newInfo.AutoResetPoints = &workflowpb.ResetPoints{}
	}

	newStats := &persistenceblobs.ExecutionStats{
		HistorySize: info.HistorySize,
	}
	return newInfo, newStats, nil
}

func (m *executionManagerImpl) DeserializeBufferedEvents(
	blobs []*serialization.DataBlob,
) ([]*historypb.HistoryEvent, error) {

	events := make([]*historypb.HistoryEvent, 0)
	for _, b := range blobs {
		if b == nil {
			// Should not happen, log and discard to prevent callers from consuming
			m.logger.Warn("discarding nil buffered event")
			continue
		}

		history, err := m.serializer.DeserializeBatchEvents(b)
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

	serializedWorkflowMutation, err := m.SerializeWorkflowMutation(&request.UpdateWorkflowMutation, request.Encoding)
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
	info *WorkflowExecutionInfo,
	stats *persistenceblobs.ExecutionStats,
) (*InternalWorkflowExecutionInfo, error) {

	if info == nil {
		return &InternalWorkflowExecutionInfo{}, nil
	}

	return &InternalWorkflowExecutionInfo{
		NamespaceID:                            info.NamespaceID,
		WorkflowID:                             info.WorkflowID,
		RunID:                                  info.RunID,
		FirstExecutionRunID:                    info.FirstExecutionRunID,
		ParentNamespaceID:                      info.ParentNamespaceID,
		ParentWorkflowID:                       info.ParentWorkflowID,
		ParentRunID:                            info.ParentRunID,
		InitiatedID:                            info.InitiatedID,
		CompletionEventBatchID:                 info.CompletionEventBatchID,
		CompletionEvent:                        info.CompletionEvent,
		TaskQueue:                              info.TaskQueue,
		WorkflowTypeName:                       info.WorkflowTypeName,
		WorkflowRunTimeout:                     info.WorkflowRunTimeout,
		WorkflowExecutionTimeout:               info.WorkflowExecutionTimeout,
		DefaultWorkflowTaskTimeout:             info.DefaultWorkflowTaskTimeout,
		State:                                  info.State,
		Status:                                 info.Status,
		LastFirstEventID:                       info.LastFirstEventID,
		LastEventTaskID:                        info.LastEventTaskID,
		NextEventID:                            info.NextEventID,
		LastProcessedEvent:                     info.LastProcessedEvent,
		StartTimestamp:                         info.StartTimestamp,
		LastUpdateTimestamp:                    info.LastUpdatedTimestamp,
		CreateRequestID:                        info.CreateRequestID,
		SignalCount:                            info.SignalCount,
		WorkflowTaskVersion:                    info.WorkflowTaskVersion,
		WorkflowTaskScheduleID:                 info.WorkflowTaskScheduleID,
		WorkflowTaskStartedID:                  info.WorkflowTaskStartedID,
		WorkflowTaskRequestID:                  info.WorkflowTaskRequestID,
		WorkflowTaskTimeout:                    info.WorkflowTaskTimeout,
		WorkflowTaskAttempt:                    info.WorkflowTaskAttempt,
		WorkflowTaskStartedTimestamp:           info.WorkflowTaskStartedTimestamp,
		WorkflowTaskScheduledTimestamp:         info.WorkflowTaskScheduledTimestamp,
		WorkflowTaskOriginalScheduledTimestamp: info.WorkflowTaskOriginalScheduledTimestamp,
		CancelRequested:                        info.CancelRequested,
		CancelRequestID:                        info.CancelRequestID,
		StickyTaskQueue:                        info.StickyTaskQueue,
		StickyScheduleToStartTimeout:           info.StickyScheduleToStartTimeout,
		ClientLibraryVersion:                   info.ClientLibraryVersion,
		ClientFeatureVersion:                   info.ClientFeatureVersion,
		ClientImpl:                             info.ClientImpl,
		AutoResetPoints:                        info.AutoResetPoints,
		Attempt:                                info.Attempt,
		HasRetryPolicy:                         info.HasRetryPolicy,
		InitialInterval:                        info.InitialInterval,
		BackoffCoefficient:                     info.BackoffCoefficient,
		MaximumInterval:                        info.MaximumInterval,
		ExpirationTime:                         info.WorkflowExpirationTime,
		MaximumAttempts:                        info.MaximumAttempts,
		NonRetryableErrorTypes:                 info.NonRetryableErrorTypes,
		BranchToken:                            info.BranchToken,
		CronSchedule:                           info.CronSchedule,
		Memo:                                   info.Memo,
		SearchAttributes:                       info.SearchAttributes,

		// attributes which are not related to mutable state
		HistorySize: stats.HistorySize,
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
		serializedCurrentWorkflowMutation, err = m.SerializeWorkflowMutation(request.CurrentWorkflowMutation, request.Encoding)
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

	if request.CurrentWorkflowMutation != nil && request.CurrentWorkflowCAS != nil {
		return serviceerror.NewInternal("ConflictResolveWorkflowExecution: current workflow & current workflow CAS both set")
	}

	newRequest := &InternalConflictResolveWorkflowExecutionRequest{
		RangeID: request.RangeID,

		Mode: request.Mode,

		ResetWorkflowSnapshot: *serializedResetWorkflowSnapshot,

		NewWorkflowSnapshot: serializedNewWorkflowMutation,

		CurrentWorkflowMutation: serializedCurrentWorkflowMutation,

		// TODO deprecate this once nDC migration is completed
		//  basically should use CurrentWorkflowMutation instead
		CurrentWorkflowCAS: request.CurrentWorkflowCAS,
	}
	return m.persistence.ConflictResolveWorkflowExecution(newRequest)
}

func (m *executionManagerImpl) ResetWorkflowExecution(
	request *ResetWorkflowExecutionRequest,
) error {

	serializedNewWorkflowSnapshot, err := m.SerializeWorkflowSnapshot(&request.NewWorkflowSnapshot)
	if err != nil {
		return err
	}
	var serializedUpdateWorkflowSnapshot *InternalWorkflowMutation
	if request.CurrentWorkflowMutation != nil {
		serializedUpdateWorkflowSnapshot, err = m.SerializeWorkflowMutation(request.CurrentWorkflowMutation, request.Encoding)
		if err != nil {
			return err
		}
	}

	newRequest := &InternalResetWorkflowExecutionRequest{
		RangeID: request.RangeID,

		BaseRunID:          request.BaseRunID,
		BaseRunNextEventID: request.BaseRunNextEventID,

		CurrentRunID:          request.CurrentRunID,
		CurrentRunNextEventID: request.CurrentRunNextEventID,

		CurrentWorkflowMutation: serializedUpdateWorkflowSnapshot,

		NewWorkflowSnapshot: *serializedNewWorkflowSnapshot,
	}
	return m.persistence.ResetWorkflowExecution(newRequest)
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
	encoding enumspb.EncodingType,
) (*InternalWorkflowMutation, error) {

	serializedExecutionInfo, err := m.SerializeExecutionInfo(
		input.ExecutionInfo,
		input.ExecutionStats,
	)
	if err != nil {
		return nil, err
	}

	var serializedNewBufferedEvents *serialization.DataBlob
	if len(input.NewBufferedEvents) > 0 {
		serializedNewBufferedEvents, err = m.serializer.SerializeBatchEvents(input.NewBufferedEvents, encoding)
		if err != nil {
			return nil, err
		}
	}

	startVersion, err := getStartVersion(input.VersionHistories, input.ReplicationState)
	if err != nil {
		return nil, err
	}
	lastWriteVersion, err := getLastWriteVersion(input.VersionHistories, input.ReplicationState)
	if err != nil {
		return nil, err
	}

	return &InternalWorkflowMutation{
		ExecutionInfo:    serializedExecutionInfo,
		ReplicationState: input.ReplicationState,
		VersionHistories: input.VersionHistories.ToProto(),
		StartVersion:     startVersion,
		LastWriteVersion: lastWriteVersion,

		UpsertActivityInfos:       input.UpsertActivityInfos,
		DeleteActivityInfos:       input.DeleteActivityInfos,
		UpsertTimerInfos:          input.UpsertTimerInfos,
		DeleteTimerInfos:          input.DeleteTimerInfos,
		UpsertChildExecutionInfos: input.UpsertChildExecutionInfos,
		DeleteChildExecutionInfo:  input.DeleteChildExecutionInfo,
		UpsertRequestCancelInfos:  input.UpsertRequestCancelInfos,
		DeleteRequestCancelInfo:   input.DeleteRequestCancelInfo,
		UpsertSignalInfos:         input.UpsertSignalInfos,
		DeleteSignalInfo:          input.DeleteSignalInfo,
		UpsertSignalRequestedIDs:  input.UpsertSignalRequestedIDs,
		DeleteSignalRequestedID:   input.DeleteSignalRequestedID,
		NewBufferedEvents:         serializedNewBufferedEvents,
		ClearBufferedEvents:       input.ClearBufferedEvents,

		TransferTasks:    input.TransferTasks,
		ReplicationTasks: input.ReplicationTasks,
		TimerTasks:       input.TimerTasks,

		Condition: input.Condition,
		Checksum:  input.Checksum,
	}, nil
}

func (m *executionManagerImpl) SerializeWorkflowSnapshot(
	input *WorkflowSnapshot,
) (*InternalWorkflowSnapshot, error) {

	serializedExecutionInfo, err := m.SerializeExecutionInfo(
		input.ExecutionInfo,
		input.ExecutionStats,
	)
	if err != nil {
		return nil, err
	}

	startVersion, err := getStartVersion(input.VersionHistories, input.ReplicationState)
	if err != nil {
		return nil, err
	}
	lastWriteVersion, err := getLastWriteVersion(input.VersionHistories, input.ReplicationState)
	if err != nil {
		return nil, err
	}

	return &InternalWorkflowSnapshot{
		ExecutionInfo:    serializedExecutionInfo,
		ReplicationState: input.ReplicationState,
		VersionHistories: input.VersionHistories.ToProto(),
		StartVersion:     startVersion,
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

		Condition: input.Condition,
		Checksum:  input.Checksum,
	}, nil
}

func (m *executionManagerImpl) SerializeVersionHistories(
	versionHistories *VersionHistories,
	encoding enumspb.EncodingType,
) (*serialization.DataBlob, error) {

	if versionHistories == nil {
		return nil, nil
	}
	return m.serializer.SerializeVersionHistories(versionHistories.ToProto(), encoding)
}

func (m *executionManagerImpl) DeserializeVersionHistories(
	blob *serialization.DataBlob,
) (*VersionHistories, error) {

	if blob == nil {
		return nil, nil
	}
	versionHistories, err := m.serializer.DeserializeVersionHistories(blob)
	if err != nil {
		return nil, err
	}
	return NewVersionHistoriesFromProto(versionHistories), nil
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
		ExecutionInfos: make([]*WorkflowExecutionInfo, len(response.ExecutionInfos), len(response.ExecutionInfos)),
		PageToken:      response.NextPageToken,
	}
	for i, info := range response.ExecutionInfos {
		newResponse.ExecutionInfos[i], _, err = m.DeserializeExecutionInfo(info)
		if err != nil {
			return nil, err
		}
	}
	return newResponse, nil
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

func getStartVersion(
	versionHistories *VersionHistories,
	replicationState *persistenceblobs.ReplicationState,
) (int64, error) {

	if replicationState == nil && versionHistories == nil {
		return common.EmptyVersion, nil
	}

	if replicationState != nil {
		return replicationState.StartVersion, nil
	}

	versionHistory, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return 0, err
	}
	versionHistoryItem, err := versionHistory.GetFirstItem()
	if err != nil {
		return 0, err
	}
	return versionHistoryItem.GetVersion(), nil
}

func getLastWriteVersion(
	versionHistories *VersionHistories,
	replicationState *persistenceblobs.ReplicationState,
) (int64, error) {

	if replicationState == nil && versionHistories == nil {
		return common.EmptyVersion, nil
	}

	if replicationState != nil {
		return replicationState.LastWriteVersion, nil
	}

	versionHistory, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return 0, err
	}
	versionHistoryItem, err := versionHistory.GetLastItem()
	if err != nil {
		return 0, err
	}
	return versionHistoryItem.GetVersion(), nil
}
