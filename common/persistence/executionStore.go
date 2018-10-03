// Copyright (c) 2017 Uber Technologies, Inc.
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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (

	// executionManagerImpl implements ExecutionManager based on ExecutionStore, statsComputer and HistorySerializer
	executionManagerImpl struct {
		serializer    HistorySerializer
		persistence   ExecutionStore
		statsComputer statsComputer
	}
)

var _ ExecutionManager = (*executionManagerImpl)(nil)

// NewExecutionManagerImpl returns new ExecutionManager
func NewExecutionManagerImpl(persistence ExecutionStore) ExecutionManager {
	return &executionManagerImpl{
		serializer:    NewHistorySerializer(),
		persistence:   persistence,
		statsComputer: statsComputer{},
	}
}

//The below three APIs are related to serialization/deserialization
func (m *executionManagerImpl) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	response, err := m.persistence.GetWorkflowExecution(request)
	if err != nil {
		return nil, err
	}
	newResponse := &GetWorkflowExecutionResponse{
		State: &WorkflowMutableState{
			TimerInfos:         response.State.TimerInfos,
			RequestCancelInfos: response.State.RequestCancelInfos,
			SignalInfos:        response.State.SignalInfos,
			SignalRequestedIDs: response.State.SignalRequestedIDs,
			ReplicationState:   response.State.ReplicationState,
		},
	}

	newResponse.State.ActivitInfos, err = m.DeserializeActivityInfos(response.State.ActivitInfos)
	if err != nil {
		return nil, err
	}
	newResponse.State.ChildExecutionInfos, err = m.DeserializeChildExecutionInfos(response.State.ChildExecutionInfos)
	if err != nil {
		return nil, err
	}
	newResponse.State.BufferedEvents, err = m.DeserializeBufferedEvents(response.State.BufferedEvents)
	if err != nil {
		return nil, err
	}
	newResponse.State.BufferedReplicationTasks, err = m.DeserializeBufferedReplicationTasks(response.State.BufferedReplicationTasks)
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

func (m *executionManagerImpl) DeserializeExecutionInfo(info *InternalWorkflowExecutionInfo) (*WorkflowExecutionInfo, error) {
	completionEvent, err := m.serializer.DeserializeEvent(info.CompletionEvent)
	if err != nil {
		return nil, err
	}
	newInfo := &WorkflowExecutionInfo{
		CompletionEvent: completionEvent,

		DomainID:                     info.DomainID,
		WorkflowID:                   info.WorkflowID,
		RunID:                        info.RunID,
		ParentDomainID:               info.ParentDomainID,
		ParentWorkflowID:             info.ParentWorkflowID,
		ParentRunID:                  info.ParentRunID,
		InitiatedID:                  info.InitiatedID,
		TaskList:                     info.TaskList,
		WorkflowTypeName:             info.WorkflowTypeName,
		WorkflowTimeout:              info.WorkflowTimeout,
		DecisionTimeoutValue:         info.DecisionTimeoutValue,
		ExecutionContext:             info.ExecutionContext,
		State:                        info.State,
		CloseStatus:                  info.CloseStatus,
		LastFirstEventID:             info.LastFirstEventID,
		NextEventID:                  info.NextEventID,
		LastProcessedEvent:           info.LastProcessedEvent,
		StartTimestamp:               info.StartTimestamp,
		LastUpdatedTimestamp:         info.LastUpdatedTimestamp,
		CreateRequestID:              info.CreateRequestID,
		HistorySize:                  info.HistorySize,
		DecisionVersion:              info.DecisionVersion,
		DecisionScheduleID:           info.DecisionScheduleID,
		DecisionStartedID:            info.DecisionStartedID,
		DecisionRequestID:            info.DecisionRequestID,
		DecisionTimeout:              info.DecisionTimeout,
		DecisionAttempt:              info.DecisionAttempt,
		DecisionTimestamp:            info.DecisionTimestamp,
		CancelRequested:              info.CancelRequested,
		CancelRequestID:              info.CancelRequestID,
		StickyTaskList:               info.StickyTaskList,
		StickyScheduleToStartTimeout: info.StickyScheduleToStartTimeout,
		ClientLibraryVersion:         info.ClientLibraryVersion,
		ClientFeatureVersion:         info.ClientFeatureVersion,
		ClientImpl:                   info.ClientImpl,
		Attempt:                      info.Attempt,
		HasRetryPolicy:               info.HasRetryPolicy,
		InitialInterval:              info.InitialInterval,
		BackoffCoefficient:           info.BackoffCoefficient,
		MaximumInterval:              info.MaximumInterval,
		ExpirationTime:               info.ExpirationTime,
		MaximumAttempts:              info.MaximumAttempts,
		NonRetriableErrors:           info.NonRetriableErrors,
	}
	return newInfo, nil
}

func (m *executionManagerImpl) DeserializeBufferedReplicationTasks(tasks map[int64]*InternalBufferedReplicationTask) (map[int64]*BufferedReplicationTask, error) {
	newBRTs := make(map[int64]*BufferedReplicationTask, 0)
	for k, v := range tasks {
		history, err := m.serializer.DeserializeBatchEvents(v.History)
		if err != nil {
			return nil, err
		}
		newHistory, err := m.serializer.DeserializeBatchEvents(v.NewRunHistory)
		if err != nil {
			return nil, err
		}
		b := &BufferedReplicationTask{
			FirstEventID: v.FirstEventID,
			NextEventID:  v.NextEventID,
			Version:      v.Version,

			History:       history,
			NewRunHistory: newHistory,
		}
		newBRTs[k] = b
	}
	return newBRTs, nil
}

func (m *executionManagerImpl) DeserializeBufferedEvents(blobs []*DataBlob) ([]*workflow.HistoryEvent, error) {
	events := make([]*workflow.HistoryEvent, 0)
	for _, b := range blobs {
		history, err := m.serializer.DeserializeBatchEvents(b)
		if err != nil {
			return nil, err
		}
		events = append(events, history...)
	}
	return events, nil
}

func (m *executionManagerImpl) DeserializeChildExecutionInfos(infos map[int64]*InternalChildExecutionInfo) (map[int64]*ChildExecutionInfo, error) {
	newInfos := make(map[int64]*ChildExecutionInfo, 0)
	for k, v := range infos {
		initiatedEvent, err := m.serializer.DeserializeEvent(v.InitiatedEvent)
		if err != nil {
			return nil, err
		}
		startedEvent, err := m.serializer.DeserializeEvent(v.StartedEvent)
		if err != nil {
			return nil, err
		}
		c := &ChildExecutionInfo{
			InitiatedEvent: initiatedEvent,
			StartedEvent:   startedEvent,

			Version:         v.Version,
			InitiatedID:     v.InitiatedID,
			StartedID:       v.StartedID,
			CreateRequestID: v.CreateRequestID,
		}
		newInfos[k] = c
	}
	return newInfos, nil
}

func (m *executionManagerImpl) DeserializeActivityInfos(infos map[int64]*InternalActivityInfo) (map[int64]*ActivityInfo, error) {
	newInfos := make(map[int64]*ActivityInfo, 0)
	for k, v := range infos {
		scheduledEvent, err := m.serializer.DeserializeEvent(v.ScheduledEvent)
		if err != nil {
			return nil, err
		}
		startedEvent, err := m.serializer.DeserializeEvent(v.StartedEvent)
		if err != nil {
			return nil, err
		}
		a := &ActivityInfo{
			ScheduledEvent: scheduledEvent,
			StartedEvent:   startedEvent,

			Version:                  v.Version,
			ScheduleID:               v.ScheduleID,
			ScheduledTime:            v.ScheduledTime,
			StartedID:                v.StartedID,
			StartedTime:              v.StartedTime,
			ActivityID:               v.ActivityID,
			RequestID:                v.RequestID,
			Details:                  v.Details,
			ScheduleToStartTimeout:   v.ScheduleToStartTimeout,
			ScheduleToCloseTimeout:   v.ScheduleToCloseTimeout,
			StartToCloseTimeout:      v.StartToCloseTimeout,
			HeartbeatTimeout:         v.HeartbeatTimeout,
			CancelRequested:          v.CancelRequested,
			CancelRequestID:          v.CancelRequestID,
			LastHeartBeatUpdatedTime: v.LastHeartBeatUpdatedTime,
			TimerTaskStatus:          v.TimerTaskStatus,
			Attempt:                  v.Attempt,
			DomainID:                 v.DomainID,
			StartedIdentity:          v.StartedIdentity,
			TaskList:                 v.TaskList,
			HasRetryPolicy:           v.HasRetryPolicy,
			InitialInterval:          v.InitialInterval,
			BackoffCoefficient:       v.BackoffCoefficient,
			MaximumInterval:          v.MaximumInterval,
			ExpirationTime:           v.ExpirationTime,
			MaximumAttempts:          v.MaximumAttempts,
			NonRetriableErrors:       v.NonRetriableErrors,
			LastTimeoutVisibility:    v.LastTimeoutVisibility,
		}
		newInfos[k] = a
	}
	return newInfos, nil
}

func (m *executionManagerImpl) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) (*UpdateWorkflowExecutionResponse, error) {
	executionInfo, err := m.SerializeExecutionInfo(request.ExecutionInfo, request.Encoding)
	if err != nil {
		return nil, err
	}
	upsertActivityInfos, err := m.SerializeUpsertActivityInfos(request.UpsertActivityInfos, request.Encoding)
	if err != nil {
		return nil, err
	}
	upsertChildExecutionInfos, err := m.SerializeUpsertChildExecutionInfos(request.UpsertChildExecutionInfos, request.Encoding)
	if err != nil {
		return nil, err
	}
	var newBufferedEvents *DataBlob
	if request.NewBufferedEvents != nil {
		newBufferedEvents, err = m.serializer.SerializeBatchEvents(request.NewBufferedEvents, request.Encoding)
		if err != nil {
			return nil, err
		}
	}
	newBufferedReplicationTask, err := m.SerializeNewBufferedReplicationTask(request.NewBufferedReplicationTask, request.Encoding)
	if err != nil {
		return nil, err
	}

	newRequest := &InternalUpdateWorkflowExecutionRequest{
		ExecutionInfo:              executionInfo,
		UpsertActivityInfos:        upsertActivityInfos,
		UpsertChildExecutionInfos:  upsertChildExecutionInfos,
		NewBufferedEvents:          newBufferedEvents,
		NewBufferedReplicationTask: newBufferedReplicationTask,

		ReplicationState:              request.ReplicationState,
		TransferTasks:                 request.TransferTasks,
		TimerTasks:                    request.TimerTasks,
		ReplicationTasks:              request.ReplicationTasks,
		DeleteTimerTask:               request.DeleteTimerTask,
		Condition:                     request.Condition,
		RangeID:                       request.RangeID,
		ContinueAsNew:                 request.ContinueAsNew,
		FinishExecution:               request.FinishExecution,
		FinishedExecutionTTL:          request.FinishedExecutionTTL,
		DeleteActivityInfos:           request.DeleteActivityInfos,
		UpserTimerInfos:               request.UpserTimerInfos,
		DeleteTimerInfos:              request.DeleteTimerInfos,
		DeleteChildExecutionInfo:      request.DeleteChildExecutionInfo,
		UpsertRequestCancelInfos:      request.UpsertRequestCancelInfos,
		DeleteRequestCancelInfo:       request.DeleteRequestCancelInfo,
		UpsertSignalInfos:             request.UpsertSignalInfos,
		DeleteSignalInfo:              request.DeleteSignalInfo,
		UpsertSignalRequestedIDs:      request.UpsertSignalRequestedIDs,
		DeleteSignalRequestedID:       request.DeleteSignalRequestedID,
		ClearBufferedEvents:           request.ClearBufferedEvents,
		DeleteBufferedReplicationTask: request.DeleteBufferedReplicationTask,
	}
	msuss := m.statsComputer.computeMutableStateUpdateStats(newRequest)
	return &UpdateWorkflowExecutionResponse{MutableStateUpdateSessionStats: msuss}, m.persistence.UpdateWorkflowExecution(newRequest)
}

func (m *executionManagerImpl) SerializeNewBufferedReplicationTask(task *BufferedReplicationTask, encoding common.EncodingType) (*InternalBufferedReplicationTask, error) {
	if task == nil {
		return nil, nil
	}
	var history, newHistory *DataBlob
	var err error
	if task.History != nil {
		history, err = m.serializer.SerializeBatchEvents(task.History, encoding)
		if err != nil {
			return nil, err
		}
	}

	if task.NewRunHistory != nil {
		newHistory, err = m.serializer.SerializeBatchEvents(task.NewRunHistory, encoding)
		if err != nil {
			return nil, err
		}
	}

	return &InternalBufferedReplicationTask{
		FirstEventID: task.FirstEventID,
		NextEventID:  task.NextEventID,
		Version:      task.Version,

		History:       history,
		NewRunHistory: newHistory,
	}, nil
}

func (m *executionManagerImpl) SerializeUpsertChildExecutionInfos(infos []*ChildExecutionInfo, encoding common.EncodingType) ([]*InternalChildExecutionInfo, error) {
	newInfos := make([]*InternalChildExecutionInfo, 0)
	for _, v := range infos {
		initiatedEvent, err := m.serializer.SerializeEvent(v.InitiatedEvent, encoding)
		if err != nil {
			return nil, err
		}
		startedEvent, err := m.serializer.SerializeEvent(v.StartedEvent, encoding)
		if err != nil {
			return nil, err
		}
		i := &InternalChildExecutionInfo{
			InitiatedEvent: initiatedEvent,
			StartedEvent:   startedEvent,

			Version:         v.Version,
			InitiatedID:     v.InitiatedID,
			CreateRequestID: v.CreateRequestID,
			StartedID:       v.StartedID,
		}
		newInfos = append(newInfos, i)
	}
	return newInfos, nil
}

func (m *executionManagerImpl) SerializeUpsertActivityInfos(infos []*ActivityInfo, encoding common.EncodingType) ([]*InternalActivityInfo, error) {
	newInfos := make([]*InternalActivityInfo, 0)
	for _, v := range infos {
		scheduledEvent, err := m.serializer.SerializeEvent(v.ScheduledEvent, encoding)
		if err != nil {
			return nil, err
		}
		startedEvent, err := m.serializer.SerializeEvent(v.StartedEvent, encoding)
		if err != nil {
			return nil, err
		}
		i := &InternalActivityInfo{
			Version:                  v.Version,
			ScheduleID:               v.ScheduleID,
			ScheduledEvent:           scheduledEvent,
			ScheduledTime:            v.ScheduledTime,
			StartedID:                v.StartedID,
			StartedEvent:             startedEvent,
			StartedTime:              v.StartedTime,
			ActivityID:               v.ActivityID,
			RequestID:                v.RequestID,
			Details:                  v.Details,
			ScheduleToStartTimeout:   v.ScheduleToStartTimeout,
			ScheduleToCloseTimeout:   v.ScheduleToCloseTimeout,
			StartToCloseTimeout:      v.StartToCloseTimeout,
			HeartbeatTimeout:         v.HeartbeatTimeout,
			CancelRequested:          v.CancelRequested,
			CancelRequestID:          v.CancelRequestID,
			LastHeartBeatUpdatedTime: v.LastHeartBeatUpdatedTime,
			TimerTaskStatus:          v.TimerTaskStatus,
			Attempt:                  v.Attempt,
			DomainID:                 v.DomainID,
			StartedIdentity:          v.StartedIdentity,
			TaskList:                 v.TaskList,
			HasRetryPolicy:           v.HasRetryPolicy,
			InitialInterval:          v.InitialInterval,
			BackoffCoefficient:       v.BackoffCoefficient,
			MaximumInterval:          v.MaximumInterval,
			ExpirationTime:           v.ExpirationTime,
			MaximumAttempts:          v.MaximumAttempts,
			NonRetriableErrors:       v.NonRetriableErrors,
			LastTimeoutVisibility:    v.LastTimeoutVisibility,
		}
		newInfos = append(newInfos, i)
	}
	return newInfos, nil
}

func (m *executionManagerImpl) SerializeExecutionInfo(info *WorkflowExecutionInfo, encoding common.EncodingType) (*InternalWorkflowExecutionInfo, error) {
	if info == nil {
		return &InternalWorkflowExecutionInfo{
			CompletionEvent: &DataBlob{},
		}, nil
	}
	var completionEvent *DataBlob
	var err error
	completionEvent, err = m.serializer.SerializeEvent(info.CompletionEvent, encoding)
	if err != nil {
		return nil, err
	}

	return &InternalWorkflowExecutionInfo{
		DomainID:                     info.DomainID,
		WorkflowID:                   info.WorkflowID,
		RunID:                        info.RunID,
		ParentDomainID:               info.ParentDomainID,
		ParentWorkflowID:             info.ParentWorkflowID,
		ParentRunID:                  info.ParentRunID,
		InitiatedID:                  info.InitiatedID,
		CompletionEvent:              completionEvent,
		TaskList:                     info.TaskList,
		WorkflowTypeName:             info.WorkflowTypeName,
		WorkflowTimeout:              info.WorkflowTimeout,
		DecisionTimeoutValue:         info.DecisionTimeoutValue,
		ExecutionContext:             info.ExecutionContext,
		State:                        info.State,
		CloseStatus:                  info.CloseStatus,
		LastFirstEventID:             info.LastFirstEventID,
		NextEventID:                  info.NextEventID,
		LastProcessedEvent:           info.LastProcessedEvent,
		StartTimestamp:               info.StartTimestamp,
		LastUpdatedTimestamp:         info.LastUpdatedTimestamp,
		CreateRequestID:              info.CreateRequestID,
		HistorySize:                  info.HistorySize,
		DecisionVersion:              info.DecisionVersion,
		DecisionScheduleID:           info.DecisionScheduleID,
		DecisionStartedID:            info.DecisionStartedID,
		DecisionRequestID:            info.DecisionRequestID,
		DecisionTimeout:              info.DecisionTimeout,
		DecisionAttempt:              info.DecisionAttempt,
		DecisionTimestamp:            info.DecisionTimestamp,
		CancelRequested:              info.CancelRequested,
		CancelRequestID:              info.CancelRequestID,
		StickyTaskList:               info.StickyTaskList,
		StickyScheduleToStartTimeout: info.StickyScheduleToStartTimeout,
		ClientLibraryVersion:         info.ClientLibraryVersion,
		ClientFeatureVersion:         info.ClientFeatureVersion,
		ClientImpl:                   info.ClientImpl,
		Attempt:                      info.Attempt,
		HasRetryPolicy:               info.HasRetryPolicy,
		InitialInterval:              info.InitialInterval,
		BackoffCoefficient:           info.BackoffCoefficient,
		MaximumInterval:              info.MaximumInterval,
		ExpirationTime:               info.ExpirationTime,
		MaximumAttempts:              info.MaximumAttempts,
		NonRetriableErrors:           info.NonRetriableErrors,
	}, nil
}

func (m *executionManagerImpl) ResetMutableState(request *ResetMutableStateRequest) error {
	executionInfo, err := m.SerializeExecutionInfo(request.ExecutionInfo, request.Encoding)
	if err != nil {
		return err
	}
	insertActivityInfos, err := m.SerializeUpsertActivityInfos(request.InsertActivityInfos, request.Encoding)
	if err != nil {
		return err
	}
	insertChildExecutionInfos, err := m.SerializeUpsertChildExecutionInfos(request.InsertChildExecutionInfos, request.Encoding)
	if err != nil {
		return err
	}

	newRequest := &InternalResetMutableStateRequest{
		PrevRunID:                 request.PrevRunID,
		ExecutionInfo:             executionInfo,
		ReplicationState:          request.ReplicationState,
		Condition:                 request.Condition,
		RangeID:                   request.RangeID,
		InsertActivityInfos:       insertActivityInfos,
		InsertTimerInfos:          request.InsertTimerInfos,
		InsertChildExecutionInfos: insertChildExecutionInfos,
		InsertRequestCancelInfos:  request.InsertRequestCancelInfos,
		InsertSignalInfos:         request.InsertSignalInfos,
		InsertSignalRequestedIDs:  request.InsertSignalRequestedIDs,
	}
	return m.persistence.ResetMutableState(newRequest)
}

func (m *executionManagerImpl) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	return m.persistence.CreateWorkflowExecution(request)
}
func (m *executionManagerImpl) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	return m.persistence.DeleteWorkflowExecution(request)
}

func (m *executionManagerImpl) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	return m.persistence.GetCurrentExecution(request)
}

// Transfer task related methods
func (m *executionManagerImpl) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	return m.persistence.GetTransferTasks(request)
}
func (m *executionManagerImpl) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	return m.persistence.CompleteTransferTask(request)
}
func (m *executionManagerImpl) RangeCompleteTransferTask(request *RangeCompleteTransferTaskRequest) error {
	return m.persistence.RangeCompleteTransferTask(request)
}

// Replication task related methods
func (m *executionManagerImpl) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	return m.persistence.GetReplicationTasks(request)
}
func (m *executionManagerImpl) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	return m.persistence.CompleteReplicationTask(request)
}

// Timer related methods.
func (m *executionManagerImpl) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	return m.persistence.GetTimerIndexTasks(request)
}
func (m *executionManagerImpl) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	return m.persistence.CompleteTimerTask(request)
}
func (m *executionManagerImpl) RangeCompleteTimerTask(request *RangeCompleteTimerTaskRequest) error {
	return m.persistence.RangeCompleteTimerTask(request)
}

func (m *executionManagerImpl) Close() {
	m.persistence.Close()
}
