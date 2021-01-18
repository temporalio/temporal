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

// Code generated by MockGen. DO NOT EDIT.
// Source: mutableStateWorkflowTaskManager.go

// Package history is a generated GoMock package.
package history

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	enums "go.temporal.io/api/enums/v1"
	failure "go.temporal.io/api/failure/v1"
	history "go.temporal.io/api/history/v1"
	taskqueue "go.temporal.io/api/taskqueue/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
)

// MockmutableStateWorkflowTaskManager is a mock of mutableStateWorkflowTaskManager interface.
type MockmutableStateWorkflowTaskManager struct {
	ctrl     *gomock.Controller
	recorder *MockmutableStateWorkflowTaskManagerMockRecorder
}

// MockmutableStateWorkflowTaskManagerMockRecorder is the mock recorder for MockmutableStateWorkflowTaskManager.
type MockmutableStateWorkflowTaskManagerMockRecorder struct {
	mock *MockmutableStateWorkflowTaskManager
}

// NewMockmutableStateWorkflowTaskManager creates a new mock instance.
func NewMockmutableStateWorkflowTaskManager(ctrl *gomock.Controller) *MockmutableStateWorkflowTaskManager {
	mock := &MockmutableStateWorkflowTaskManager{ctrl: ctrl}
	mock.recorder = &MockmutableStateWorkflowTaskManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockmutableStateWorkflowTaskManager) EXPECT() *MockmutableStateWorkflowTaskManagerMockRecorder {
	return m.recorder
}

// AddFirstWorkflowTaskScheduled mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddFirstWorkflowTaskScheduled(startEvent *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddFirstWorkflowTaskScheduled", startEvent)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddFirstWorkflowTaskScheduled indicates an expected call of AddFirstWorkflowTaskScheduled.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddFirstWorkflowTaskScheduled(startEvent interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddFirstWorkflowTaskScheduled", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddFirstWorkflowTaskScheduled), startEvent)
}

// AddWorkflowTaskCompletedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskCompletedEvent(scheduleEventID, startedEventID int64, request *workflowservice.RespondWorkflowTaskCompletedRequest, maxResetPoints int) (*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskCompletedEvent", scheduleEventID, startedEventID, request, maxResetPoints)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskCompletedEvent indicates an expected call of AddWorkflowTaskCompletedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskCompletedEvent(scheduleEventID, startedEventID, request, maxResetPoints interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskCompletedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskCompletedEvent), scheduleEventID, startedEventID, request, maxResetPoints)
}

// AddWorkflowTaskFailedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskFailedEvent(scheduleEventID, startedEventID int64, cause enums.WorkflowTaskFailedCause, failure *failure.Failure, identity, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskFailedEvent", scheduleEventID, startedEventID, cause, failure, identity, binChecksum, baseRunID, newRunID, forkEventVersion)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskFailedEvent indicates an expected call of AddWorkflowTaskFailedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskFailedEvent(scheduleEventID, startedEventID, cause, failure, identity, binChecksum, baseRunID, newRunID, forkEventVersion interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskFailedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskFailedEvent), scheduleEventID, startedEventID, cause, failure, identity, binChecksum, baseRunID, newRunID, forkEventVersion)
}

// AddWorkflowTaskScheduleToStartTimeoutEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID int64) (*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskScheduleToStartTimeoutEvent", scheduleEventID)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskScheduleToStartTimeoutEvent indicates an expected call of AddWorkflowTaskScheduleToStartTimeoutEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskScheduleToStartTimeoutEvent(scheduleEventID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskScheduleToStartTimeoutEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskScheduleToStartTimeoutEvent), scheduleEventID)
}

// AddWorkflowTaskScheduledEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool) (*workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskScheduledEvent", bypassTaskGeneration)
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskScheduledEvent indicates an expected call of AddWorkflowTaskScheduledEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskScheduledEvent(bypassTaskGeneration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskScheduledEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskScheduledEvent), bypassTaskGeneration)
}

// AddWorkflowTaskScheduledEventAsHeartbeat mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp *time.Time) (*workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskScheduledEventAsHeartbeat", bypassTaskGeneration, originalScheduledTimestamp)
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskScheduledEventAsHeartbeat indicates an expected call of AddWorkflowTaskScheduledEventAsHeartbeat.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration, originalScheduledTimestamp interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskScheduledEventAsHeartbeat", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskScheduledEventAsHeartbeat), bypassTaskGeneration, originalScheduledTimestamp)
}

// AddWorkflowTaskStartedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskStartedEvent(scheduleEventID int64, requestID string, request *workflowservice.PollWorkflowTaskQueueRequest) (*history.HistoryEvent, *workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskStartedEvent", scheduleEventID, requestID, request)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(*workflowTaskInfo)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// AddWorkflowTaskStartedEvent indicates an expected call of AddWorkflowTaskStartedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskStartedEvent(scheduleEventID, requestID, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskStartedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskStartedEvent), scheduleEventID, requestID, request)
}

// AddWorkflowTaskTimedOutEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID int64) (*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTaskTimedOutEvent", scheduleEventID, startedEventID)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTaskTimedOutEvent indicates an expected call of AddWorkflowTaskTimedOutEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) AddWorkflowTaskTimedOutEvent(scheduleEventID, startedEventID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTaskTimedOutEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).AddWorkflowTaskTimedOutEvent), scheduleEventID, startedEventID)
}

// CreateTransientWorkflowTaskEvents mocks base method.
func (m *MockmutableStateWorkflowTaskManager) CreateTransientWorkflowTaskEvents(workflowTask *workflowTaskInfo, identity string) (*history.HistoryEvent, *history.HistoryEvent) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTransientWorkflowTaskEvents", workflowTask, identity)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(*history.HistoryEvent)
	return ret0, ret1
}

// CreateTransientWorkflowTaskEvents indicates an expected call of CreateTransientWorkflowTaskEvents.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) CreateTransientWorkflowTaskEvents(workflowTask, identity interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTransientWorkflowTaskEvents", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).CreateTransientWorkflowTaskEvents), workflowTask, identity)
}

// DeleteWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) DeleteWorkflowTask() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteWorkflowTask")
}

// DeleteWorkflowTask indicates an expected call of DeleteWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) DeleteWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).DeleteWorkflowTask))
}

// FailWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) FailWorkflowTask(incrementAttempt bool) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "FailWorkflowTask", incrementAttempt)
}

// FailWorkflowTask indicates an expected call of FailWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) FailWorkflowTask(incrementAttempt interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FailWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).FailWorkflowTask), incrementAttempt)
}

// GetInFlightWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) GetInFlightWorkflowTask() (*workflowTaskInfo, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetInFlightWorkflowTask")
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetInFlightWorkflowTask indicates an expected call of GetInFlightWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) GetInFlightWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetInFlightWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).GetInFlightWorkflowTask))
}

// GetPendingWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) GetPendingWorkflowTask() (*workflowTaskInfo, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPendingWorkflowTask")
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetPendingWorkflowTask indicates an expected call of GetPendingWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) GetPendingWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPendingWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).GetPendingWorkflowTask))
}

// GetWorkflowTaskInfo mocks base method.
func (m *MockmutableStateWorkflowTaskManager) GetWorkflowTaskInfo(scheduleEventID int64) (*workflowTaskInfo, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowTaskInfo", scheduleEventID)
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// GetWorkflowTaskInfo indicates an expected call of GetWorkflowTaskInfo.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) GetWorkflowTaskInfo(scheduleEventID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowTaskInfo", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).GetWorkflowTaskInfo), scheduleEventID)
}

// HasInFlightWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) HasInFlightWorkflowTask() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasInFlightWorkflowTask")
	ret0, _ := ret[0].(bool)
	return ret0
}

// HasInFlightWorkflowTask indicates an expected call of HasInFlightWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) HasInFlightWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasInFlightWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).HasInFlightWorkflowTask))
}

// HasPendingWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) HasPendingWorkflowTask() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasPendingWorkflowTask")
	ret0, _ := ret[0].(bool)
	return ret0
}

// HasPendingWorkflowTask indicates an expected call of HasPendingWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) HasPendingWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasPendingWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).HasPendingWorkflowTask))
}

// HasProcessedOrPendingWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) HasProcessedOrPendingWorkflowTask() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasProcessedOrPendingWorkflowTask")
	ret0, _ := ret[0].(bool)
	return ret0
}

// HasProcessedOrPendingWorkflowTask indicates an expected call of HasProcessedOrPendingWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) HasProcessedOrPendingWorkflowTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasProcessedOrPendingWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).HasProcessedOrPendingWorkflowTask))
}

// ReplicateTransientWorkflowTaskScheduled mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateTransientWorkflowTaskScheduled() (*workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateTransientWorkflowTaskScheduled")
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReplicateTransientWorkflowTaskScheduled indicates an expected call of ReplicateTransientWorkflowTaskScheduled.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateTransientWorkflowTaskScheduled() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateTransientWorkflowTaskScheduled", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateTransientWorkflowTaskScheduled))
}

// ReplicateWorkflowTaskCompletedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateWorkflowTaskCompletedEvent(event *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateWorkflowTaskCompletedEvent", event)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplicateWorkflowTaskCompletedEvent indicates an expected call of ReplicateWorkflowTaskCompletedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateWorkflowTaskCompletedEvent(event interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateWorkflowTaskCompletedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateWorkflowTaskCompletedEvent), event)
}

// ReplicateWorkflowTaskFailedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateWorkflowTaskFailedEvent() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateWorkflowTaskFailedEvent")
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplicateWorkflowTaskFailedEvent indicates an expected call of ReplicateWorkflowTaskFailedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateWorkflowTaskFailedEvent() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateWorkflowTaskFailedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateWorkflowTaskFailedEvent))
}

// ReplicateWorkflowTaskScheduledEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateWorkflowTaskScheduledEvent(version, scheduleID int64, taskQueue *taskqueue.TaskQueue, startToCloseTimeoutSeconds, attempt int32, scheduleTimestamp, originalScheduledTimestamp *time.Time) (*workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateWorkflowTaskScheduledEvent", version, scheduleID, taskQueue, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp)
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReplicateWorkflowTaskScheduledEvent indicates an expected call of ReplicateWorkflowTaskScheduledEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateWorkflowTaskScheduledEvent(version, scheduleID, taskQueue, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateWorkflowTaskScheduledEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateWorkflowTaskScheduledEvent), version, scheduleID, taskQueue, startToCloseTimeoutSeconds, attempt, scheduleTimestamp, originalScheduledTimestamp)
}

// ReplicateWorkflowTaskStartedEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateWorkflowTaskStartedEvent(workflowTask *workflowTaskInfo, version, scheduleID, startedID int64, requestID string, timestamp time.Time) (*workflowTaskInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateWorkflowTaskStartedEvent", workflowTask, version, scheduleID, startedID, requestID, timestamp)
	ret0, _ := ret[0].(*workflowTaskInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReplicateWorkflowTaskStartedEvent indicates an expected call of ReplicateWorkflowTaskStartedEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateWorkflowTaskStartedEvent(workflowTask, version, scheduleID, startedID, requestID, timestamp interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateWorkflowTaskStartedEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateWorkflowTaskStartedEvent), workflowTask, version, scheduleID, startedID, requestID, timestamp)
}

// ReplicateWorkflowTaskTimedOutEvent mocks base method.
func (m *MockmutableStateWorkflowTaskManager) ReplicateWorkflowTaskTimedOutEvent(timeoutType enums.TimeoutType) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplicateWorkflowTaskTimedOutEvent", timeoutType)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReplicateWorkflowTaskTimedOutEvent indicates an expected call of ReplicateWorkflowTaskTimedOutEvent.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) ReplicateWorkflowTaskTimedOutEvent(timeoutType interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplicateWorkflowTaskTimedOutEvent", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).ReplicateWorkflowTaskTimedOutEvent), timeoutType)
}

// UpdateWorkflowTask mocks base method.
func (m *MockmutableStateWorkflowTaskManager) UpdateWorkflowTask(workflowTask *workflowTaskInfo) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UpdateWorkflowTask", workflowTask)
}

// UpdateWorkflowTask indicates an expected call of UpdateWorkflowTask.
func (mr *MockmutableStateWorkflowTaskManagerMockRecorder) UpdateWorkflowTask(workflowTask interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkflowTask", reflect.TypeOf((*MockmutableStateWorkflowTaskManager)(nil).UpdateWorkflowTask), workflowTask)
}
