// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
//

// Code generated by MockGen. DO NOT EDIT.
// Source: task_generator.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package workflow -source task_generator.go -destination task_generator_mock.go
//

// Package workflow is a generated GoMock package.
package workflow

import (
	reflect "reflect"
	time "time"

	history "go.temporal.io/api/history/v1"
	persistence "go.temporal.io/server/api/persistence/v1"
	hsm "go.temporal.io/server/service/history/hsm"
	tasks "go.temporal.io/server/service/history/tasks"
	gomock "go.uber.org/mock/gomock"
)

// MockTaskGenerator is a mock of TaskGenerator interface.
type MockTaskGenerator struct {
	ctrl     *gomock.Controller
	recorder *MockTaskGeneratorMockRecorder
}

// MockTaskGeneratorMockRecorder is the mock recorder for MockTaskGenerator.
type MockTaskGeneratorMockRecorder struct {
	mock *MockTaskGenerator
}

// NewMockTaskGenerator creates a new mock instance.
func NewMockTaskGenerator(ctrl *gomock.Controller) *MockTaskGenerator {
	mock := &MockTaskGenerator{ctrl: ctrl}
	mock.recorder = &MockTaskGeneratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTaskGenerator) EXPECT() *MockTaskGeneratorMockRecorder {
	return m.recorder
}

// GenerateActivityRetryTasks mocks base method.
func (m *MockTaskGenerator) GenerateActivityRetryTasks(activityInfo *persistence.ActivityInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateActivityRetryTasks", activityInfo)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateActivityRetryTasks indicates an expected call of GenerateActivityRetryTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateActivityRetryTasks(activityInfo any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateActivityRetryTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateActivityRetryTasks), activityInfo)
}

// GenerateActivityTasks mocks base method.
func (m *MockTaskGenerator) GenerateActivityTasks(activityScheduledEventID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateActivityTasks", activityScheduledEventID)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateActivityTasks indicates an expected call of GenerateActivityTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateActivityTasks(activityScheduledEventID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateActivityTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateActivityTasks), activityScheduledEventID)
}

// GenerateActivityTimerTasks mocks base method.
func (m *MockTaskGenerator) GenerateActivityTimerTasks() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateActivityTimerTasks")
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateActivityTimerTasks indicates an expected call of GenerateActivityTimerTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateActivityTimerTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateActivityTimerTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateActivityTimerTasks))
}

// GenerateChildWorkflowTasks mocks base method.
func (m *MockTaskGenerator) GenerateChildWorkflowTasks(event *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateChildWorkflowTasks", event)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateChildWorkflowTasks indicates an expected call of GenerateChildWorkflowTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateChildWorkflowTasks(event any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateChildWorkflowTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateChildWorkflowTasks), event)
}

// GenerateDelayedWorkflowTasks mocks base method.
func (m *MockTaskGenerator) GenerateDelayedWorkflowTasks(startEvent *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateDelayedWorkflowTasks", startEvent)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateDelayedWorkflowTasks indicates an expected call of GenerateDelayedWorkflowTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateDelayedWorkflowTasks(startEvent any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateDelayedWorkflowTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateDelayedWorkflowTasks), startEvent)
}

// GenerateDeleteExecutionTask mocks base method.
func (m *MockTaskGenerator) GenerateDeleteExecutionTask() (*tasks.DeleteExecutionTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateDeleteExecutionTask")
	ret0, _ := ret[0].(*tasks.DeleteExecutionTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenerateDeleteExecutionTask indicates an expected call of GenerateDeleteExecutionTask.
func (mr *MockTaskGeneratorMockRecorder) GenerateDeleteExecutionTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateDeleteExecutionTask", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateDeleteExecutionTask))
}

// GenerateDeleteHistoryEventTask mocks base method.
func (m *MockTaskGenerator) GenerateDeleteHistoryEventTask(closeTime time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateDeleteHistoryEventTask", closeTime)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateDeleteHistoryEventTask indicates an expected call of GenerateDeleteHistoryEventTask.
func (mr *MockTaskGeneratorMockRecorder) GenerateDeleteHistoryEventTask(closeTime any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateDeleteHistoryEventTask", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateDeleteHistoryEventTask), closeTime)
}

// GenerateDirtySubStateMachineTasks mocks base method.
func (m *MockTaskGenerator) GenerateDirtySubStateMachineTasks(stateMachineRegistry *hsm.Registry) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateDirtySubStateMachineTasks", stateMachineRegistry)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateDirtySubStateMachineTasks indicates an expected call of GenerateDirtySubStateMachineTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateDirtySubStateMachineTasks(stateMachineRegistry any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateDirtySubStateMachineTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateDirtySubStateMachineTasks), stateMachineRegistry)
}

// GenerateHistoryReplicationTasks mocks base method.
func (m *MockTaskGenerator) GenerateHistoryReplicationTasks(eventBatches [][]*history.HistoryEvent) ([]tasks.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateHistoryReplicationTasks", eventBatches)
	ret0, _ := ret[0].([]tasks.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenerateHistoryReplicationTasks indicates an expected call of GenerateHistoryReplicationTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateHistoryReplicationTasks(eventBatches any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateHistoryReplicationTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateHistoryReplicationTasks), eventBatches)
}

// GenerateMigrationTasks mocks base method.
func (m *MockTaskGenerator) GenerateMigrationTasks() ([]tasks.Task, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateMigrationTasks")
	ret0, _ := ret[0].([]tasks.Task)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GenerateMigrationTasks indicates an expected call of GenerateMigrationTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateMigrationTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateMigrationTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateMigrationTasks))
}

// GenerateRecordWorkflowStartedTasks mocks base method.
func (m *MockTaskGenerator) GenerateRecordWorkflowStartedTasks(startEvent *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateRecordWorkflowStartedTasks", startEvent)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateRecordWorkflowStartedTasks indicates an expected call of GenerateRecordWorkflowStartedTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateRecordWorkflowStartedTasks(startEvent any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateRecordWorkflowStartedTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateRecordWorkflowStartedTasks), startEvent)
}

// GenerateRequestCancelExternalTasks mocks base method.
func (m *MockTaskGenerator) GenerateRequestCancelExternalTasks(event *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateRequestCancelExternalTasks", event)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateRequestCancelExternalTasks indicates an expected call of GenerateRequestCancelExternalTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateRequestCancelExternalTasks(event any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateRequestCancelExternalTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateRequestCancelExternalTasks), event)
}

// GenerateScheduleSpeculativeWorkflowTaskTasks mocks base method.
func (m *MockTaskGenerator) GenerateScheduleSpeculativeWorkflowTaskTasks(workflowTask *WorkflowTaskInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateScheduleSpeculativeWorkflowTaskTasks", workflowTask)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateScheduleSpeculativeWorkflowTaskTasks indicates an expected call of GenerateScheduleSpeculativeWorkflowTaskTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateScheduleSpeculativeWorkflowTaskTasks(workflowTask any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateScheduleSpeculativeWorkflowTaskTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateScheduleSpeculativeWorkflowTaskTasks), workflowTask)
}

// GenerateScheduleWorkflowTaskTasks mocks base method.
func (m *MockTaskGenerator) GenerateScheduleWorkflowTaskTasks(workflowTaskScheduledEventID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateScheduleWorkflowTaskTasks", workflowTaskScheduledEventID)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateScheduleWorkflowTaskTasks indicates an expected call of GenerateScheduleWorkflowTaskTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateScheduleWorkflowTaskTasks(workflowTaskScheduledEventID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateScheduleWorkflowTaskTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateScheduleWorkflowTaskTasks), workflowTaskScheduledEventID)
}

// GenerateSignalExternalTasks mocks base method.
func (m *MockTaskGenerator) GenerateSignalExternalTasks(event *history.HistoryEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateSignalExternalTasks", event)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateSignalExternalTasks indicates an expected call of GenerateSignalExternalTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateSignalExternalTasks(event any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateSignalExternalTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateSignalExternalTasks), event)
}

// GenerateStartWorkflowTaskTasks mocks base method.
func (m *MockTaskGenerator) GenerateStartWorkflowTaskTasks(workflowTaskScheduledEventID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateStartWorkflowTaskTasks", workflowTaskScheduledEventID)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateStartWorkflowTaskTasks indicates an expected call of GenerateStartWorkflowTaskTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateStartWorkflowTaskTasks(workflowTaskScheduledEventID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateStartWorkflowTaskTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateStartWorkflowTaskTasks), workflowTaskScheduledEventID)
}

// GenerateUpsertVisibilityTask mocks base method.
func (m *MockTaskGenerator) GenerateUpsertVisibilityTask() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateUpsertVisibilityTask")
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateUpsertVisibilityTask indicates an expected call of GenerateUpsertVisibilityTask.
func (mr *MockTaskGeneratorMockRecorder) GenerateUpsertVisibilityTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateUpsertVisibilityTask", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateUpsertVisibilityTask))
}

// GenerateUserTimerTasks mocks base method.
func (m *MockTaskGenerator) GenerateUserTimerTasks() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateUserTimerTasks")
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateUserTimerTasks indicates an expected call of GenerateUserTimerTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateUserTimerTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateUserTimerTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateUserTimerTasks))
}

// GenerateWorkflowCloseTasks mocks base method.
func (m *MockTaskGenerator) GenerateWorkflowCloseTasks(closedTime time.Time, deleteAfterClose bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateWorkflowCloseTasks", closedTime, deleteAfterClose)
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateWorkflowCloseTasks indicates an expected call of GenerateWorkflowCloseTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateWorkflowCloseTasks(closedTime, deleteAfterClose any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateWorkflowCloseTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateWorkflowCloseTasks), closedTime, deleteAfterClose)
}

// GenerateWorkflowResetTasks mocks base method.
func (m *MockTaskGenerator) GenerateWorkflowResetTasks() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateWorkflowResetTasks")
	ret0, _ := ret[0].(error)
	return ret0
}

// GenerateWorkflowResetTasks indicates an expected call of GenerateWorkflowResetTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateWorkflowResetTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateWorkflowResetTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateWorkflowResetTasks))
}

// GenerateWorkflowStartTasks mocks base method.
func (m *MockTaskGenerator) GenerateWorkflowStartTasks(startEvent *history.HistoryEvent) (int32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenerateWorkflowStartTasks", startEvent)
	ret0, _ := ret[0].(int32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenerateWorkflowStartTasks indicates an expected call of GenerateWorkflowStartTasks.
func (mr *MockTaskGeneratorMockRecorder) GenerateWorkflowStartTasks(startEvent any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenerateWorkflowStartTasks", reflect.TypeOf((*MockTaskGenerator)(nil).GenerateWorkflowStartTasks), startEvent)
}
