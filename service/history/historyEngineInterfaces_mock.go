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
// Source: queueTask.go

// Package history is a generated GoMock package.
package history

import (
	reflect "reflect"

	"github.com/gogo/protobuf/types"
	gomock "github.com/golang/mock/gomock"

	task "github.com/temporalio/temporal/common/task"

	enumsgenpb "github.com/temporalio/temporal/.gen/proto/enums/v1"
)

// MockqueueTaskInfo is a mock of queueTaskInfo interface
type MockqueueTaskInfo struct {
	ctrl     *gomock.Controller
	recorder *MockqueueTaskInfoMockRecorder
}

// MockqueueTaskInfoMockRecorder is the mock recorder for MockqueueTaskInfo
type MockqueueTaskInfoMockRecorder struct {
	mock *MockqueueTaskInfo
}

// NewMockqueueTaskInfo creates a new mock instance
func NewMockqueueTaskInfo(ctrl *gomock.Controller) *MockqueueTaskInfo {
	mock := &MockqueueTaskInfo{ctrl: ctrl}
	mock.recorder = &MockqueueTaskInfoMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockqueueTaskInfo) EXPECT() *MockqueueTaskInfoMockRecorder {
	return m.recorder
}

// GetVersion mocks base method
func (m *MockqueueTaskInfo) GetVersion() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVersion")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetVersion indicates an expected call of GetVersion
func (mr *MockqueueTaskInfoMockRecorder) GetVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVersion", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetVersion))
}

// GetTaskId mocks base method
func (m *MockqueueTaskInfo) GetTaskId() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskId")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetTaskId indicates an expected call of GetTaskId
func (mr *MockqueueTaskInfoMockRecorder) GetTaskID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskId", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetTaskId))
}

// GetTaskType mocks base method
func (m *MockqueueTaskInfo) GetTaskType() enumsgenpb.TaskType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskType")
	ret0, _ := ret[0].(enumsgenpb.TaskType)
	return ret0
}

// GetTaskType indicates an expected call of GetTaskType
func (mr *MockqueueTaskInfoMockRecorder) GetTaskType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskType", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetTaskType))
}

// GetVisibilityTimestamp mocks base method
func (m *MockqueueTaskInfo) GetVisibilityTimestamp() *types.Timestamp {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVisibilityTimestamp")
	ret0, _ := ret[0].(*types.Timestamp)
	return ret0
}

// GetVisibilityTimestamp indicates an expected call of GetVisibilityTimestamp
func (mr *MockqueueTaskInfoMockRecorder) GetVisibilityTimestamp() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVisibilityTimestamp", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetVisibilityTimestamp))
}

// GetWorkflowId mocks base method
func (m *MockqueueTaskInfo) GetWorkflowId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkflowId indicates an expected call of GetWorkflowId
func (mr *MockqueueTaskInfoMockRecorder) GetWorkflowID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowId", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetWorkflowId))
}

// GetRunId mocks base method
func (m *MockqueueTaskInfo) GetRunId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRunId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetRunId indicates an expected call of GetRunId
func (mr *MockqueueTaskInfoMockRecorder) GetRunID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRunId", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetRunId))
}

// GetNamespaceId mocks base method
func (m *MockqueueTaskInfo) GetNamespaceId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetNamespaceId indicates an expected call of GetNamespaceId
func (mr *MockqueueTaskInfoMockRecorder) GetNamespaceID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceId", reflect.TypeOf((*MockqueueTaskInfo)(nil).GetNamespaceId))
}

// MockqueueTask is a mock of queueTask interface
type MockqueueTask struct {
	ctrl     *gomock.Controller
	recorder *MockqueueTaskMockRecorder
}

// MockqueueTaskMockRecorder is the mock recorder for MockqueueTask
type MockqueueTaskMockRecorder struct {
	mock *MockqueueTask
}

// NewMockqueueTask creates a new mock instance
func NewMockqueueTask(ctrl *gomock.Controller) *MockqueueTask {
	mock := &MockqueueTask{ctrl: ctrl}
	mock.recorder = &MockqueueTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockqueueTask) EXPECT() *MockqueueTaskMockRecorder {
	return m.recorder
}

// Execute mocks base method
func (m *MockqueueTask) Execute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute
func (mr *MockqueueTaskMockRecorder) Execute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockqueueTask)(nil).Execute))
}

// HandleErr mocks base method
func (m *MockqueueTask) HandleErr(err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleErr", err)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleErr indicates an expected call of HandleErr
func (mr *MockqueueTaskMockRecorder) HandleErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleErr", reflect.TypeOf((*MockqueueTask)(nil).HandleErr), err)
}

// RetryErr mocks base method
func (m *MockqueueTask) RetryErr(err error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryErr", err)
	ret0, _ := ret[0].(bool)
	return ret0
}

// RetryErr indicates an expected call of RetryErr
func (mr *MockqueueTaskMockRecorder) RetryErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryErr", reflect.TypeOf((*MockqueueTask)(nil).RetryErr), err)
}

// Ack mocks base method
func (m *MockqueueTask) Ack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Ack")
}

// Ack indicates an expected call of Ack
func (mr *MockqueueTaskMockRecorder) Ack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockqueueTask)(nil).Ack))
}

// Nack mocks base method
func (m *MockqueueTask) Nack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Nack")
}

// Nack indicates an expected call of Nack
func (mr *MockqueueTaskMockRecorder) Nack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nack", reflect.TypeOf((*MockqueueTask)(nil).Nack))
}

// State mocks base method
func (m *MockqueueTask) State() task.State {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "State")
	ret0, _ := ret[0].(task.State)
	return ret0
}

// State indicates an expected call of State
func (mr *MockqueueTaskMockRecorder) State() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "State", reflect.TypeOf((*MockqueueTask)(nil).State))
}

// Priority mocks base method
func (m *MockqueueTask) Priority() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Priority")
	ret0, _ := ret[0].(int)
	return ret0
}

// Priority indicates an expected call of Priority
func (mr *MockqueueTaskMockRecorder) Priority() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Priority", reflect.TypeOf((*MockqueueTask)(nil).Priority))
}

// SetPriority mocks base method
func (m *MockqueueTask) SetPriority(arg0 int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetPriority", arg0)
}

// SetPriority indicates an expected call of SetPriority
func (mr *MockqueueTaskMockRecorder) SetPriority(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetPriority", reflect.TypeOf((*MockqueueTask)(nil).SetPriority), arg0)
}

// GetVersion mocks base method
func (m *MockqueueTask) GetVersion() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVersion")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetVersion indicates an expected call of GetVersion
func (mr *MockqueueTaskMockRecorder) GetVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVersion", reflect.TypeOf((*MockqueueTask)(nil).GetVersion))
}

// GetTaskId mocks base method
func (m *MockqueueTask) GetTaskId() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskId")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetTaskId indicates an expected call of GetTaskId
func (mr *MockqueueTaskMockRecorder) GetTaskID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskId", reflect.TypeOf((*MockqueueTask)(nil).GetTaskId))
}

// GetTaskType mocks base method
func (m *MockqueueTask) GetTaskType() enumsgenpb.TaskType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskType")
	ret0, _ := ret[0].(enumsgenpb.TaskType)
	return ret0
}

// GetTaskType indicates an expected call of GetTaskType
func (mr *MockqueueTaskMockRecorder) GetTaskType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskType", reflect.TypeOf((*MockqueueTask)(nil).GetTaskType))
}

// GetVisibilityTimestamp mocks base method
func (m *MockqueueTask) GetVisibilityTimestamp() *types.Timestamp {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVisibilityTimestamp")
	ret0, _ := ret[0].(*types.Timestamp)
	return ret0
}

// GetVisibilityTimestamp indicates an expected call of GetVisibilityTimestamp
func (mr *MockqueueTaskMockRecorder) GetVisibilityTimestamp() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVisibilityTimestamp", reflect.TypeOf((*MockqueueTask)(nil).GetVisibilityTimestamp))
}

// GetWorkflowId mocks base method
func (m *MockqueueTask) GetWorkflowId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkflowId indicates an expected call of GetWorkflowId
func (mr *MockqueueTaskMockRecorder) GetWorkflowID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowId", reflect.TypeOf((*MockqueueTask)(nil).GetWorkflowId))
}

// GetRunId mocks base method
func (m *MockqueueTask) GetRunId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRunId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetRunId indicates an expected call of GetRunId
func (mr *MockqueueTaskMockRecorder) GetRunID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRunId", reflect.TypeOf((*MockqueueTask)(nil).GetRunId))
}

// GetNamespaceId mocks base method
func (m *MockqueueTask) GetNamespaceId() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceId")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetNamespaceId indicates an expected call of GetNamespaceId
func (mr *MockqueueTaskMockRecorder) GetNamespaceID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceId", reflect.TypeOf((*MockqueueTask)(nil).GetNamespaceId))
}

// GetQueueType mocks base method
func (m *MockqueueTask) GetQueueType() queueType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetQueueType")
	ret0, _ := ret[0].(queueType)
	return ret0
}

// GetQueueType indicates an expected call of GetQueueType
func (mr *MockqueueTaskMockRecorder) GetQueueType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetQueueType", reflect.TypeOf((*MockqueueTask)(nil).GetQueueType))
}

// GetShard mocks base method
func (m *MockqueueTask) GetShard() ShardContext {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShard")
	ret0, _ := ret[0].(ShardContext)
	return ret0
}

// GetShard indicates an expected call of GetShard
func (mr *MockqueueTaskMockRecorder) GetShard() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShard", reflect.TypeOf((*MockqueueTask)(nil).GetShard))
}

// MockqueueTaskExecutor is a mock of queueTaskExecutor interface
type MockqueueTaskExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockqueueTaskExecutorMockRecorder
}

// MockqueueTaskExecutorMockRecorder is the mock recorder for MockqueueTaskExecutor
type MockqueueTaskExecutorMockRecorder struct {
	mock *MockqueueTaskExecutor
}

// NewMockqueueTaskExecutor creates a new mock instance
func NewMockqueueTaskExecutor(ctrl *gomock.Controller) *MockqueueTaskExecutor {
	mock := &MockqueueTaskExecutor{ctrl: ctrl}
	mock.recorder = &MockqueueTaskExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockqueueTaskExecutor) EXPECT() *MockqueueTaskExecutorMockRecorder {
	return m.recorder
}

// execute mocks base method
func (m *MockqueueTaskExecutor) execute(taskInfo queueTaskInfo, shouldProcessTask bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "execute", taskInfo, shouldProcessTask)
	ret0, _ := ret[0].(error)
	return ret0
}

// execute indicates an expected call of execute
func (mr *MockqueueTaskExecutorMockRecorder) execute(taskInfo, shouldProcessTask interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "execute", reflect.TypeOf((*MockqueueTaskExecutor)(nil).execute), taskInfo, shouldProcessTask)
}

// MockqueueTaskProcessor is a mock of queueTaskProcessor interface
type MockqueueTaskProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockqueueTaskProcessorMockRecorder
}

// MockqueueTaskProcessorMockRecorder is the mock recorder for MockqueueTaskProcessor
type MockqueueTaskProcessorMockRecorder struct {
	mock *MockqueueTaskProcessor
}

// NewMockqueueTaskProcessor creates a new mock instance
func NewMockqueueTaskProcessor(ctrl *gomock.Controller) *MockqueueTaskProcessor {
	mock := &MockqueueTaskProcessor{ctrl: ctrl}
	mock.recorder = &MockqueueTaskProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockqueueTaskProcessor) EXPECT() *MockqueueTaskProcessorMockRecorder {
	return m.recorder
}

// Start mocks base method
func (m *MockqueueTaskProcessor) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start
func (mr *MockqueueTaskProcessorMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockqueueTaskProcessor)(nil).Start))
}

// Stop mocks base method
func (m *MockqueueTaskProcessor) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop
func (mr *MockqueueTaskProcessorMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockqueueTaskProcessor)(nil).Stop))
}

// StopShardProcessor mocks base method
func (m *MockqueueTaskProcessor) StopShardProcessor(arg0 ShardContext) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StopShardProcessor", arg0)
}

// StopShardProcessor indicates an expected call of StopShardProcessor
func (mr *MockqueueTaskProcessorMockRecorder) StopShardProcessor(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StopShardProcessor", reflect.TypeOf((*MockqueueTaskProcessor)(nil).StopShardProcessor), arg0)
}

// Submit mocks base method
func (m *MockqueueTaskProcessor) Submit(arg0 queueTask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Submit", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Submit indicates an expected call of Submit
func (mr *MockqueueTaskProcessorMockRecorder) Submit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockqueueTaskProcessor)(nil).Submit), arg0)
}

// TrySubmit mocks base method
func (m *MockqueueTaskProcessor) TrySubmit(arg0 queueTask) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TrySubmit", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TrySubmit indicates an expected call of TrySubmit
func (mr *MockqueueTaskProcessorMockRecorder) TrySubmit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrySubmit", reflect.TypeOf((*MockqueueTaskProcessor)(nil).TrySubmit), arg0)
}
