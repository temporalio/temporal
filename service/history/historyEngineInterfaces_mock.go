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
// Source: historyEngineInterfaces.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	repication "go.temporal.io/server/api/replication/v1"
	queues "go.temporal.io/server/service/history/queues"
	tasks "go.temporal.io/server/service/history/tasks"
)

// MockqueueProcessor is a mock of queueProcessor interface.
type MockqueueProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockqueueProcessorMockRecorder
}

// MockqueueProcessorMockRecorder is the mock recorder for MockqueueProcessor.
type MockqueueProcessorMockRecorder struct {
	mock *MockqueueProcessor
}

// NewMockqueueProcessor creates a new mock instance.
func NewMockqueueProcessor(ctrl *gomock.Controller) *MockqueueProcessor {
	mock := &MockqueueProcessor{ctrl: ctrl}
	mock.recorder = &MockqueueProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockqueueProcessor) EXPECT() *MockqueueProcessorMockRecorder {
	return m.recorder
}

// Start mocks base method.
func (m *MockqueueProcessor) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockqueueProcessorMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockqueueProcessor)(nil).Start))
}

// Stop mocks base method.
func (m *MockqueueProcessor) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockqueueProcessorMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockqueueProcessor)(nil).Stop))
}

// notifyNewTask mocks base method.
func (m *MockqueueProcessor) notifyNewTask() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "notifyNewTask")
}

// notifyNewTask indicates an expected call of notifyNewTask.
func (mr *MockqueueProcessorMockRecorder) notifyNewTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "notifyNewTask", reflect.TypeOf((*MockqueueProcessor)(nil).notifyNewTask))
}

// MockReplicatorQueueProcessor is a mock of ReplicatorQueueProcessor interface.
type MockReplicatorQueueProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockReplicatorQueueProcessorMockRecorder
}

// MockReplicatorQueueProcessorMockRecorder is the mock recorder for MockReplicatorQueueProcessor.
type MockReplicatorQueueProcessorMockRecorder struct {
	mock *MockReplicatorQueueProcessor
}

// NewMockReplicatorQueueProcessor creates a new mock instance.
func NewMockReplicatorQueueProcessor(ctrl *gomock.Controller) *MockReplicatorQueueProcessor {
	mock := &MockReplicatorQueueProcessor{ctrl: ctrl}
	mock.recorder = &MockReplicatorQueueProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReplicatorQueueProcessor) EXPECT() *MockReplicatorQueueProcessorMockRecorder {
	return m.recorder
}

// Start mocks base method.
func (m *MockReplicatorQueueProcessor) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockReplicatorQueueProcessorMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockReplicatorQueueProcessor)(nil).Start))
}

// Stop mocks base method.
func (m *MockReplicatorQueueProcessor) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockReplicatorQueueProcessorMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockReplicatorQueueProcessor)(nil).Stop))
}

// getTask mocks base method.
func (m *MockReplicatorQueueProcessor) getTask(ctx context.Context, taskInfo *repication.ReplicationTaskInfo) (*repication.ReplicationTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getTask", ctx, taskInfo)
	ret0, _ := ret[0].(*repication.ReplicationTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// getTask indicates an expected call of getTask.
func (mr *MockReplicatorQueueProcessorMockRecorder) getTask(ctx, taskInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getTask", reflect.TypeOf((*MockReplicatorQueueProcessor)(nil).getTask), ctx, taskInfo)
}

// getTasks mocks base method.
func (m *MockReplicatorQueueProcessor) getTasks(ctx context.Context, pollingCluster string, lastReadTaskID int64) (*repication.ReplicationMessages, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getTasks", ctx, pollingCluster, lastReadTaskID)
	ret0, _ := ret[0].(*repication.ReplicationMessages)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// getTasks indicates an expected call of getTasks.
func (mr *MockReplicatorQueueProcessorMockRecorder) getTasks(ctx, pollingCluster, lastReadTaskID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getTasks", reflect.TypeOf((*MockReplicatorQueueProcessor)(nil).getTasks), ctx, pollingCluster, lastReadTaskID)
}

// notifyNewTask mocks base method.
func (m *MockReplicatorQueueProcessor) notifyNewTask() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "notifyNewTask")
}

// notifyNewTask indicates an expected call of notifyNewTask.
func (mr *MockReplicatorQueueProcessorMockRecorder) notifyNewTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "notifyNewTask", reflect.TypeOf((*MockReplicatorQueueProcessor)(nil).notifyNewTask))
}

// MockqueueAckMgr is a mock of queueAckMgr interface.
type MockqueueAckMgr struct {
	ctrl     *gomock.Controller
	recorder *MockqueueAckMgrMockRecorder
}

// MockqueueAckMgrMockRecorder is the mock recorder for MockqueueAckMgr.
type MockqueueAckMgrMockRecorder struct {
	mock *MockqueueAckMgr
}

// NewMockqueueAckMgr creates a new mock instance.
func NewMockqueueAckMgr(ctrl *gomock.Controller) *MockqueueAckMgr {
	mock := &MockqueueAckMgr{ctrl: ctrl}
	mock.recorder = &MockqueueAckMgrMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockqueueAckMgr) EXPECT() *MockqueueAckMgrMockRecorder {
	return m.recorder
}

// getFinishedChan mocks base method.
func (m *MockqueueAckMgr) getFinishedChan() <-chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getFinishedChan")
	ret0, _ := ret[0].(<-chan struct{})
	return ret0
}

// getFinishedChan indicates an expected call of getFinishedChan.
func (mr *MockqueueAckMgrMockRecorder) getFinishedChan() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getFinishedChan", reflect.TypeOf((*MockqueueAckMgr)(nil).getFinishedChan))
}

// getQueueAckLevel mocks base method.
func (m *MockqueueAckMgr) getQueueAckLevel() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getQueueAckLevel")
	ret0, _ := ret[0].(int64)
	return ret0
}

// getQueueAckLevel indicates an expected call of getQueueAckLevel.
func (mr *MockqueueAckMgrMockRecorder) getQueueAckLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getQueueAckLevel", reflect.TypeOf((*MockqueueAckMgr)(nil).getQueueAckLevel))
}

// getQueueReadLevel mocks base method.
func (m *MockqueueAckMgr) getQueueReadLevel() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getQueueReadLevel")
	ret0, _ := ret[0].(int64)
	return ret0
}

// getQueueReadLevel indicates an expected call of getQueueReadLevel.
func (mr *MockqueueAckMgrMockRecorder) getQueueReadLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getQueueReadLevel", reflect.TypeOf((*MockqueueAckMgr)(nil).getQueueReadLevel))
}

// readQueueTasks mocks base method.
func (m *MockqueueAckMgr) readQueueTasks() ([]queues.Executable, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "readQueueTasks")
	ret0, _ := ret[0].([]queues.Executable)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// readQueueTasks indicates an expected call of readQueueTasks.
func (mr *MockqueueAckMgrMockRecorder) readQueueTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "readQueueTasks", reflect.TypeOf((*MockqueueAckMgr)(nil).readQueueTasks))
}

// updateQueueAckLevel mocks base method.
func (m *MockqueueAckMgr) updateQueueAckLevel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateQueueAckLevel")
	ret0, _ := ret[0].(error)
	return ret0
}

// updateQueueAckLevel indicates an expected call of updateQueueAckLevel.
func (mr *MockqueueAckMgrMockRecorder) updateQueueAckLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateQueueAckLevel", reflect.TypeOf((*MockqueueAckMgr)(nil).updateQueueAckLevel))
}

// Mockprocessor is a mock of processor interface.
type Mockprocessor struct {
	ctrl     *gomock.Controller
	recorder *MockprocessorMockRecorder
}

// MockprocessorMockRecorder is the mock recorder for Mockprocessor.
type MockprocessorMockRecorder struct {
	mock *Mockprocessor
}

// NewMockprocessor creates a new mock instance.
func NewMockprocessor(ctrl *gomock.Controller) *Mockprocessor {
	mock := &Mockprocessor{ctrl: ctrl}
	mock.recorder = &MockprocessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockprocessor) EXPECT() *MockprocessorMockRecorder {
	return m.recorder
}

// queueShutdown mocks base method.
func (m *Mockprocessor) queueShutdown() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "queueShutdown")
	ret0, _ := ret[0].(error)
	return ret0
}

// queueShutdown indicates an expected call of queueShutdown.
func (mr *MockprocessorMockRecorder) queueShutdown() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "queueShutdown", reflect.TypeOf((*Mockprocessor)(nil).queueShutdown))
}

// readTasks mocks base method.
func (m *Mockprocessor) readTasks(readLevel int64) ([]tasks.Task, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "readTasks", readLevel)
	ret0, _ := ret[0].([]tasks.Task)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// readTasks indicates an expected call of readTasks.
func (mr *MockprocessorMockRecorder) readTasks(readLevel interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "readTasks", reflect.TypeOf((*Mockprocessor)(nil).readTasks), readLevel)
}

// updateAckLevel mocks base method.
func (m *Mockprocessor) updateAckLevel(taskID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateAckLevel", taskID)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateAckLevel indicates an expected call of updateAckLevel.
func (mr *MockprocessorMockRecorder) updateAckLevel(taskID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateAckLevel", reflect.TypeOf((*Mockprocessor)(nil).updateAckLevel), taskID)
}

// MocktimerQueueAckMgr is a mock of timerQueueAckMgr interface.
type MocktimerQueueAckMgr struct {
	ctrl     *gomock.Controller
	recorder *MocktimerQueueAckMgrMockRecorder
}

// MocktimerQueueAckMgrMockRecorder is the mock recorder for MocktimerQueueAckMgr.
type MocktimerQueueAckMgrMockRecorder struct {
	mock *MocktimerQueueAckMgr
}

// NewMocktimerQueueAckMgr creates a new mock instance.
func NewMocktimerQueueAckMgr(ctrl *gomock.Controller) *MocktimerQueueAckMgr {
	mock := &MocktimerQueueAckMgr{ctrl: ctrl}
	mock.recorder = &MocktimerQueueAckMgrMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocktimerQueueAckMgr) EXPECT() *MocktimerQueueAckMgrMockRecorder {
	return m.recorder
}

// getAckLevel mocks base method.
func (m *MocktimerQueueAckMgr) getAckLevel() tasks.Key {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getAckLevel")
	ret0, _ := ret[0].(tasks.Key)
	return ret0
}

// getAckLevel indicates an expected call of getAckLevel.
func (mr *MocktimerQueueAckMgrMockRecorder) getAckLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getAckLevel", reflect.TypeOf((*MocktimerQueueAckMgr)(nil).getAckLevel))
}

// getFinishedChan mocks base method.
func (m *MocktimerQueueAckMgr) getFinishedChan() <-chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getFinishedChan")
	ret0, _ := ret[0].(<-chan struct{})
	return ret0
}

// getFinishedChan indicates an expected call of getFinishedChan.
func (mr *MocktimerQueueAckMgrMockRecorder) getFinishedChan() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getFinishedChan", reflect.TypeOf((*MocktimerQueueAckMgr)(nil).getFinishedChan))
}

// getReadLevel mocks base method.
func (m *MocktimerQueueAckMgr) getReadLevel() tasks.Key {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getReadLevel")
	ret0, _ := ret[0].(tasks.Key)
	return ret0
}

// getReadLevel indicates an expected call of getReadLevel.
func (mr *MocktimerQueueAckMgrMockRecorder) getReadLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getReadLevel", reflect.TypeOf((*MocktimerQueueAckMgr)(nil).getReadLevel))
}

// readTimerTasks mocks base method.
func (m *MocktimerQueueAckMgr) readTimerTasks() ([]queues.Executable, *time.Time, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "readTimerTasks")
	ret0, _ := ret[0].([]queues.Executable)
	ret1, _ := ret[1].(*time.Time)
	ret2, _ := ret[2].(bool)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// readTimerTasks indicates an expected call of readTimerTasks.
func (mr *MocktimerQueueAckMgrMockRecorder) readTimerTasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "readTimerTasks", reflect.TypeOf((*MocktimerQueueAckMgr)(nil).readTimerTasks))
}

// updateAckLevel mocks base method.
func (m *MocktimerQueueAckMgr) updateAckLevel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateAckLevel")
	ret0, _ := ret[0].(error)
	return ret0
}

// updateAckLevel indicates an expected call of updateAckLevel.
func (mr *MocktimerQueueAckMgrMockRecorder) updateAckLevel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateAckLevel", reflect.TypeOf((*MocktimerQueueAckMgr)(nil).updateAckLevel))
}
