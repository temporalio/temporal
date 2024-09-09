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
// Source: ack_manager.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package replication -source ack_manager.go -destination ack_manager_mock.go
//

// Package replication is a generated GoMock package.
package replication

import (
	context "context"
	reflect "reflect"
	time "time"

	repication "go.temporal.io/server/api/replication/v1"
	collection "go.temporal.io/server/common/collection"
	tasks "go.temporal.io/server/service/history/tasks"
	gomock "go.uber.org/mock/gomock"
)

// MockAckManager is a mock of AckManager interface.
type MockAckManager struct {
	ctrl     *gomock.Controller
	recorder *MockAckManagerMockRecorder
}

// MockAckManagerMockRecorder is the mock recorder for MockAckManager.
type MockAckManagerMockRecorder struct {
	mock *MockAckManager
}

// NewMockAckManager creates a new mock instance.
func NewMockAckManager(ctrl *gomock.Controller) *MockAckManager {
	mock := &MockAckManager{ctrl: ctrl}
	mock.recorder = &MockAckManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAckManager) EXPECT() *MockAckManagerMockRecorder {
	return m.recorder
}

// ConvertTask mocks base method.
func (m *MockAckManager) ConvertTask(ctx context.Context, task tasks.Task) (*repication.ReplicationTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConvertTask", ctx, task)
	ret0, _ := ret[0].(*repication.ReplicationTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConvertTask indicates an expected call of ConvertTask.
func (mr *MockAckManagerMockRecorder) ConvertTask(ctx, task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConvertTask", reflect.TypeOf((*MockAckManager)(nil).ConvertTask), ctx, task)
}

// ConvertTaskByCluster mocks base method.
func (m *MockAckManager) ConvertTaskByCluster(ctx context.Context, task tasks.Task, targetClusterID int32) (*repication.ReplicationTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConvertTaskByCluster", ctx, task, targetClusterID)
	ret0, _ := ret[0].(*repication.ReplicationTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConvertTaskByCluster indicates an expected call of ConvertTaskByCluster.
func (mr *MockAckManagerMockRecorder) ConvertTaskByCluster(ctx, task, targetClusterID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConvertTaskByCluster", reflect.TypeOf((*MockAckManager)(nil).ConvertTaskByCluster), ctx, task, targetClusterID)
}

// GetMaxTaskInfo mocks base method.
func (m *MockAckManager) GetMaxTaskInfo() (int64, time.Time) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMaxTaskInfo")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(time.Time)
	return ret0, ret1
}

// GetMaxTaskInfo indicates an expected call of GetMaxTaskInfo.
func (mr *MockAckManagerMockRecorder) GetMaxTaskInfo() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMaxTaskInfo", reflect.TypeOf((*MockAckManager)(nil).GetMaxTaskInfo))
}

// GetReplicationTasksIter mocks base method.
func (m *MockAckManager) GetReplicationTasksIter(ctx context.Context, pollingCluster string, minInclusiveTaskID, maxExclusiveTaskID int64) (collection.Iterator[tasks.Task], error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReplicationTasksIter", ctx, pollingCluster, minInclusiveTaskID, maxExclusiveTaskID)
	ret0, _ := ret[0].(collection.Iterator[tasks.Task])
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReplicationTasksIter indicates an expected call of GetReplicationTasksIter.
func (mr *MockAckManagerMockRecorder) GetReplicationTasksIter(ctx, pollingCluster, minInclusiveTaskID, maxExclusiveTaskID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReplicationTasksIter", reflect.TypeOf((*MockAckManager)(nil).GetReplicationTasksIter), ctx, pollingCluster, minInclusiveTaskID, maxExclusiveTaskID)
}

// GetTask mocks base method.
func (m *MockAckManager) GetTask(ctx context.Context, taskInfo *repication.ReplicationTaskInfo) (*repication.ReplicationTask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTask", ctx, taskInfo)
	ret0, _ := ret[0].(*repication.ReplicationTask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTask indicates an expected call of GetTask.
func (mr *MockAckManagerMockRecorder) GetTask(ctx, taskInfo any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTask", reflect.TypeOf((*MockAckManager)(nil).GetTask), ctx, taskInfo)
}

// GetTasks mocks base method.
func (m *MockAckManager) GetTasks(ctx context.Context, pollingCluster string, queryMessageID int64) (*repication.ReplicationMessages, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTasks", ctx, pollingCluster, queryMessageID)
	ret0, _ := ret[0].(*repication.ReplicationMessages)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTasks indicates an expected call of GetTasks.
func (mr *MockAckManagerMockRecorder) GetTasks(ctx, pollingCluster, queryMessageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTasks", reflect.TypeOf((*MockAckManager)(nil).GetTasks), ctx, pollingCluster, queryMessageID)
}

// NotifyNewTasks mocks base method.
func (m *MockAckManager) NotifyNewTasks(tasks []tasks.Task) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "NotifyNewTasks", tasks)
}

// NotifyNewTasks indicates an expected call of NotifyNewTasks.
func (mr *MockAckManagerMockRecorder) NotifyNewTasks(tasks any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyNewTasks", reflect.TypeOf((*MockAckManager)(nil).NotifyNewTasks), tasks)
}

// SubscribeNotification mocks base method.
func (m *MockAckManager) SubscribeNotification() (<-chan struct{}, string) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeNotification")
	ret0, _ := ret[0].(<-chan struct{})
	ret1, _ := ret[1].(string)
	return ret0, ret1
}

// SubscribeNotification indicates an expected call of SubscribeNotification.
func (mr *MockAckManagerMockRecorder) SubscribeNotification() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SubscribeNotification", reflect.TypeOf((*MockAckManager)(nil).SubscribeNotification))
}

// UnsubscribeNotification mocks base method.
func (m *MockAckManager) UnsubscribeNotification(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UnsubscribeNotification", arg0)
}

// UnsubscribeNotification indicates an expected call of UnsubscribeNotification.
func (mr *MockAckManagerMockRecorder) UnsubscribeNotification(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnsubscribeNotification", reflect.TypeOf((*MockAckManager)(nil).UnsubscribeNotification), arg0)
}
