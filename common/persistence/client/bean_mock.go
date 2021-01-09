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
// Source: bean.go

// Package client is a generated GoMock package.
package client

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	persistence "go.temporal.io/server/common/persistence"
)

// MockBean is a mock of Bean interface
type MockBean struct {
	ctrl     *gomock.Controller
	recorder *MockBeanMockRecorder
}

// MockBeanMockRecorder is the mock recorder for MockBean
type MockBeanMockRecorder struct {
	mock *MockBean
}

// NewMockBean creates a new mock instance
func NewMockBean(ctrl *gomock.Controller) *MockBean {
	mock := &MockBean{ctrl: ctrl}
	mock.recorder = &MockBeanMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockBean) EXPECT() *MockBeanMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockBean) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close
func (mr *MockBeanMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockBean)(nil).Close))
}

// GetClusterMetadataManager mocks base method
func (m *MockBean) GetClusterMetadataManager() persistence.ClusterMetadataManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterMetadataManager")
	ret0, _ := ret[0].(persistence.ClusterMetadataManager)
	return ret0
}

// GetClusterMetadataManager indicates an expected call of GetClusterMetadataManager
func (mr *MockBeanMockRecorder) GetClusterMetadataManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterMetadataManager", reflect.TypeOf((*MockBean)(nil).GetClusterMetadataManager))
}

// SetClusterMetadataManager mocks base method
func (m *MockBean) SetClusterMetadataManager(arg0 persistence.ClusterMetadataManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetClusterMetadataManager", arg0)
}

// SetClusterMetadataManager indicates an expected call of SetClusterMetadataManager
func (mr *MockBeanMockRecorder) SetClusterMetadataManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetClusterMetadataManager", reflect.TypeOf((*MockBean)(nil).SetClusterMetadataManager), arg0)
}

// GetMetadataManager mocks base method
func (m *MockBean) GetMetadataManager() persistence.MetadataManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMetadataManager")
	ret0, _ := ret[0].(persistence.MetadataManager)
	return ret0
}

// GetMetadataManager indicates an expected call of GetMetadataManager
func (mr *MockBeanMockRecorder) GetMetadataManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetadataManager", reflect.TypeOf((*MockBean)(nil).GetMetadataManager))
}

// SetMetadataManager mocks base method
func (m *MockBean) SetMetadataManager(arg0 persistence.MetadataManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetMetadataManager", arg0)
}

// SetMetadataManager indicates an expected call of SetMetadataManager
func (mr *MockBeanMockRecorder) SetMetadataManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMetadataManager", reflect.TypeOf((*MockBean)(nil).SetMetadataManager), arg0)
}

// GetTaskManager mocks base method
func (m *MockBean) GetTaskManager() persistence.TaskManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskManager")
	ret0, _ := ret[0].(persistence.TaskManager)
	return ret0
}

// GetTaskManager indicates an expected call of GetTaskManager
func (mr *MockBeanMockRecorder) GetTaskManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskManager", reflect.TypeOf((*MockBean)(nil).GetTaskManager))
}

// SetTaskManager mocks base method
func (m *MockBean) SetTaskManager(arg0 persistence.TaskManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTaskManager", arg0)
}

// SetTaskManager indicates an expected call of SetTaskManager
func (mr *MockBeanMockRecorder) SetTaskManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTaskManager", reflect.TypeOf((*MockBean)(nil).SetTaskManager), arg0)
}

// GetVisibilityManager mocks base method
func (m *MockBean) GetVisibilityManager() persistence.VisibilityManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVisibilityManager")
	ret0, _ := ret[0].(persistence.VisibilityManager)
	return ret0
}

// GetVisibilityManager indicates an expected call of GetVisibilityManager
func (mr *MockBeanMockRecorder) GetVisibilityManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVisibilityManager", reflect.TypeOf((*MockBean)(nil).GetVisibilityManager))
}

// SetVisibilityManager mocks base method
func (m *MockBean) SetVisibilityManager(arg0 persistence.VisibilityManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetVisibilityManager", arg0)
}

// SetVisibilityManager indicates an expected call of SetVisibilityManager
func (mr *MockBeanMockRecorder) SetVisibilityManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetVisibilityManager", reflect.TypeOf((*MockBean)(nil).SetVisibilityManager), arg0)
}

// GetNamespaceReplicationQueue mocks base method
func (m *MockBean) GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceReplicationQueue")
	ret0, _ := ret[0].(persistence.NamespaceReplicationQueue)
	return ret0
}

// GetNamespaceReplicationQueue indicates an expected call of GetNamespaceReplicationQueue
func (mr *MockBeanMockRecorder) GetNamespaceReplicationQueue() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceReplicationQueue", reflect.TypeOf((*MockBean)(nil).GetNamespaceReplicationQueue))
}

// SetNamespaceReplicationQueue mocks base method
func (m *MockBean) SetNamespaceReplicationQueue(arg0 persistence.NamespaceReplicationQueue) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetNamespaceReplicationQueue", arg0)
}

// SetNamespaceReplicationQueue indicates an expected call of SetNamespaceReplicationQueue
func (mr *MockBeanMockRecorder) SetNamespaceReplicationQueue(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetNamespaceReplicationQueue", reflect.TypeOf((*MockBean)(nil).SetNamespaceReplicationQueue), arg0)
}

// GetShardManager mocks base method
func (m *MockBean) GetShardManager() persistence.ShardManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardManager")
	ret0, _ := ret[0].(persistence.ShardManager)
	return ret0
}

// GetShardManager indicates an expected call of GetShardManager
func (mr *MockBeanMockRecorder) GetShardManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardManager", reflect.TypeOf((*MockBean)(nil).GetShardManager))
}

// SetShardManager mocks base method
func (m *MockBean) SetShardManager(arg0 persistence.ShardManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetShardManager", arg0)
}

// SetShardManager indicates an expected call of SetShardManager
func (mr *MockBeanMockRecorder) SetShardManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetShardManager", reflect.TypeOf((*MockBean)(nil).SetShardManager), arg0)
}

// GetHistoryManager mocks base method
func (m *MockBean) GetHistoryManager() persistence.HistoryManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHistoryManager")
	ret0, _ := ret[0].(persistence.HistoryManager)
	return ret0
}

// GetHistoryManager indicates an expected call of GetHistoryManager
func (mr *MockBeanMockRecorder) GetHistoryManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHistoryManager", reflect.TypeOf((*MockBean)(nil).GetHistoryManager))
}

// SetHistoryManager mocks base method
func (m *MockBean) SetHistoryManager(arg0 persistence.HistoryManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetHistoryManager", arg0)
}

// SetHistoryManager indicates an expected call of SetHistoryManager
func (mr *MockBeanMockRecorder) SetHistoryManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHistoryManager", reflect.TypeOf((*MockBean)(nil).SetHistoryManager), arg0)
}

// GetExecutionManager mocks base method
func (m *MockBean) GetExecutionManager(arg0 int32) (persistence.ExecutionManager, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExecutionManager", arg0)
	ret0, _ := ret[0].(persistence.ExecutionManager)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetExecutionManager indicates an expected call of GetExecutionManager
func (mr *MockBeanMockRecorder) GetExecutionManager(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExecutionManager", reflect.TypeOf((*MockBean)(nil).GetExecutionManager), arg0)
}

// SetExecutionManager mocks base method
func (m *MockBean) SetExecutionManager(arg0 int32, arg1 persistence.ExecutionManager) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetExecutionManager", arg0, arg1)
}

// SetExecutionManager indicates an expected call of SetExecutionManager
func (mr *MockBeanMockRecorder) SetExecutionManager(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetExecutionManager", reflect.TypeOf((*MockBean)(nil).SetExecutionManager), arg0, arg1)
}
