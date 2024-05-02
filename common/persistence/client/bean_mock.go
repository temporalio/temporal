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

// MockBean is a mock of Bean interface.
type MockBean struct {
	ctrl     *gomock.Controller
	recorder *MockBeanMockRecorder
}

// MockBeanMockRecorder is the mock recorder for MockBean.
type MockBeanMockRecorder struct {
	mock *MockBean
}

// NewMockBean creates a new mock instance.
func NewMockBean(ctrl *gomock.Controller) *MockBean {
	mock := &MockBean{ctrl: ctrl}
	mock.recorder = &MockBeanMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBean) EXPECT() *MockBeanMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockBean) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockBeanMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockBean)(nil).Close))
}

// GetClusterMetadataManager mocks base method.
func (m *MockBean) GetClusterMetadataManager() persistence.ClusterMetadataManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterMetadataManager")
	ret0, _ := ret[0].(persistence.ClusterMetadataManager)
	return ret0
}

// GetClusterMetadataManager indicates an expected call of GetClusterMetadataManager.
func (mr *MockBeanMockRecorder) GetClusterMetadataManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterMetadataManager", reflect.TypeOf((*MockBean)(nil).GetClusterMetadataManager))
}

// GetExecutionManager mocks base method.
func (m *MockBean) GetExecutionManager() persistence.ExecutionManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExecutionManager")
	ret0, _ := ret[0].(persistence.ExecutionManager)
	return ret0
}

// GetExecutionManager indicates an expected call of GetExecutionManager.
func (mr *MockBeanMockRecorder) GetExecutionManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExecutionManager", reflect.TypeOf((*MockBean)(nil).GetExecutionManager))
}

// GetMetadataManager mocks base method.
func (m *MockBean) GetMetadataManager() persistence.MetadataManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMetadataManager")
	ret0, _ := ret[0].(persistence.MetadataManager)
	return ret0
}

// GetMetadataManager indicates an expected call of GetMetadataManager.
func (mr *MockBeanMockRecorder) GetMetadataManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetadataManager", reflect.TypeOf((*MockBean)(nil).GetMetadataManager))
}

// GetNamespaceReplicationQueue mocks base method.
func (m *MockBean) GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceReplicationQueue")
	ret0, _ := ret[0].(persistence.NamespaceReplicationQueue)
	return ret0
}

// GetNamespaceReplicationQueue indicates an expected call of GetNamespaceReplicationQueue.
func (mr *MockBeanMockRecorder) GetNamespaceReplicationQueue() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceReplicationQueue", reflect.TypeOf((*MockBean)(nil).GetNamespaceReplicationQueue))
}

// GetNexusEndpointManager mocks base method.
func (m *MockBean) GetNexusEndpointManager() persistence.NexusEndpointManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNexusEndpointManager")
	ret0, _ := ret[0].(persistence.NexusEndpointManager)
	return ret0
}

// GetNexusEndpointManager indicates an expected call of GetNexusEndpointManager.
func (mr *MockBeanMockRecorder) GetNexusEndpointManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNexusEndpointManager", reflect.TypeOf((*MockBean)(nil).GetNexusEndpointManager))
}

// GetShardManager mocks base method.
func (m *MockBean) GetShardManager() persistence.ShardManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardManager")
	ret0, _ := ret[0].(persistence.ShardManager)
	return ret0
}

// GetShardManager indicates an expected call of GetShardManager.
func (mr *MockBeanMockRecorder) GetShardManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardManager", reflect.TypeOf((*MockBean)(nil).GetShardManager))
}

// GetTaskManager mocks base method.
func (m *MockBean) GetTaskManager() persistence.TaskManager {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskManager")
	ret0, _ := ret[0].(persistence.TaskManager)
	return ret0
}

// GetTaskManager indicates an expected call of GetTaskManager.
func (mr *MockBeanMockRecorder) GetTaskManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskManager", reflect.TypeOf((*MockBean)(nil).GetTaskManager))
}
