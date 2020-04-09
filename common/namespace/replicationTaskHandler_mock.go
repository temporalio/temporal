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
// Source: replicationTaskExecutor.go

// Package namespace is a generated GoMock package.
package namespace

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	replication "github.com/temporalio/temporal/.gen/proto/replication"
)

// MockReplicationTaskExecutor is a mock of ReplicationTaskExecutor interface.
type MockReplicationTaskExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockReplicationTaskExecutorMockRecorder
}

// MockReplicationTaskExecutorMockRecorder is the mock recorder for MockReplicationTaskExecutor.
type MockReplicationTaskExecutorMockRecorder struct {
	mock *MockReplicationTaskExecutor
}

// NewMockReplicationTaskExecutor creates a new mock instance.
func NewMockReplicationTaskExecutor(ctrl *gomock.Controller) *MockReplicationTaskExecutor {
	mock := &MockReplicationTaskExecutor{ctrl: ctrl}
	mock.recorder = &MockReplicationTaskExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReplicationTaskExecutor) EXPECT() *MockReplicationTaskExecutorMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockReplicationTaskExecutor) Execute(task *replication.NamespaceTaskAttributes) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", task)
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockReplicationTaskExecutorMockRecorder) Execute(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockReplicationTaskExecutor)(nil).Execute), task)
}
