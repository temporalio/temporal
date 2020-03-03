// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

// Code generated by MockGen. DO NOT EDIT.
// Source: replicationTaskExecutor.go

// Package history is a generated GoMock package.
package history

import (
	gomock "github.com/golang/mock/gomock"
	replication "github.com/temporalio/temporal/.gen/proto/replication"
	reflect "reflect"
)

// MockreplicationTaskExecutor is a mock of replicationTaskExecutor interface
type MockreplicationTaskExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockreplicationTaskExecutorMockRecorder
}

// MockreplicationTaskExecutorMockRecorder is the mock recorder for MockreplicationTaskExecutor
type MockreplicationTaskExecutorMockRecorder struct {
	mock *MockreplicationTaskExecutor
}

// NewMockreplicationTaskExecutor creates a new mock instance
func NewMockreplicationTaskExecutor(ctrl *gomock.Controller) *MockreplicationTaskExecutor {
	mock := &MockreplicationTaskExecutor{ctrl: ctrl}
	mock.recorder = &MockreplicationTaskExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockreplicationTaskExecutor) EXPECT() *MockreplicationTaskExecutorMockRecorder {
	return m.recorder
}

// execute mocks base method
func (m *MockreplicationTaskExecutor) execute(sourceCluster string, replicationTask *replication.ReplicationTask, forceApply bool) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "execute", sourceCluster, replicationTask, forceApply)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// execute indicates an expected call of execute
func (mr *MockreplicationTaskExecutorMockRecorder) execute(sourceCluster, replicationTask, forceApply interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "execute", reflect.TypeOf((*MockreplicationTaskExecutor)(nil).execute), sourceCluster, replicationTask, forceApply)
}
