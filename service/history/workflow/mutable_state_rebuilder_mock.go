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
// Source: mutable_state_rebuilder.go

// Package workflow is a generated GoMock package.
package workflow

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "go.temporal.io/api/common/v1"
	v10 "go.temporal.io/api/history/v1"
	namespace "go.temporal.io/server/common/namespace"
)

// MockMutableStateRebuilder is a mock of MutableStateRebuilder interface.
type MockMutableStateRebuilder struct {
	ctrl     *gomock.Controller
	recorder *MockMutableStateRebuilderMockRecorder
}

// MockMutableStateRebuilderMockRecorder is the mock recorder for MockMutableStateRebuilder.
type MockMutableStateRebuilderMockRecorder struct {
	mock *MockMutableStateRebuilder
}

// NewMockMutableStateRebuilder creates a new mock instance.
func NewMockMutableStateRebuilder(ctrl *gomock.Controller) *MockMutableStateRebuilder {
	mock := &MockMutableStateRebuilder{ctrl: ctrl}
	mock.recorder = &MockMutableStateRebuilderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMutableStateRebuilder) EXPECT() *MockMutableStateRebuilderMockRecorder {
	return m.recorder
}

// ApplyEvents mocks base method.
func (m *MockMutableStateRebuilder) ApplyEvents(ctx context.Context, namespaceID namespace.ID, requestID string, execution *v1.WorkflowExecution, history [][]*v10.HistoryEvent, newRunHistory []*v10.HistoryEvent, newRunID string) (MutableState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ApplyEvents", ctx, namespaceID, requestID, execution, history, newRunHistory, newRunID)
	ret0, _ := ret[0].(MutableState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ApplyEvents indicates an expected call of ApplyEvents.
func (mr *MockMutableStateRebuilderMockRecorder) ApplyEvents(ctx, namespaceID, requestID, execution, history, newRunHistory, newRunID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyEvents", reflect.TypeOf((*MockMutableStateRebuilder)(nil).ApplyEvents), ctx, namespaceID, requestID, execution, history, newRunHistory, newRunID)
}
