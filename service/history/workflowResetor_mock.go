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
// Source: workflowResetor.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	historyservice "github.com/temporalio/temporal/.gen/proto/historyservice"
	workflowservice "go.temporal.io/temporal-proto/workflowservice"
	reflect "reflect"
)

// MockworkflowResetor is a mock of workflowResetor interface.
type MockworkflowResetor struct {
	ctrl     *gomock.Controller
	recorder *MockworkflowResetorMockRecorder
}

// MockworkflowResetorMockRecorder is the mock recorder for MockworkflowResetor.
type MockworkflowResetorMockRecorder struct {
	mock *MockworkflowResetor
}

// NewMockworkflowResetor creates a new mock instance.
func NewMockworkflowResetor(ctrl *gomock.Controller) *MockworkflowResetor {
	mock := &MockworkflowResetor{ctrl: ctrl}
	mock.recorder = &MockworkflowResetorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockworkflowResetor) EXPECT() *MockworkflowResetorMockRecorder {
	return m.recorder
}

// ResetWorkflowExecution mocks base method.
func (m *MockworkflowResetor) ResetWorkflowExecution(ctx context.Context, resetRequest *workflowservice.ResetWorkflowExecutionRequest, baseContext workflowExecutionContext, baseMutableState mutableState, currContext workflowExecutionContext, currMutableState mutableState) (*historyservice.ResetWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetWorkflowExecution", ctx, resetRequest, baseContext, baseMutableState, currContext, currMutableState)
	ret0, _ := ret[0].(*historyservice.ResetWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetWorkflowExecution indicates an expected call of ResetWorkflowExecution.
func (mr *MockworkflowResetorMockRecorder) ResetWorkflowExecution(ctx, resetRequest, baseContext, baseMutableState, currContext, currMutableState interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetWorkflowExecution", reflect.TypeOf((*MockworkflowResetor)(nil).ResetWorkflowExecution), ctx, resetRequest, baseContext, baseMutableState, currContext, currMutableState)
}

// ApplyResetEvent mocks base method.
func (m *MockworkflowResetor) ApplyResetEvent(ctx context.Context, request *historyservice.ReplicateEventsRequest, namespaceID, workflowID, currentRunID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ApplyResetEvent", ctx, request, namespaceID, workflowID, currentRunID)
	ret0, _ := ret[0].(error)
	return ret0
}

// ApplyResetEvent indicates an expected call of ApplyResetEvent.
func (mr *MockworkflowResetorMockRecorder) ApplyResetEvent(ctx, request, namespaceID, workflowID, currentRunID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyResetEvent", reflect.TypeOf((*MockworkflowResetor)(nil).ApplyResetEvent), ctx, request, namespaceID, workflowID, currentRunID)
}
