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
// Source: nDCTransactionMgrForExistingWorkflow.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
)

// MocknDCTransactionMgrForExistingWorkflow is a mock of nDCTransactionMgrForExistingWorkflow interface.
type MocknDCTransactionMgrForExistingWorkflow struct {
	ctrl     *gomock.Controller
	recorder *MocknDCTransactionMgrForExistingWorkflowMockRecorder
}

// MocknDCTransactionMgrForExistingWorkflowMockRecorder is the mock recorder for MocknDCTransactionMgrForExistingWorkflow.
type MocknDCTransactionMgrForExistingWorkflowMockRecorder struct {
	mock *MocknDCTransactionMgrForExistingWorkflow
}

// NewMocknDCTransactionMgrForExistingWorkflow creates a new mock instance.
func NewMocknDCTransactionMgrForExistingWorkflow(ctrl *gomock.Controller) *MocknDCTransactionMgrForExistingWorkflow {
	mock := &MocknDCTransactionMgrForExistingWorkflow{ctrl: ctrl}
	mock.recorder = &MocknDCTransactionMgrForExistingWorkflowMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocknDCTransactionMgrForExistingWorkflow) EXPECT() *MocknDCTransactionMgrForExistingWorkflowMockRecorder {
	return m.recorder
}

// dispatchForExistingWorkflow mocks base method.
func (m *MocknDCTransactionMgrForExistingWorkflow) dispatchForExistingWorkflow(ctx context.Context, now time.Time, isWorkflowRebuilt bool, targetWorkflow, newWorkflow nDCWorkflow) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "dispatchForExistingWorkflow", ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	ret0, _ := ret[0].(error)
	return ret0
}

// dispatchForExistingWorkflow indicates an expected call of dispatchForExistingWorkflow.
func (mr *MocknDCTransactionMgrForExistingWorkflowMockRecorder) dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "dispatchForExistingWorkflow", reflect.TypeOf((*MocknDCTransactionMgrForExistingWorkflow)(nil).dispatchForExistingWorkflow), ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
}
