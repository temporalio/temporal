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
// Source: transaction_manager_existing_workflow.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package ndc -source transaction_manager_existing_workflow.go -destination transaction_manager_existing_workflow_mock.go
//

// Package ndc is a generated GoMock package.
package ndc

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MocktransactionMgrForExistingWorkflow is a mock of transactionMgrForExistingWorkflow interface.
type MocktransactionMgrForExistingWorkflow struct {
	ctrl     *gomock.Controller
	recorder *MocktransactionMgrForExistingWorkflowMockRecorder
}

// MocktransactionMgrForExistingWorkflowMockRecorder is the mock recorder for MocktransactionMgrForExistingWorkflow.
type MocktransactionMgrForExistingWorkflowMockRecorder struct {
	mock *MocktransactionMgrForExistingWorkflow
}

// NewMocktransactionMgrForExistingWorkflow creates a new mock instance.
func NewMocktransactionMgrForExistingWorkflow(ctrl *gomock.Controller) *MocktransactionMgrForExistingWorkflow {
	mock := &MocktransactionMgrForExistingWorkflow{ctrl: ctrl}
	mock.recorder = &MocktransactionMgrForExistingWorkflowMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocktransactionMgrForExistingWorkflow) EXPECT() *MocktransactionMgrForExistingWorkflowMockRecorder {
	return m.recorder
}

// dispatchForExistingWorkflow mocks base method.
func (m *MocktransactionMgrForExistingWorkflow) dispatchForExistingWorkflow(ctx context.Context, isWorkflowRebuilt bool, targetWorkflow, newWorkflow Workflow) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "dispatchForExistingWorkflow", ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	ret0, _ := ret[0].(error)
	return ret0
}

// dispatchForExistingWorkflow indicates an expected call of dispatchForExistingWorkflow.
func (mr *MocktransactionMgrForExistingWorkflowMockRecorder) dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "dispatchForExistingWorkflow", reflect.TypeOf((*MocktransactionMgrForExistingWorkflow)(nil).dispatchForExistingWorkflow), ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
}
