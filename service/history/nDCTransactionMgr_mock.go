// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
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
// Source: nDCTransactionMgr.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"

	persistence "github.com/temporalio/temporal/common/persistence"
)

// MocknDCTransactionMgr is a mock of nDCTransactionMgr interface
type MocknDCTransactionMgr struct {
	ctrl     *gomock.Controller
	recorder *MocknDCTransactionMgrMockRecorder
}

// MocknDCTransactionMgrMockRecorder is the mock recorder for MocknDCTransactionMgr
type MocknDCTransactionMgrMockRecorder struct {
	mock *MocknDCTransactionMgr
}

// NewMocknDCTransactionMgr creates a new mock instance
func NewMocknDCTransactionMgr(ctrl *gomock.Controller) *MocknDCTransactionMgr {
	mock := &MocknDCTransactionMgr{ctrl: ctrl}
	mock.recorder = &MocknDCTransactionMgrMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MocknDCTransactionMgr) EXPECT() *MocknDCTransactionMgrMockRecorder {
	return m.recorder
}

// createWorkflow mocks base method
func (m *MocknDCTransactionMgr) createWorkflow(ctx context.Context, now time.Time, targetWorkflow nDCWorkflow) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "createWorkflow", ctx, now, targetWorkflow)
	ret0, _ := ret[0].(error)
	return ret0
}

// createWorkflow indicates an expected call of createWorkflow
func (mr *MocknDCTransactionMgrMockRecorder) createWorkflow(ctx, now, targetWorkflow interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "createWorkflow", reflect.TypeOf((*MocknDCTransactionMgr)(nil).createWorkflow), ctx, now, targetWorkflow)
}

// updateWorkflow mocks base method
func (m *MocknDCTransactionMgr) updateWorkflow(ctx context.Context, now time.Time, isWorkflowRebuilt bool, targetWorkflow, newWorkflow nDCWorkflow) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflow", ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflow indicates an expected call of updateWorkflow
func (mr *MocknDCTransactionMgrMockRecorder) updateWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflow", reflect.TypeOf((*MocknDCTransactionMgr)(nil).updateWorkflow), ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
}

// backfillWorkflow mocks base method
func (m *MocknDCTransactionMgr) backfillWorkflow(ctx context.Context, now time.Time, targetWorkflow nDCWorkflow, targetWorkflowEvents *persistence.WorkflowEvents) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "backfillWorkflow", ctx, now, targetWorkflow, targetWorkflowEvents)
	ret0, _ := ret[0].(error)
	return ret0
}

// backfillWorkflow indicates an expected call of backfillWorkflow
func (mr *MocknDCTransactionMgrMockRecorder) backfillWorkflow(ctx, now, targetWorkflow, targetWorkflowEvents interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "backfillWorkflow", reflect.TypeOf((*MocknDCTransactionMgr)(nil).backfillWorkflow), ctx, now, targetWorkflow, targetWorkflowEvents)
}

// checkWorkflowExists mocks base method
func (m *MocknDCTransactionMgr) checkWorkflowExists(ctx context.Context, domainID, workflowID, runID string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "checkWorkflowExists", ctx, domainID, workflowID, runID)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// checkWorkflowExists indicates an expected call of checkWorkflowExists
func (mr *MocknDCTransactionMgrMockRecorder) checkWorkflowExists(ctx, domainID, workflowID, runID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "checkWorkflowExists", reflect.TypeOf((*MocknDCTransactionMgr)(nil).checkWorkflowExists), ctx, domainID, workflowID, runID)
}

// getCurrentWorkflowRunID mocks base method
func (m *MocknDCTransactionMgr) getCurrentWorkflowRunID(ctx context.Context, domainID, workflowID string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getCurrentWorkflowRunID", ctx, domainID, workflowID)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// getCurrentWorkflowRunID indicates an expected call of getCurrentWorkflowRunID
func (mr *MocknDCTransactionMgrMockRecorder) getCurrentWorkflowRunID(ctx, domainID, workflowID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getCurrentWorkflowRunID", reflect.TypeOf((*MocknDCTransactionMgr)(nil).getCurrentWorkflowRunID), ctx, domainID, workflowID)
}

// loadNDCWorkflow mocks base method
func (m *MocknDCTransactionMgr) loadNDCWorkflow(ctx context.Context, domainID, workflowID, runID string) (nDCWorkflow, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "loadNDCWorkflow", ctx, domainID, workflowID, runID)
	ret0, _ := ret[0].(nDCWorkflow)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// loadNDCWorkflow indicates an expected call of loadNDCWorkflow
func (mr *MocknDCTransactionMgrMockRecorder) loadNDCWorkflow(ctx, domainID, workflowID, runID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "loadNDCWorkflow", reflect.TypeOf((*MocknDCTransactionMgr)(nil).loadNDCWorkflow), ctx, domainID, workflowID, runID)
}
