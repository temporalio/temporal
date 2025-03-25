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
// Source: history_branch_util.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package persistence -source history_branch_util.go -destination history_branch_util_mock.go
//

// Package persistence is a generated GoMock package.
package persistence

import (
	reflect "reflect"
	time "time"

	persistence "go.temporal.io/server/api/persistence/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockHistoryBranchUtil is a mock of HistoryBranchUtil interface.
type MockHistoryBranchUtil struct {
	ctrl     *gomock.Controller
	recorder *MockHistoryBranchUtilMockRecorder
	isgomock struct{}
}

// MockHistoryBranchUtilMockRecorder is the mock recorder for MockHistoryBranchUtil.
type MockHistoryBranchUtilMockRecorder struct {
	mock *MockHistoryBranchUtil
}

// NewMockHistoryBranchUtil creates a new mock instance.
func NewMockHistoryBranchUtil(ctrl *gomock.Controller) *MockHistoryBranchUtil {
	mock := &MockHistoryBranchUtil{ctrl: ctrl}
	mock.recorder = &MockHistoryBranchUtilMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHistoryBranchUtil) EXPECT() *MockHistoryBranchUtilMockRecorder {
	return m.recorder
}

// NewHistoryBranch mocks base method.
func (m *MockHistoryBranchUtil) NewHistoryBranch(namespaceID, workflowID, runID, treeID string, branchID *string, ancestors []*persistence.HistoryBranchRange, runTimeout, executionTimeout, retentionDuration time.Duration) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewHistoryBranch", namespaceID, workflowID, runID, treeID, branchID, ancestors, runTimeout, executionTimeout, retentionDuration)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewHistoryBranch indicates an expected call of NewHistoryBranch.
func (mr *MockHistoryBranchUtilMockRecorder) NewHistoryBranch(namespaceID, workflowID, runID, treeID, branchID, ancestors, runTimeout, executionTimeout, retentionDuration any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewHistoryBranch", reflect.TypeOf((*MockHistoryBranchUtil)(nil).NewHistoryBranch), namespaceID, workflowID, runID, treeID, branchID, ancestors, runTimeout, executionTimeout, retentionDuration)
}

// ParseHistoryBranchInfo mocks base method.
func (m *MockHistoryBranchUtil) ParseHistoryBranchInfo(branchToken []byte) (*persistence.HistoryBranch, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ParseHistoryBranchInfo", branchToken)
	ret0, _ := ret[0].(*persistence.HistoryBranch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ParseHistoryBranchInfo indicates an expected call of ParseHistoryBranchInfo.
func (mr *MockHistoryBranchUtilMockRecorder) ParseHistoryBranchInfo(branchToken any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ParseHistoryBranchInfo", reflect.TypeOf((*MockHistoryBranchUtil)(nil).ParseHistoryBranchInfo), branchToken)
}

// UpdateHistoryBranchInfo mocks base method.
func (m *MockHistoryBranchUtil) UpdateHistoryBranchInfo(branchToken []byte, branchInfo *persistence.HistoryBranch, runID string) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateHistoryBranchInfo", branchToken, branchInfo, runID)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateHistoryBranchInfo indicates an expected call of UpdateHistoryBranchInfo.
func (mr *MockHistoryBranchUtilMockRecorder) UpdateHistoryBranchInfo(branchToken, branchInfo, runID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateHistoryBranchInfo", reflect.TypeOf((*MockHistoryBranchUtil)(nil).UpdateHistoryBranchInfo), branchToken, branchInfo, runID)
}
