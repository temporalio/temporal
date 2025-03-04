// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
//

// Code generated by MockGen. DO NOT EDIT.
// Source: branch_manager.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package ndc -source branch_manager.go -destination branch_manager_mock.go
//

// Package ndc is a generated GoMock package.
package ndc

import (
	context "context"
	reflect "reflect"

	history "go.temporal.io/server/api/history/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockBranchMgr is a mock of BranchMgr interface.
type MockBranchMgr struct {
	ctrl     *gomock.Controller
	recorder *MockBranchMgrMockRecorder
}

// MockBranchMgrMockRecorder is the mock recorder for MockBranchMgr.
type MockBranchMgrMockRecorder struct {
	mock *MockBranchMgr
}

// NewMockBranchMgr creates a new mock instance.
func NewMockBranchMgr(ctrl *gomock.Controller) *MockBranchMgr {
	mock := &MockBranchMgr{ctrl: ctrl}
	mock.recorder = &MockBranchMgrMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBranchMgr) EXPECT() *MockBranchMgrMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockBranchMgr) Create(ctx context.Context, incomingVersionHistory *history.VersionHistory, incomingFirstEventID, incomingFirstEventVersion int64) (bool, int32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(int32)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Create indicates an expected call of Create.
func (mr *MockBranchMgrMockRecorder) Create(ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockBranchMgr)(nil).Create), ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion)
}

// GetOrCreate mocks base method.
func (m *MockBranchMgr) GetOrCreate(ctx context.Context, incomingVersionHistory *history.VersionHistory, incomingFirstEventID, incomingFirstEventVersion int64) (bool, int32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrCreate", ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(int32)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetOrCreate indicates an expected call of GetOrCreate.
func (mr *MockBranchMgrMockRecorder) GetOrCreate(ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrCreate", reflect.TypeOf((*MockBranchMgr)(nil).GetOrCreate), ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion)
}
