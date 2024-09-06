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
// Source: conflict_resolver.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package ndc -source conflict_resolver.go -destination conflict_resolver_mock.go
//

// Package ndc is a generated GoMock package.
package ndc

import (
	context "context"
	reflect "reflect"

	workflow "go.temporal.io/server/service/history/workflow"
	gomock "go.uber.org/mock/gomock"
)

// MockConflictResolver is a mock of ConflictResolver interface.
type MockConflictResolver struct {
	ctrl     *gomock.Controller
	recorder *MockConflictResolverMockRecorder
}

// MockConflictResolverMockRecorder is the mock recorder for MockConflictResolver.
type MockConflictResolverMockRecorder struct {
	mock *MockConflictResolver
}

// NewMockConflictResolver creates a new mock instance.
func NewMockConflictResolver(ctrl *gomock.Controller) *MockConflictResolver {
	mock := &MockConflictResolver{ctrl: ctrl}
	mock.recorder = &MockConflictResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConflictResolver) EXPECT() *MockConflictResolverMockRecorder {
	return m.recorder
}

// GetOrRebuildCurrentMutableState mocks base method.
func (m *MockConflictResolver) GetOrRebuildCurrentMutableState(ctx context.Context, branchIndex int32, incomingVersion int64) (workflow.MutableState, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrRebuildCurrentMutableState", ctx, branchIndex, incomingVersion)
	ret0, _ := ret[0].(workflow.MutableState)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetOrRebuildCurrentMutableState indicates an expected call of GetOrRebuildCurrentMutableState.
func (mr *MockConflictResolverMockRecorder) GetOrRebuildCurrentMutableState(ctx, branchIndex, incomingVersion any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrRebuildCurrentMutableState", reflect.TypeOf((*MockConflictResolver)(nil).GetOrRebuildCurrentMutableState), ctx, branchIndex, incomingVersion)
}

// GetOrRebuildMutableState mocks base method.
func (m *MockConflictResolver) GetOrRebuildMutableState(ctx context.Context, branchIndex int32) (workflow.MutableState, bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOrRebuildMutableState", ctx, branchIndex)
	ret0, _ := ret[0].(workflow.MutableState)
	ret1, _ := ret[1].(bool)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetOrRebuildMutableState indicates an expected call of GetOrRebuildMutableState.
func (mr *MockConflictResolverMockRecorder) GetOrRebuildMutableState(ctx, branchIndex any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOrRebuildMutableState", reflect.TypeOf((*MockConflictResolver)(nil).GetOrRebuildMutableState), ctx, branchIndex)
}
