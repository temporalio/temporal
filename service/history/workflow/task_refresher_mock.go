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
// Source: task_refresher.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package workflow -source task_refresher.go -destination task_refresher_mock.go
//

// Package workflow is a generated GoMock package.
package workflow

import (
	context "context"
	reflect "reflect"

	persistence "go.temporal.io/server/api/persistence/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockTaskRefresher is a mock of TaskRefresher interface.
type MockTaskRefresher struct {
	ctrl     *gomock.Controller
	recorder *MockTaskRefresherMockRecorder
}

// MockTaskRefresherMockRecorder is the mock recorder for MockTaskRefresher.
type MockTaskRefresherMockRecorder struct {
	mock *MockTaskRefresher
}

// NewMockTaskRefresher creates a new mock instance.
func NewMockTaskRefresher(ctrl *gomock.Controller) *MockTaskRefresher {
	mock := &MockTaskRefresher{ctrl: ctrl}
	mock.recorder = &MockTaskRefresherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTaskRefresher) EXPECT() *MockTaskRefresherMockRecorder {
	return m.recorder
}

// PartialRefresh mocks base method.
func (m *MockTaskRefresher) PartialRefresh(ctx context.Context, mutableState MutableState, minVersionedTransition *persistence.VersionedTransition) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PartialRefresh", ctx, mutableState, minVersionedTransition)
	ret0, _ := ret[0].(error)
	return ret0
}

// PartialRefresh indicates an expected call of PartialRefresh.
func (mr *MockTaskRefresherMockRecorder) PartialRefresh(ctx, mutableState, minVersionedTransition any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PartialRefresh", reflect.TypeOf((*MockTaskRefresher)(nil).PartialRefresh), ctx, mutableState, minVersionedTransition)
}

// Refresh mocks base method.
func (m *MockTaskRefresher) Refresh(ctx context.Context, mutableState MutableState) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Refresh", ctx, mutableState)
	ret0, _ := ret[0].(error)
	return ret0
}

// Refresh indicates an expected call of Refresh.
func (mr *MockTaskRefresherMockRecorder) Refresh(ctx, mutableState any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Refresh", reflect.TypeOf((*MockTaskRefresher)(nil).Refresh), ctx, mutableState)
}
