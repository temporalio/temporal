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
// Source: activity_state_replicator.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package ndc -source activity_state_replicator.go -destination activity_state_replicator_mock.go
//

// Package ndc is a generated GoMock package.
package ndc

import (
	context "context"
	reflect "reflect"

	historyservice "go.temporal.io/server/api/historyservice/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockActivityStateReplicator is a mock of ActivityStateReplicator interface.
type MockActivityStateReplicator struct {
	ctrl     *gomock.Controller
	recorder *MockActivityStateReplicatorMockRecorder
	isgomock struct{}
}

// MockActivityStateReplicatorMockRecorder is the mock recorder for MockActivityStateReplicator.
type MockActivityStateReplicatorMockRecorder struct {
	mock *MockActivityStateReplicator
}

// NewMockActivityStateReplicator creates a new mock instance.
func NewMockActivityStateReplicator(ctrl *gomock.Controller) *MockActivityStateReplicator {
	mock := &MockActivityStateReplicator{ctrl: ctrl}
	mock.recorder = &MockActivityStateReplicatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockActivityStateReplicator) EXPECT() *MockActivityStateReplicatorMockRecorder {
	return m.recorder
}

// SyncActivitiesState mocks base method.
func (m *MockActivityStateReplicator) SyncActivitiesState(ctx context.Context, request *historyservice.SyncActivitiesRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncActivitiesState", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncActivitiesState indicates an expected call of SyncActivitiesState.
func (mr *MockActivityStateReplicatorMockRecorder) SyncActivitiesState(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncActivitiesState", reflect.TypeOf((*MockActivityStateReplicator)(nil).SyncActivitiesState), ctx, request)
}

// SyncActivityState mocks base method.
func (m *MockActivityStateReplicator) SyncActivityState(ctx context.Context, request *historyservice.SyncActivityRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncActivityState", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncActivityState indicates an expected call of SyncActivityState.
func (mr *MockActivityStateReplicatorMockRecorder) SyncActivityState(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncActivityState", reflect.TypeOf((*MockActivityStateReplicator)(nil).SyncActivityState), ctx, request)
}
