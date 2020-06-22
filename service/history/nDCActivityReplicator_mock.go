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
// Source: nDCActivityReplicator.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	historyservice "github.com/temporalio/temporal/.gen/proto/historyservice/v1"
	reflect "reflect"
)

// MocknDCActivityReplicator is a mock of nDCActivityReplicator interface.
type MocknDCActivityReplicator struct {
	ctrl     *gomock.Controller
	recorder *MocknDCActivityReplicatorMockRecorder
}

// MocknDCActivityReplicatorMockRecorder is the mock recorder for MocknDCActivityReplicator.
type MocknDCActivityReplicatorMockRecorder struct {
	mock *MocknDCActivityReplicator
}

// NewMocknDCActivityReplicator creates a new mock instance.
func NewMocknDCActivityReplicator(ctrl *gomock.Controller) *MocknDCActivityReplicator {
	mock := &MocknDCActivityReplicator{ctrl: ctrl}
	mock.recorder = &MocknDCActivityReplicatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MocknDCActivityReplicator) EXPECT() *MocknDCActivityReplicatorMockRecorder {
	return m.recorder
}

// SyncActivity mocks base method.
func (m *MocknDCActivityReplicator) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SyncActivity", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

// SyncActivity indicates an expected call of SyncActivity.
func (mr *MocknDCActivityReplicatorMockRecorder) SyncActivity(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SyncActivity", reflect.TypeOf((*MocknDCActivityReplicator)(nil).SyncActivity), ctx, request)
}
