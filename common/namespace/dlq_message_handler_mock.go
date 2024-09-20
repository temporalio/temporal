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
// Source: dlq_message_handler.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package namespace -source dlq_message_handler.go -destination dlq_message_handler_mock.go
//

// Package namespace is a generated GoMock package.
package namespace

import (
	context "context"
	reflect "reflect"

	repication "go.temporal.io/server/api/replication/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockDLQMessageHandler is a mock of DLQMessageHandler interface.
type MockDLQMessageHandler struct {
	ctrl     *gomock.Controller
	recorder *MockDLQMessageHandlerMockRecorder
}

// MockDLQMessageHandlerMockRecorder is the mock recorder for MockDLQMessageHandler.
type MockDLQMessageHandlerMockRecorder struct {
	mock *MockDLQMessageHandler
}

// NewMockDLQMessageHandler creates a new mock instance.
func NewMockDLQMessageHandler(ctrl *gomock.Controller) *MockDLQMessageHandler {
	mock := &MockDLQMessageHandler{ctrl: ctrl}
	mock.recorder = &MockDLQMessageHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDLQMessageHandler) EXPECT() *MockDLQMessageHandlerMockRecorder {
	return m.recorder
}

// Merge mocks base method.
func (m *MockDLQMessageHandler) Merge(ctx context.Context, lastMessageID int64, pageSize int, pageToken []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Merge", ctx, lastMessageID, pageSize, pageToken)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Merge indicates an expected call of Merge.
func (mr *MockDLQMessageHandlerMockRecorder) Merge(ctx, lastMessageID, pageSize, pageToken any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Merge", reflect.TypeOf((*MockDLQMessageHandler)(nil).Merge), ctx, lastMessageID, pageSize, pageToken)
}

// Purge mocks base method.
func (m *MockDLQMessageHandler) Purge(ctx context.Context, lastMessageID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Purge", ctx, lastMessageID)
	ret0, _ := ret[0].(error)
	return ret0
}

// Purge indicates an expected call of Purge.
func (mr *MockDLQMessageHandlerMockRecorder) Purge(ctx, lastMessageID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Purge", reflect.TypeOf((*MockDLQMessageHandler)(nil).Purge), ctx, lastMessageID)
}

// Read mocks base method.
func (m *MockDLQMessageHandler) Read(ctx context.Context, lastMessageID int64, pageSize int, pageToken []byte) ([]*repication.ReplicationTask, []byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", ctx, lastMessageID, pageSize, pageToken)
	ret0, _ := ret[0].([]*repication.ReplicationTask)
	ret1, _ := ret[1].([]byte)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Read indicates an expected call of Read.
func (mr *MockDLQMessageHandlerMockRecorder) Read(ctx, lastMessageID, pageSize, pageToken any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockDLQMessageHandler)(nil).Read), ctx, lastMessageID, pageSize, pageToken)
}
