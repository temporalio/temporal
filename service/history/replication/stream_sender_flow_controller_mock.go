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
// Source: stream_sender_flow_controller.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package replication -source stream_sender_flow_controller.go -destination stream_sender_flow_controller_mock.go
//

// Package replication is a generated GoMock package.
package replication

import (
	reflect "reflect"

	enums "go.temporal.io/server/api/enums/v1"
	repication "go.temporal.io/server/api/replication/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockSenderFlowController is a mock of SenderFlowController interface.
type MockSenderFlowController struct {
	ctrl     *gomock.Controller
	recorder *MockSenderFlowControllerMockRecorder
}

// MockSenderFlowControllerMockRecorder is the mock recorder for MockSenderFlowController.
type MockSenderFlowControllerMockRecorder struct {
	mock *MockSenderFlowController
}

// NewMockSenderFlowController creates a new mock instance.
func NewMockSenderFlowController(ctrl *gomock.Controller) *MockSenderFlowController {
	mock := &MockSenderFlowController{ctrl: ctrl}
	mock.recorder = &MockSenderFlowControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSenderFlowController) EXPECT() *MockSenderFlowControllerMockRecorder {
	return m.recorder
}

// RefreshReceiverFlowControlInfo mocks base method.
func (m *MockSenderFlowController) RefreshReceiverFlowControlInfo(syncState *repication.SyncReplicationState) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RefreshReceiverFlowControlInfo", syncState)
}

// RefreshReceiverFlowControlInfo indicates an expected call of RefreshReceiverFlowControlInfo.
func (mr *MockSenderFlowControllerMockRecorder) RefreshReceiverFlowControlInfo(syncState any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RefreshReceiverFlowControlInfo", reflect.TypeOf((*MockSenderFlowController)(nil).RefreshReceiverFlowControlInfo), syncState)
}

// Wait mocks base method.
func (m *MockSenderFlowController) Wait(priority enums.TaskPriority) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Wait", priority)
}

// Wait indicates an expected call of Wait.
func (mr *MockSenderFlowControllerMockRecorder) Wait(priority any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Wait", reflect.TypeOf((*MockSenderFlowController)(nil).Wait), priority)
}
