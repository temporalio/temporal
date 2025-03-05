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

// Code generated by MockGen. DO NOT EDIT.
// Source: scheduler.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package queues -source scheduler.go -destination scheduler_mock.go
//

// Package queues is a generated GoMock package.
package queues

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockScheduler is a mock of Scheduler interface.
type MockScheduler struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerMockRecorder
}

// MockSchedulerMockRecorder is the mock recorder for MockScheduler.
type MockSchedulerMockRecorder struct {
	mock *MockScheduler
}

// NewMockScheduler creates a new mock instance.
func NewMockScheduler(ctrl *gomock.Controller) *MockScheduler {
	mock := &MockScheduler{ctrl: ctrl}
	mock.recorder = &MockSchedulerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockScheduler) EXPECT() *MockSchedulerMockRecorder {
	return m.recorder
}

// Start mocks base method.
func (m *MockScheduler) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockSchedulerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockScheduler)(nil).Start))
}

// Stop mocks base method.
func (m *MockScheduler) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockSchedulerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockScheduler)(nil).Stop))
}

// Submit mocks base method.
func (m *MockScheduler) Submit(arg0 Executable) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Submit", arg0)
}

// Submit indicates an expected call of Submit.
func (mr *MockSchedulerMockRecorder) Submit(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockScheduler)(nil).Submit), arg0)
}

// TaskChannelKeyFn mocks base method.
func (m *MockScheduler) TaskChannelKeyFn() TaskChannelKeyFn {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TaskChannelKeyFn")
	ret0, _ := ret[0].(TaskChannelKeyFn)
	return ret0
}

// TaskChannelKeyFn indicates an expected call of TaskChannelKeyFn.
func (mr *MockSchedulerMockRecorder) TaskChannelKeyFn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TaskChannelKeyFn", reflect.TypeOf((*MockScheduler)(nil).TaskChannelKeyFn))
}

// TrySubmit mocks base method.
func (m *MockScheduler) TrySubmit(arg0 Executable) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TrySubmit", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// TrySubmit indicates an expected call of TrySubmit.
func (mr *MockSchedulerMockRecorder) TrySubmit(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrySubmit", reflect.TypeOf((*MockScheduler)(nil).TrySubmit), arg0)
}
