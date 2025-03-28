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
// Source: scheduler.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package tasks -source scheduler.go -destination scheduler_mock.go
//

// Package tasks is a generated GoMock package.
package tasks

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockScheduler is a mock of Scheduler interface.
type MockScheduler[T Task] struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerMockRecorder[T]
	isgomock struct{}
}

// MockSchedulerMockRecorder is the mock recorder for MockScheduler.
type MockSchedulerMockRecorder[T Task] struct {
	mock *MockScheduler[T]
}

// NewMockScheduler creates a new mock instance.
func NewMockScheduler[T Task](ctrl *gomock.Controller) *MockScheduler[T] {
	mock := &MockScheduler[T]{ctrl: ctrl}
	mock.recorder = &MockSchedulerMockRecorder[T]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockScheduler[T]) EXPECT() *MockSchedulerMockRecorder[T] {
	return m.recorder
}

// Start mocks base method.
func (m *MockScheduler[T]) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockSchedulerMockRecorder[T]) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockScheduler[T])(nil).Start))
}

// Stop mocks base method.
func (m *MockScheduler[T]) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockSchedulerMockRecorder[T]) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockScheduler[T])(nil).Stop))
}

// Submit mocks base method.
func (m *MockScheduler[T]) Submit(task T) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Submit", task)
}

// Submit indicates an expected call of Submit.
func (mr *MockSchedulerMockRecorder[T]) Submit(task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Submit", reflect.TypeOf((*MockScheduler[T])(nil).Submit), task)
}

// TrySubmit mocks base method.
func (m *MockScheduler[T]) TrySubmit(task T) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TrySubmit", task)
	ret0, _ := ret[0].(bool)
	return ret0
}

// TrySubmit indicates an expected call of TrySubmit.
func (mr *MockSchedulerMockRecorder[T]) TrySubmit(task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrySubmit", reflect.TypeOf((*MockScheduler[T])(nil).TrySubmit), task)
}
