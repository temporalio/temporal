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
// Source: task.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../LICENSE -package chasm -source task.go -destination task_mock.go
//

// Package chasm is a generated GoMock package.
package chasm

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockTaskHandler is a mock of TaskHandler interface.
type MockTaskHandler[C any, T any] struct {
	ctrl     *gomock.Controller
	recorder *MockTaskHandlerMockRecorder[C, T]
}

// MockTaskHandlerMockRecorder is the mock recorder for MockTaskHandler.
type MockTaskHandlerMockRecorder[C any, T any] struct {
	mock *MockTaskHandler[C, T]
}

// NewMockTaskHandler creates a new mock instance.
func NewMockTaskHandler[C any, T any](ctrl *gomock.Controller) *MockTaskHandler[C, T] {
	mock := &MockTaskHandler[C, T]{ctrl: ctrl}
	mock.recorder = &MockTaskHandlerMockRecorder[C, T]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTaskHandler[C, T]) EXPECT() *MockTaskHandlerMockRecorder[C, T] {
	return m.recorder
}

// Execute mocks base method.
func (m *MockTaskHandler[C, T]) Execute(arg0 context.Context, arg1 ComponentRef, arg2 T) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockTaskHandlerMockRecorder[C, T]) Execute(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockTaskHandler[C, T])(nil).Execute), arg0, arg1, arg2)
}

// Validate mocks base method.
func (m *MockTaskHandler[C, T]) Validate(arg0 Context, arg1 C, arg2 T) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Validate", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Validate indicates an expected call of Validate.
func (mr *MockTaskHandlerMockRecorder[C, T]) Validate(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Validate", reflect.TypeOf((*MockTaskHandler[C, T])(nil).Validate), arg0, arg1, arg2)
}
