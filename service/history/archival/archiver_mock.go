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
// Source: archiver.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package archival -source archiver.go -destination archiver_mock.go
//

// Package archival is a generated GoMock package.
package archival

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockArchiver is a mock of Archiver interface.
type MockArchiver struct {
	ctrl     *gomock.Controller
	recorder *MockArchiverMockRecorder
}

// MockArchiverMockRecorder is the mock recorder for MockArchiver.
type MockArchiverMockRecorder struct {
	mock *MockArchiver
}

// NewMockArchiver creates a new mock instance.
func NewMockArchiver(ctrl *gomock.Controller) *MockArchiver {
	mock := &MockArchiver{ctrl: ctrl}
	mock.recorder = &MockArchiverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockArchiver) EXPECT() *MockArchiverMockRecorder {
	return m.recorder
}

// Archive mocks base method.
func (m *MockArchiver) Archive(arg0 context.Context, arg1 *Request) (*Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Archive", arg0, arg1)
	ret0, _ := ret[0].(*Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Archive indicates an expected call of Archive.
func (mr *MockArchiverMockRecorder) Archive(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Archive", reflect.TypeOf((*MockArchiver)(nil).Archive), arg0, arg1)
}
