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
// Source: historyIterator.go

// Package archiver is a generated GoMock package.
package archiver

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"

	archiverspb "github.com/temporalio/temporal/api/archiver/v1"
)

// MockHistoryIterator is a mock of HistoryIterator interface
type MockHistoryIterator struct {
	ctrl     *gomock.Controller
	recorder *MockHistoryIteratorMockRecorder
}

// MockHistoryIteratorMockRecorder is the mock recorder for MockHistoryIterator
type MockHistoryIteratorMockRecorder struct {
	mock *MockHistoryIterator
}

// NewMockHistoryIterator creates a new mock instance
func NewMockHistoryIterator(ctrl *gomock.Controller) *MockHistoryIterator {
	mock := &MockHistoryIterator{ctrl: ctrl}
	mock.recorder = &MockHistoryIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockHistoryIterator) EXPECT() *MockHistoryIteratorMockRecorder {
	return m.recorder
}

// Next mocks base method
func (m *MockHistoryIterator) Next() (*archiverspb.HistoryBlob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(*archiverspb.HistoryBlob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next
func (mr *MockHistoryIteratorMockRecorder) Next() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockHistoryIterator)(nil).Next))
}

// HasNext mocks base method
func (m *MockHistoryIterator) HasNext() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasNext")
	ret0, _ := ret[0].(bool)
	return ret0
}

// HasNext indicates an expected call of HasNext
func (mr *MockHistoryIteratorMockRecorder) HasNext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasNext", reflect.TypeOf((*MockHistoryIterator)(nil).HasNext))
}

// GetState mocks base method
func (m *MockHistoryIterator) GetState() ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetState")
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetState indicates an expected call of GetState
func (mr *MockHistoryIteratorMockRecorder) GetState() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetState", reflect.TypeOf((*MockHistoryIterator)(nil).GetState))
}
