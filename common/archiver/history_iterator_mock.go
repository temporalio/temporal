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
// Source: history_iterator.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package archiver -source history_iterator.go -destination history_iterator_mock.go
//

// Package archiver is a generated GoMock package.
package archiver

import (
	context "context"
	reflect "reflect"

	archiver "go.temporal.io/server/api/archiver/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockHistoryIterator is a mock of HistoryIterator interface.
type MockHistoryIterator struct {
	ctrl     *gomock.Controller
	recorder *MockHistoryIteratorMockRecorder
}

// MockHistoryIteratorMockRecorder is the mock recorder for MockHistoryIterator.
type MockHistoryIteratorMockRecorder struct {
	mock *MockHistoryIterator
}

// NewMockHistoryIterator creates a new mock instance.
func NewMockHistoryIterator(ctrl *gomock.Controller) *MockHistoryIterator {
	mock := &MockHistoryIterator{ctrl: ctrl}
	mock.recorder = &MockHistoryIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHistoryIterator) EXPECT() *MockHistoryIteratorMockRecorder {
	return m.recorder
}

// GetState mocks base method.
func (m *MockHistoryIterator) GetState() ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetState")
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetState indicates an expected call of GetState.
func (mr *MockHistoryIteratorMockRecorder) GetState() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetState", reflect.TypeOf((*MockHistoryIterator)(nil).GetState))
}

// HasNext mocks base method.
func (m *MockHistoryIterator) HasNext() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasNext")
	ret0, _ := ret[0].(bool)
	return ret0
}

// HasNext indicates an expected call of HasNext.
func (mr *MockHistoryIteratorMockRecorder) HasNext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasNext", reflect.TypeOf((*MockHistoryIterator)(nil).HasNext))
}

// Next mocks base method.
func (m *MockHistoryIterator) Next(arg0 context.Context) (*archiver.HistoryBlob, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", arg0)
	ret0, _ := ret[0].(*archiver.HistoryBlob)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next.
func (mr *MockHistoryIteratorMockRecorder) Next(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockHistoryIterator)(nil).Next), arg0)
}

// MockSizeEstimator is a mock of SizeEstimator interface.
type MockSizeEstimator struct {
	ctrl     *gomock.Controller
	recorder *MockSizeEstimatorMockRecorder
}

// MockSizeEstimatorMockRecorder is the mock recorder for MockSizeEstimator.
type MockSizeEstimatorMockRecorder struct {
	mock *MockSizeEstimator
}

// NewMockSizeEstimator creates a new mock instance.
func NewMockSizeEstimator(ctrl *gomock.Controller) *MockSizeEstimator {
	mock := &MockSizeEstimator{ctrl: ctrl}
	mock.recorder = &MockSizeEstimatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSizeEstimator) EXPECT() *MockSizeEstimatorMockRecorder {
	return m.recorder
}

// EstimateSize mocks base method.
func (m *MockSizeEstimator) EstimateSize(v any) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EstimateSize", v)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// EstimateSize indicates an expected call of EstimateSize.
func (mr *MockSizeEstimatorMockRecorder) EstimateSize(v any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EstimateSize", reflect.TypeOf((*MockSizeEstimator)(nil).EstimateSize), v)
}
