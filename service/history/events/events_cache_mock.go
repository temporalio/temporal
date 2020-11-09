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
// Source: cache.go

// Package events is a generated GoMock package.
package events

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	history "go.temporal.io/api/history/v1"
)

// MockCache is a mock of Cache interface.
type MockCache struct {
	ctrl     *gomock.Controller
	recorder *MockCacheMockRecorder
}

// MockCacheMockRecorder is the mock recorder for MockCache.
type MockCacheMockRecorder struct {
	mock *MockCache
}

// NewMockCache creates a new mock instance.
func NewMockCache(ctrl *gomock.Controller) *MockCache {
	mock := &MockCache{ctrl: ctrl}
	mock.recorder = &MockCacheMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCache) EXPECT() *MockCacheMockRecorder {
	return m.recorder
}

// GetEvent mocks base method.
func (m *MockCache) GetEvent(namespaceID, workflowID, runID string, firstEventID, eventID int64, branchToken []byte) (*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEvent", namespaceID, workflowID, runID, firstEventID, eventID, branchToken)
	ret0, _ := ret[0].(*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetEvent indicates an expected call of GetEvent.
func (mr *MockCacheMockRecorder) GetEvent(namespaceID, workflowID, runID, firstEventID, eventID, branchToken interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEvent", reflect.TypeOf((*MockCache)(nil).GetEvent), namespaceID, workflowID, runID, firstEventID, eventID, branchToken)
}

// PutEvent mocks base method.
func (m *MockCache) PutEvent(namespaceID, workflowID, runID string, eventID int64, event *history.HistoryEvent) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "PutEvent", namespaceID, workflowID, runID, eventID, event)
}

// PutEvent indicates an expected call of PutEvent.
func (mr *MockCacheMockRecorder) PutEvent(namespaceID, workflowID, runID, eventID, event interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutEvent", reflect.TypeOf((*MockCache)(nil).PutEvent), namespaceID, workflowID, runID, eventID, event)
}

// DeleteEvent mocks base method.
func (m *MockCache) DeleteEvent(namespaceID, workflowID, runID string, eventID int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteEvent", namespaceID, workflowID, runID, eventID)
}

// DeleteEvent indicates an expected call of DeleteEvent.
func (mr *MockCacheMockRecorder) DeleteEvent(namespaceID, workflowID, runID, eventID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteEvent", reflect.TypeOf((*MockCache)(nil).DeleteEvent), namespaceID, workflowID, runID, eventID)
}
