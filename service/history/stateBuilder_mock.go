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
// Source: stateBuilder.go

// Package history is a generated GoMock package.
package history

import (
	gomock "github.com/golang/mock/gomock"
	common "go.temporal.io/temporal-proto/common/v1"
	history "go.temporal.io/temporal-proto/history/v1"
	reflect "reflect"
)

// MockstateBuilder is a mock of stateBuilder interface
type MockstateBuilder struct {
	ctrl     *gomock.Controller
	recorder *MockstateBuilderMockRecorder
}

// MockstateBuilderMockRecorder is the mock recorder for MockstateBuilder
type MockstateBuilderMockRecorder struct {
	mock *MockstateBuilder
}

// NewMockstateBuilder creates a new mock instance
func NewMockstateBuilder(ctrl *gomock.Controller) *MockstateBuilder {
	mock := &MockstateBuilder{ctrl: ctrl}
	mock.recorder = &MockstateBuilderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockstateBuilder) EXPECT() *MockstateBuilderMockRecorder {
	return m.recorder
}

// applyEvents mocks base method
func (m *MockstateBuilder) applyEvents(namespaceID, requestID string, execution common.WorkflowExecution, history, newRunHistory []*history.HistoryEvent, newRunNDC bool) (mutableState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "applyEvents", namespaceID, requestID, execution, history, newRunHistory, newRunNDC)
	ret0, _ := ret[0].(mutableState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// applyEvents indicates an expected call of applyEvents
func (mr *MockstateBuilderMockRecorder) applyEvents(namespaceID, requestID, execution, history, newRunHistory, newRunNDC interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "applyEvents", reflect.TypeOf((*MockstateBuilder)(nil).applyEvents), namespaceID, requestID, execution, history, newRunHistory, newRunNDC)
}
