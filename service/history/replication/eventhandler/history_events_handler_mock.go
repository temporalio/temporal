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
// Source: history_events_handler.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../../LICENSE -package eventhandler -source history_events_handler.go -destination history_events_handler_mock.go
//

// Package eventhandler is a generated GoMock package.
package eventhandler

import (
	context "context"
	reflect "reflect"

	history "go.temporal.io/api/history/v1"
	history0 "go.temporal.io/server/api/history/v1"
	workflow "go.temporal.io/server/api/workflow/v1"
	definition "go.temporal.io/server/common/definition"
	gomock "go.uber.org/mock/gomock"
)

// MockHistoryEventsHandler is a mock of HistoryEventsHandler interface.
type MockHistoryEventsHandler struct {
	ctrl     *gomock.Controller
	recorder *MockHistoryEventsHandlerMockRecorder
}

// MockHistoryEventsHandlerMockRecorder is the mock recorder for MockHistoryEventsHandler.
type MockHistoryEventsHandlerMockRecorder struct {
	mock *MockHistoryEventsHandler
}

// NewMockHistoryEventsHandler creates a new mock instance.
func NewMockHistoryEventsHandler(ctrl *gomock.Controller) *MockHistoryEventsHandler {
	mock := &MockHistoryEventsHandler{ctrl: ctrl}
	mock.recorder = &MockHistoryEventsHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHistoryEventsHandler) EXPECT() *MockHistoryEventsHandlerMockRecorder {
	return m.recorder
}

// HandleHistoryEvents mocks base method.
func (m *MockHistoryEventsHandler) HandleHistoryEvents(ctx context.Context, sourceClusterName string, workflowKey definition.WorkflowKey, baseExecutionInfo *workflow.BaseExecutionInfo, versionHistoryItems []*history0.VersionHistoryItem, historyEvents [][]*history.HistoryEvent, newEvents []*history.HistoryEvent, newRunID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleHistoryEvents", ctx, sourceClusterName, workflowKey, baseExecutionInfo, versionHistoryItems, historyEvents, newEvents, newRunID)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleHistoryEvents indicates an expected call of HandleHistoryEvents.
func (mr *MockHistoryEventsHandlerMockRecorder) HandleHistoryEvents(ctx, sourceClusterName, workflowKey, baseExecutionInfo, versionHistoryItems, historyEvents, newEvents, newRunID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleHistoryEvents", reflect.TypeOf((*MockHistoryEventsHandler)(nil).HandleHistoryEvents), ctx, sourceClusterName, workflowKey, baseExecutionInfo, versionHistoryItems, historyEvents, newEvents, newRunID)
}
