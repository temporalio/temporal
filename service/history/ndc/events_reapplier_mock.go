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
// Source: events_reapplier.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package ndc -source events_reapplier.go -destination events_reapplier_mock.go
//

// Package ndc is a generated GoMock package.
package ndc

import (
	context "context"
	reflect "reflect"

	history "go.temporal.io/api/history/v1"
	workflow "go.temporal.io/server/service/history/workflow"
	update "go.temporal.io/server/service/history/workflow/update"
	gomock "go.uber.org/mock/gomock"
)

// MockEventsReapplier is a mock of EventsReapplier interface.
type MockEventsReapplier struct {
	ctrl     *gomock.Controller
	recorder *MockEventsReapplierMockRecorder
}

// MockEventsReapplierMockRecorder is the mock recorder for MockEventsReapplier.
type MockEventsReapplierMockRecorder struct {
	mock *MockEventsReapplier
}

// NewMockEventsReapplier creates a new mock instance.
func NewMockEventsReapplier(ctrl *gomock.Controller) *MockEventsReapplier {
	mock := &MockEventsReapplier{ctrl: ctrl}
	mock.recorder = &MockEventsReapplierMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventsReapplier) EXPECT() *MockEventsReapplierMockRecorder {
	return m.recorder
}

// ReapplyEvents mocks base method.
func (m *MockEventsReapplier) ReapplyEvents(ctx context.Context, ms workflow.MutableState, updateRegistry update.Registry, historyEvents []*history.HistoryEvent, runID string) ([]*history.HistoryEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReapplyEvents", ctx, ms, updateRegistry, historyEvents, runID)
	ret0, _ := ret[0].([]*history.HistoryEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReapplyEvents indicates an expected call of ReapplyEvents.
func (mr *MockEventsReapplierMockRecorder) ReapplyEvents(ctx, ms, updateRegistry, historyEvents, runID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReapplyEvents", reflect.TypeOf((*MockEventsReapplier)(nil).ReapplyEvents), ctx, ms, updateRegistry, historyEvents, runID)
}
