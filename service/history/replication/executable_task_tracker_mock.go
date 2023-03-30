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
// Source: executable_task_tracker.go

// Package replication is a generated GoMock package.
package replication

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	backoff "go.temporal.io/server/common/backoff"
	tasks "go.temporal.io/server/common/tasks"
)

// MockTrackableExecutableTask is a mock of TrackableExecutableTask interface.
type MockTrackableExecutableTask struct {
	ctrl     *gomock.Controller
	recorder *MockTrackableExecutableTaskMockRecorder
}

// MockTrackableExecutableTaskMockRecorder is the mock recorder for MockTrackableExecutableTask.
type MockTrackableExecutableTaskMockRecorder struct {
	mock *MockTrackableExecutableTask
}

// NewMockTrackableExecutableTask creates a new mock instance.
func NewMockTrackableExecutableTask(ctrl *gomock.Controller) *MockTrackableExecutableTask {
	mock := &MockTrackableExecutableTask{ctrl: ctrl}
	mock.recorder = &MockTrackableExecutableTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTrackableExecutableTask) EXPECT() *MockTrackableExecutableTaskMockRecorder {
	return m.recorder
}

// Ack mocks base method.
func (m *MockTrackableExecutableTask) Ack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Ack")
}

// Ack indicates an expected call of Ack.
func (mr *MockTrackableExecutableTaskMockRecorder) Ack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockTrackableExecutableTask)(nil).Ack))
}

// Cancel mocks base method.
func (m *MockTrackableExecutableTask) Cancel() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Cancel")
}

// Cancel indicates an expected call of Cancel.
func (mr *MockTrackableExecutableTaskMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockTrackableExecutableTask)(nil).Cancel))
}

// Execute mocks base method.
func (m *MockTrackableExecutableTask) Execute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockTrackableExecutableTaskMockRecorder) Execute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockTrackableExecutableTask)(nil).Execute))
}

// HandleErr mocks base method.
func (m *MockTrackableExecutableTask) HandleErr(err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleErr", err)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleErr indicates an expected call of HandleErr.
func (mr *MockTrackableExecutableTaskMockRecorder) HandleErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleErr", reflect.TypeOf((*MockTrackableExecutableTask)(nil).HandleErr), err)
}

// IsRetryableError mocks base method.
func (m *MockTrackableExecutableTask) IsRetryableError(err error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsRetryableError", err)
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsRetryableError indicates an expected call of IsRetryableError.
func (mr *MockTrackableExecutableTaskMockRecorder) IsRetryableError(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRetryableError", reflect.TypeOf((*MockTrackableExecutableTask)(nil).IsRetryableError), err)
}

// MarkPoisonPill mocks base method.
func (m *MockTrackableExecutableTask) MarkPoisonPill() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkPoisonPill")
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkPoisonPill indicates an expected call of MarkPoisonPill.
func (mr *MockTrackableExecutableTaskMockRecorder) MarkPoisonPill() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkPoisonPill", reflect.TypeOf((*MockTrackableExecutableTask)(nil).MarkPoisonPill))
}

// Nack mocks base method.
func (m *MockTrackableExecutableTask) Nack(err error) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Nack", err)
}

// Nack indicates an expected call of Nack.
func (mr *MockTrackableExecutableTaskMockRecorder) Nack(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nack", reflect.TypeOf((*MockTrackableExecutableTask)(nil).Nack), err)
}

// Reschedule mocks base method.
func (m *MockTrackableExecutableTask) Reschedule() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Reschedule")
}

// Reschedule indicates an expected call of Reschedule.
func (mr *MockTrackableExecutableTaskMockRecorder) Reschedule() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reschedule", reflect.TypeOf((*MockTrackableExecutableTask)(nil).Reschedule))
}

// RetryPolicy mocks base method.
func (m *MockTrackableExecutableTask) RetryPolicy() backoff.RetryPolicy {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryPolicy")
	ret0, _ := ret[0].(backoff.RetryPolicy)
	return ret0
}

// RetryPolicy indicates an expected call of RetryPolicy.
func (mr *MockTrackableExecutableTaskMockRecorder) RetryPolicy() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryPolicy", reflect.TypeOf((*MockTrackableExecutableTask)(nil).RetryPolicy))
}

// State mocks base method.
func (m *MockTrackableExecutableTask) State() tasks.State {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "State")
	ret0, _ := ret[0].(tasks.State)
	return ret0
}

// State indicates an expected call of State.
func (mr *MockTrackableExecutableTaskMockRecorder) State() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "State", reflect.TypeOf((*MockTrackableExecutableTask)(nil).State))
}

// TaskCreationTime mocks base method.
func (m *MockTrackableExecutableTask) TaskCreationTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TaskCreationTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// TaskCreationTime indicates an expected call of TaskCreationTime.
func (mr *MockTrackableExecutableTaskMockRecorder) TaskCreationTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TaskCreationTime", reflect.TypeOf((*MockTrackableExecutableTask)(nil).TaskCreationTime))
}

// TaskID mocks base method.
func (m *MockTrackableExecutableTask) TaskID() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TaskID")
	ret0, _ := ret[0].(int64)
	return ret0
}

// TaskID indicates an expected call of TaskID.
func (mr *MockTrackableExecutableTaskMockRecorder) TaskID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TaskID", reflect.TypeOf((*MockTrackableExecutableTask)(nil).TaskID))
}

// MockExecutableTaskTracker is a mock of ExecutableTaskTracker interface.
type MockExecutableTaskTracker struct {
	ctrl     *gomock.Controller
	recorder *MockExecutableTaskTrackerMockRecorder
}

// MockExecutableTaskTrackerMockRecorder is the mock recorder for MockExecutableTaskTracker.
type MockExecutableTaskTrackerMockRecorder struct {
	mock *MockExecutableTaskTracker
}

// NewMockExecutableTaskTracker creates a new mock instance.
func NewMockExecutableTaskTracker(ctrl *gomock.Controller) *MockExecutableTaskTracker {
	mock := &MockExecutableTaskTracker{ctrl: ctrl}
	mock.recorder = &MockExecutableTaskTrackerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExecutableTaskTracker) EXPECT() *MockExecutableTaskTrackerMockRecorder {
	return m.recorder
}

// Cancel mocks base method.
func (m *MockExecutableTaskTracker) Cancel() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Cancel")
}

// Cancel indicates an expected call of Cancel.
func (mr *MockExecutableTaskTrackerMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockExecutableTaskTracker)(nil).Cancel))
}

// LowWatermark mocks base method.
func (m *MockExecutableTaskTracker) LowWatermark() *WatermarkInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LowWatermark")
	ret0, _ := ret[0].(*WatermarkInfo)
	return ret0
}

// LowWatermark indicates an expected call of LowWatermark.
func (mr *MockExecutableTaskTrackerMockRecorder) LowWatermark() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LowWatermark", reflect.TypeOf((*MockExecutableTaskTracker)(nil).LowWatermark))
}

// TrackTasks mocks base method.
func (m *MockExecutableTaskTracker) TrackTasks(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) {
	m.ctrl.T.Helper()
	varargs := []interface{}{highWatermarkInfo}
	for _, a := range tasks {
		varargs = append(varargs, a)
	}
	m.ctrl.Call(m, "TrackTasks", varargs...)
}

// TrackTasks indicates an expected call of TrackTasks.
func (mr *MockExecutableTaskTrackerMockRecorder) TrackTasks(highWatermarkInfo interface{}, tasks ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{highWatermarkInfo}, tasks...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrackTasks", reflect.TypeOf((*MockExecutableTaskTracker)(nil).TrackTasks), varargs...)
}
