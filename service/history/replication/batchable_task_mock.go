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
// Source: batchable_task.go

// Package replication is a generated GoMock package.
package replication

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	backoff "go.temporal.io/server/common/backoff"
	tasks "go.temporal.io/server/common/tasks"
)

// MockBatchableTask is a mock of BatchableTask interface.
type MockBatchableTask struct {
	ctrl     *gomock.Controller
	recorder *MockBatchableTaskMockRecorder
}

// MockBatchableTaskMockRecorder is the mock recorder for MockBatchableTask.
type MockBatchableTaskMockRecorder struct {
	mock *MockBatchableTask
}

// NewMockBatchableTask creates a new mock instance.
func NewMockBatchableTask(ctrl *gomock.Controller) *MockBatchableTask {
	mock := &MockBatchableTask{ctrl: ctrl}
	mock.recorder = &MockBatchableTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBatchableTask) EXPECT() *MockBatchableTaskMockRecorder {
	return m.recorder
}

// Abort mocks base method.
func (m *MockBatchableTask) Abort() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Abort")
}

// Abort indicates an expected call of Abort.
func (mr *MockBatchableTaskMockRecorder) Abort() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Abort", reflect.TypeOf((*MockBatchableTask)(nil).Abort))
}

// Ack mocks base method.
func (m *MockBatchableTask) Ack() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Ack")
}

// Ack indicates an expected call of Ack.
func (mr *MockBatchableTaskMockRecorder) Ack() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockBatchableTask)(nil).Ack))
}

// BatchWith mocks base method.
func (m *MockBatchableTask) BatchWith(task BatchableTask) (TrackableExecutableTask, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BatchWith", task)
	ret0, _ := ret[0].(TrackableExecutableTask)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// BatchWith indicates an expected call of BatchWith.
func (mr *MockBatchableTaskMockRecorder) BatchWith(task interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BatchWith", reflect.TypeOf((*MockBatchableTask)(nil).BatchWith), task)
}

// CanBatch mocks base method.
func (m *MockBatchableTask) CanBatch() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CanBatch")
	ret0, _ := ret[0].(bool)
	return ret0
}

// CanBatch indicates an expected call of CanBatch.
func (mr *MockBatchableTaskMockRecorder) CanBatch() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CanBatch", reflect.TypeOf((*MockBatchableTask)(nil).CanBatch))
}

// Cancel mocks base method.
func (m *MockBatchableTask) Cancel() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Cancel")
}

// Cancel indicates an expected call of Cancel.
func (mr *MockBatchableTaskMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockBatchableTask)(nil).Cancel))
}

// Execute mocks base method.
func (m *MockBatchableTask) Execute() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute")
	ret0, _ := ret[0].(error)
	return ret0
}

// Execute indicates an expected call of Execute.
func (mr *MockBatchableTaskMockRecorder) Execute() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockBatchableTask)(nil).Execute))
}

// HandleErr mocks base method.
func (m *MockBatchableTask) HandleErr(err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleErr", err)
	ret0, _ := ret[0].(error)
	return ret0
}

// HandleErr indicates an expected call of HandleErr.
func (mr *MockBatchableTaskMockRecorder) HandleErr(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleErr", reflect.TypeOf((*MockBatchableTask)(nil).HandleErr), err)
}

// IsRetryableError mocks base method.
func (m *MockBatchableTask) IsRetryableError(err error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsRetryableError", err)
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsRetryableError indicates an expected call of IsRetryableError.
func (mr *MockBatchableTaskMockRecorder) IsRetryableError(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRetryableError", reflect.TypeOf((*MockBatchableTask)(nil).IsRetryableError), err)
}

// MarkPoisonPill mocks base method.
func (m *MockBatchableTask) MarkPoisonPill() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkPoisonPill")
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkPoisonPill indicates an expected call of MarkPoisonPill.
func (mr *MockBatchableTaskMockRecorder) MarkPoisonPill() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkPoisonPill", reflect.TypeOf((*MockBatchableTask)(nil).MarkPoisonPill))
}

// MarkUnbatchable mocks base method.
func (m *MockBatchableTask) MarkUnbatchable() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "MarkUnbatchable")
}

// MarkUnbatchable indicates an expected call of MarkUnbatchable.
func (mr *MockBatchableTaskMockRecorder) MarkUnbatchable() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkUnbatchable", reflect.TypeOf((*MockBatchableTask)(nil).MarkUnbatchable))
}

// Nack mocks base method.
func (m *MockBatchableTask) Nack(err error) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Nack", err)
}

// Nack indicates an expected call of Nack.
func (mr *MockBatchableTaskMockRecorder) Nack(err interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nack", reflect.TypeOf((*MockBatchableTask)(nil).Nack), err)
}

// QueueID mocks base method.
func (m *MockBatchableTask) QueueID() interface{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueID")
	ret0, _ := ret[0].(interface{})
	return ret0
}

// QueueID indicates an expected call of QueueID.
func (mr *MockBatchableTaskMockRecorder) QueueID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueID", reflect.TypeOf((*MockBatchableTask)(nil).QueueID))
}

// Reschedule mocks base method.
func (m *MockBatchableTask) Reschedule() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Reschedule")
}

// Reschedule indicates an expected call of Reschedule.
func (mr *MockBatchableTaskMockRecorder) Reschedule() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reschedule", reflect.TypeOf((*MockBatchableTask)(nil).Reschedule))
}

// RetryPolicy mocks base method.
func (m *MockBatchableTask) RetryPolicy() backoff.RetryPolicy {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RetryPolicy")
	ret0, _ := ret[0].(backoff.RetryPolicy)
	return ret0
}

// RetryPolicy indicates an expected call of RetryPolicy.
func (mr *MockBatchableTaskMockRecorder) RetryPolicy() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RetryPolicy", reflect.TypeOf((*MockBatchableTask)(nil).RetryPolicy))
}

// SourceClusterName mocks base method.
func (m *MockBatchableTask) SourceClusterName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SourceClusterName")
	ret0, _ := ret[0].(string)
	return ret0
}

// SourceClusterName indicates an expected call of SourceClusterName.
func (mr *MockBatchableTaskMockRecorder) SourceClusterName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SourceClusterName", reflect.TypeOf((*MockBatchableTask)(nil).SourceClusterName))
}

// State mocks base method.
func (m *MockBatchableTask) State() tasks.State {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "State")
	ret0, _ := ret[0].(tasks.State)
	return ret0
}

// State indicates an expected call of State.
func (mr *MockBatchableTaskMockRecorder) State() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "State", reflect.TypeOf((*MockBatchableTask)(nil).State))
}

// TaskCreationTime mocks base method.
func (m *MockBatchableTask) TaskCreationTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TaskCreationTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// TaskCreationTime indicates an expected call of TaskCreationTime.
func (mr *MockBatchableTaskMockRecorder) TaskCreationTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TaskCreationTime", reflect.TypeOf((*MockBatchableTask)(nil).TaskCreationTime))
}

// TaskID mocks base method.
func (m *MockBatchableTask) TaskID() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TaskID")
	ret0, _ := ret[0].(int64)
	return ret0
}

// TaskID indicates an expected call of TaskID.
func (mr *MockBatchableTaskMockRecorder) TaskID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TaskID", reflect.TypeOf((*MockBatchableTask)(nil).TaskID))
}
