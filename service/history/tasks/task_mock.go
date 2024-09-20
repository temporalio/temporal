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
//	mockgen -copyright_file ../../../LICENSE -package tasks -source task.go -destination task_mock.go
//

// Package tasks is a generated GoMock package.
package tasks

import (
	reflect "reflect"
	time "time"

	v1 "go.temporal.io/server/api/enums/v1"
	gomock "go.uber.org/mock/gomock"
)

// MockTask is a mock of Task interface.
type MockTask struct {
	ctrl     *gomock.Controller
	recorder *MockTaskMockRecorder
}

// MockTaskMockRecorder is the mock recorder for MockTask.
type MockTaskMockRecorder struct {
	mock *MockTask
}

// NewMockTask creates a new mock instance.
func NewMockTask(ctrl *gomock.Controller) *MockTask {
	mock := &MockTask{ctrl: ctrl}
	mock.recorder = &MockTaskMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTask) EXPECT() *MockTaskMockRecorder {
	return m.recorder
}

// GetCategory mocks base method.
func (m *MockTask) GetCategory() Category {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCategory")
	ret0, _ := ret[0].(Category)
	return ret0
}

// GetCategory indicates an expected call of GetCategory.
func (mr *MockTaskMockRecorder) GetCategory() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCategory", reflect.TypeOf((*MockTask)(nil).GetCategory))
}

// GetKey mocks base method.
func (m *MockTask) GetKey() Key {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetKey")
	ret0, _ := ret[0].(Key)
	return ret0
}

// GetKey indicates an expected call of GetKey.
func (mr *MockTaskMockRecorder) GetKey() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetKey", reflect.TypeOf((*MockTask)(nil).GetKey))
}

// GetNamespaceID mocks base method.
func (m *MockTask) GetNamespaceID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetNamespaceID indicates an expected call of GetNamespaceID.
func (mr *MockTaskMockRecorder) GetNamespaceID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceID", reflect.TypeOf((*MockTask)(nil).GetNamespaceID))
}

// GetRunID mocks base method.
func (m *MockTask) GetRunID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRunID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetRunID indicates an expected call of GetRunID.
func (mr *MockTaskMockRecorder) GetRunID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRunID", reflect.TypeOf((*MockTask)(nil).GetRunID))
}

// GetTaskID mocks base method.
func (m *MockTask) GetTaskID() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskID")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetTaskID indicates an expected call of GetTaskID.
func (mr *MockTaskMockRecorder) GetTaskID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskID", reflect.TypeOf((*MockTask)(nil).GetTaskID))
}

// GetType mocks base method.
func (m *MockTask) GetType() v1.TaskType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetType")
	ret0, _ := ret[0].(v1.TaskType)
	return ret0
}

// GetType indicates an expected call of GetType.
func (mr *MockTaskMockRecorder) GetType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetType", reflect.TypeOf((*MockTask)(nil).GetType))
}

// GetVisibilityTime mocks base method.
func (m *MockTask) GetVisibilityTime() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVisibilityTime")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// GetVisibilityTime indicates an expected call of GetVisibilityTime.
func (mr *MockTaskMockRecorder) GetVisibilityTime() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVisibilityTime", reflect.TypeOf((*MockTask)(nil).GetVisibilityTime))
}

// GetWorkflowID mocks base method.
func (m *MockTask) GetWorkflowID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkflowID indicates an expected call of GetWorkflowID.
func (mr *MockTaskMockRecorder) GetWorkflowID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowID", reflect.TypeOf((*MockTask)(nil).GetWorkflowID))
}

// SetTaskID mocks base method.
func (m *MockTask) SetTaskID(id int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTaskID", id)
}

// SetTaskID indicates an expected call of SetTaskID.
func (mr *MockTaskMockRecorder) SetTaskID(id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTaskID", reflect.TypeOf((*MockTask)(nil).SetTaskID), id)
}

// SetVisibilityTime mocks base method.
func (m *MockTask) SetVisibilityTime(timestamp time.Time) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetVisibilityTime", timestamp)
}

// SetVisibilityTime indicates an expected call of SetVisibilityTime.
func (mr *MockTaskMockRecorder) SetVisibilityTime(timestamp any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetVisibilityTime", reflect.TypeOf((*MockTask)(nil).SetVisibilityTime), timestamp)
}

// MockHasVersion is a mock of HasVersion interface.
type MockHasVersion struct {
	ctrl     *gomock.Controller
	recorder *MockHasVersionMockRecorder
}

// MockHasVersionMockRecorder is the mock recorder for MockHasVersion.
type MockHasVersionMockRecorder struct {
	mock *MockHasVersion
}

// NewMockHasVersion creates a new mock instance.
func NewMockHasVersion(ctrl *gomock.Controller) *MockHasVersion {
	mock := &MockHasVersion{ctrl: ctrl}
	mock.recorder = &MockHasVersionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHasVersion) EXPECT() *MockHasVersionMockRecorder {
	return m.recorder
}

// GetVersion mocks base method.
func (m *MockHasVersion) GetVersion() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVersion")
	ret0, _ := ret[0].(int64)
	return ret0
}

// GetVersion indicates an expected call of GetVersion.
func (mr *MockHasVersionMockRecorder) GetVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVersion", reflect.TypeOf((*MockHasVersion)(nil).GetVersion))
}

// MockHasStateMachineTaskType is a mock of HasStateMachineTaskType interface.
type MockHasStateMachineTaskType struct {
	ctrl     *gomock.Controller
	recorder *MockHasStateMachineTaskTypeMockRecorder
}

// MockHasStateMachineTaskTypeMockRecorder is the mock recorder for MockHasStateMachineTaskType.
type MockHasStateMachineTaskTypeMockRecorder struct {
	mock *MockHasStateMachineTaskType
}

// NewMockHasStateMachineTaskType creates a new mock instance.
func NewMockHasStateMachineTaskType(ctrl *gomock.Controller) *MockHasStateMachineTaskType {
	mock := &MockHasStateMachineTaskType{ctrl: ctrl}
	mock.recorder = &MockHasStateMachineTaskTypeMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHasStateMachineTaskType) EXPECT() *MockHasStateMachineTaskTypeMockRecorder {
	return m.recorder
}

// StateMachineTaskType mocks base method.
func (m *MockHasStateMachineTaskType) StateMachineTaskType() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateMachineTaskType")
	ret0, _ := ret[0].(string)
	return ret0
}

// StateMachineTaskType indicates an expected call of StateMachineTaskType.
func (mr *MockHasStateMachineTaskTypeMockRecorder) StateMachineTaskType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateMachineTaskType", reflect.TypeOf((*MockHasStateMachineTaskType)(nil).StateMachineTaskType))
}

// MockHasDestination is a mock of HasDestination interface.
type MockHasDestination struct {
	ctrl     *gomock.Controller
	recorder *MockHasDestinationMockRecorder
}

// MockHasDestinationMockRecorder is the mock recorder for MockHasDestination.
type MockHasDestinationMockRecorder struct {
	mock *MockHasDestination
}

// NewMockHasDestination creates a new mock instance.
func NewMockHasDestination(ctrl *gomock.Controller) *MockHasDestination {
	mock := &MockHasDestination{ctrl: ctrl}
	mock.recorder = &MockHasDestinationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHasDestination) EXPECT() *MockHasDestinationMockRecorder {
	return m.recorder
}

// GetDestination mocks base method.
func (m *MockHasDestination) GetDestination() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDestination")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetDestination indicates an expected call of GetDestination.
func (mr *MockHasDestinationMockRecorder) GetDestination() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDestination", reflect.TypeOf((*MockHasDestination)(nil).GetDestination))
}
