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
// Source: controller.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package shard -source controller.go -destination controller_mock.go
//

// Package shard is a generated GoMock package.
package shard

import (
	context "context"
	reflect "reflect"

	namespace "go.temporal.io/server/common/namespace"
	pingable "go.temporal.io/server/common/pingable"
	interfaces "go.temporal.io/server/service/history/interfaces"
	gomock "go.uber.org/mock/gomock"
)

// MockController is a mock of Controller interface.
type MockController struct {
	ctrl     *gomock.Controller
	recorder *MockControllerMockRecorder
	isgomock struct{}
}

// MockControllerMockRecorder is the mock recorder for MockController.
type MockControllerMockRecorder struct {
	mock *MockController
}

// NewMockController creates a new mock instance.
func NewMockController(ctrl *gomock.Controller) *MockController {
	mock := &MockController{ctrl: ctrl}
	mock.recorder = &MockControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockController) EXPECT() *MockControllerMockRecorder {
	return m.recorder
}

// CloseShardByID mocks base method.
func (m *MockController) CloseShardByID(shardID int32) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CloseShardByID", shardID)
}

// CloseShardByID indicates an expected call of CloseShardByID.
func (mr *MockControllerMockRecorder) CloseShardByID(shardID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseShardByID", reflect.TypeOf((*MockController)(nil).CloseShardByID), shardID)
}

// GetPingChecks mocks base method.
func (m *MockController) GetPingChecks() []pingable.Check {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPingChecks")
	ret0, _ := ret[0].([]pingable.Check)
	return ret0
}

// GetPingChecks indicates an expected call of GetPingChecks.
func (mr *MockControllerMockRecorder) GetPingChecks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPingChecks", reflect.TypeOf((*MockController)(nil).GetPingChecks))
}

// GetShardByID mocks base method.
func (m *MockController) GetShardByID(shardID int32) (interfaces.ShardContext, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardByID", shardID)
	ret0, _ := ret[0].(interfaces.ShardContext)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetShardByID indicates an expected call of GetShardByID.
func (mr *MockControllerMockRecorder) GetShardByID(shardID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardByID", reflect.TypeOf((*MockController)(nil).GetShardByID), shardID)
}

// GetShardByNamespaceWorkflow mocks base method.
func (m *MockController) GetShardByNamespaceWorkflow(namespaceID namespace.ID, workflowID string) (interfaces.ShardContext, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardByNamespaceWorkflow", namespaceID, workflowID)
	ret0, _ := ret[0].(interfaces.ShardContext)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetShardByNamespaceWorkflow indicates an expected call of GetShardByNamespaceWorkflow.
func (mr *MockControllerMockRecorder) GetShardByNamespaceWorkflow(namespaceID, workflowID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardByNamespaceWorkflow", reflect.TypeOf((*MockController)(nil).GetShardByNamespaceWorkflow), namespaceID, workflowID)
}

// InitialShardsAcquired mocks base method.
func (m *MockController) InitialShardsAcquired(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitialShardsAcquired", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// InitialShardsAcquired indicates an expected call of InitialShardsAcquired.
func (mr *MockControllerMockRecorder) InitialShardsAcquired(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitialShardsAcquired", reflect.TypeOf((*MockController)(nil).InitialShardsAcquired), arg0)
}

// ShardIDs mocks base method.
func (m *MockController) ShardIDs() []int32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardIDs")
	ret0, _ := ret[0].([]int32)
	return ret0
}

// ShardIDs indicates an expected call of ShardIDs.
func (mr *MockControllerMockRecorder) ShardIDs() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardIDs", reflect.TypeOf((*MockController)(nil).ShardIDs))
}

// Start mocks base method.
func (m *MockController) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockControllerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockController)(nil).Start))
}

// Stop mocks base method.
func (m *MockController) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockControllerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockController)(nil).Stop))
}
