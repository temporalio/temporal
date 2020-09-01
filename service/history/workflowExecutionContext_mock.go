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
// Source: workflowExecutionContext.go

// Package history is a generated GoMock package.
package history

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	common "go.temporal.io/api/common/v1"
	persistenceblobs "go.temporal.io/server/api/persistenceblobs/v1"
	persistence "go.temporal.io/server/common/persistence"
)

// MockworkflowExecutionContext is a mock of workflowExecutionContext interface.
type MockworkflowExecutionContext struct {
	ctrl     *gomock.Controller
	recorder *MockworkflowExecutionContextMockRecorder
}

// MockworkflowExecutionContextMockRecorder is the mock recorder for MockworkflowExecutionContext.
type MockworkflowExecutionContextMockRecorder struct {
	mock *MockworkflowExecutionContext
}

// NewMockworkflowExecutionContext creates a new mock instance.
func NewMockworkflowExecutionContext(ctrl *gomock.Controller) *MockworkflowExecutionContext {
	mock := &MockworkflowExecutionContext{ctrl: ctrl}
	mock.recorder = &MockworkflowExecutionContextMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockworkflowExecutionContext) EXPECT() *MockworkflowExecutionContextMockRecorder {
	return m.recorder
}

// getNamespace mocks base method.
func (m *MockworkflowExecutionContext) getNamespace() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getNamespace")
	ret0, _ := ret[0].(string)
	return ret0
}

// getNamespace indicates an expected call of getNamespace.
func (mr *MockworkflowExecutionContextMockRecorder) getNamespace() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getNamespace", reflect.TypeOf((*MockworkflowExecutionContext)(nil).getNamespace))
}

// getNamespaceID mocks base method.
func (m *MockworkflowExecutionContext) getNamespaceID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getNamespaceID")
	ret0, _ := ret[0].(string)
	return ret0
}

// getNamespaceID indicates an expected call of getNamespaceID.
func (mr *MockworkflowExecutionContextMockRecorder) getNamespaceID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getNamespaceID", reflect.TypeOf((*MockworkflowExecutionContext)(nil).getNamespaceID))
}

// getExecution mocks base method.
func (m *MockworkflowExecutionContext) getExecution() *common.WorkflowExecution {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getExecution")
	ret0, _ := ret[0].(*common.WorkflowExecution)
	return ret0
}

// getExecution indicates an expected call of getExecution.
func (mr *MockworkflowExecutionContextMockRecorder) getExecution() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getExecution", reflect.TypeOf((*MockworkflowExecutionContext)(nil).getExecution))
}

// loadWorkflowExecution mocks base method.
func (m *MockworkflowExecutionContext) loadWorkflowExecution() (mutableState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "loadWorkflowExecution")
	ret0, _ := ret[0].(mutableState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// loadWorkflowExecution indicates an expected call of loadWorkflowExecution.
func (mr *MockworkflowExecutionContextMockRecorder) loadWorkflowExecution() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "loadWorkflowExecution", reflect.TypeOf((*MockworkflowExecutionContext)(nil).loadWorkflowExecution))
}

// loadWorkflowExecutionForReplication mocks base method.
func (m *MockworkflowExecutionContext) loadWorkflowExecutionForReplication(incomingVersion int64) (mutableState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "loadWorkflowExecutionForReplication", incomingVersion)
	ret0, _ := ret[0].(mutableState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// loadWorkflowExecutionForReplication indicates an expected call of loadWorkflowExecutionForReplication.
func (mr *MockworkflowExecutionContextMockRecorder) loadWorkflowExecutionForReplication(incomingVersion interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "loadWorkflowExecutionForReplication", reflect.TypeOf((*MockworkflowExecutionContext)(nil).loadWorkflowExecutionForReplication), incomingVersion)
}

// loadExecutionStats mocks base method.
func (m *MockworkflowExecutionContext) loadExecutionStats() (*persistenceblobs.ExecutionStats, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "loadExecutionStats")
	ret0, _ := ret[0].(*persistenceblobs.ExecutionStats)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// loadExecutionStats indicates an expected call of loadExecutionStats.
func (mr *MockworkflowExecutionContextMockRecorder) loadExecutionStats() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "loadExecutionStats", reflect.TypeOf((*MockworkflowExecutionContext)(nil).loadExecutionStats))
}

// clear mocks base method.
func (m *MockworkflowExecutionContext) clear() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "clear")
}

// clear indicates an expected call of clear.
func (mr *MockworkflowExecutionContextMockRecorder) clear() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "clear", reflect.TypeOf((*MockworkflowExecutionContext)(nil).clear))
}

// lock mocks base method.
func (m *MockworkflowExecutionContext) lock(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "lock", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// lock indicates an expected call of lock.
func (mr *MockworkflowExecutionContextMockRecorder) lock(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "lock", reflect.TypeOf((*MockworkflowExecutionContext)(nil).lock), ctx)
}

// unlock mocks base method.
func (m *MockworkflowExecutionContext) unlock() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "unlock")
}

// unlock indicates an expected call of unlock.
func (mr *MockworkflowExecutionContextMockRecorder) unlock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "unlock", reflect.TypeOf((*MockworkflowExecutionContext)(nil).unlock))
}

// getHistorySize mocks base method.
func (m *MockworkflowExecutionContext) getHistorySize() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getHistorySize")
	ret0, _ := ret[0].(int64)
	return ret0
}

// getHistorySize indicates an expected call of getHistorySize.
func (mr *MockworkflowExecutionContextMockRecorder) getHistorySize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getHistorySize", reflect.TypeOf((*MockworkflowExecutionContext)(nil).getHistorySize))
}

// setHistorySize mocks base method.
func (m *MockworkflowExecutionContext) setHistorySize(size int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "setHistorySize", size)
}

// setHistorySize indicates an expected call of setHistorySize.
func (mr *MockworkflowExecutionContextMockRecorder) setHistorySize(size interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "setHistorySize", reflect.TypeOf((*MockworkflowExecutionContext)(nil).setHistorySize), size)
}

// reapplyEvents mocks base method.
func (m *MockworkflowExecutionContext) reapplyEvents(eventBatches []*persistence.WorkflowEvents) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "reapplyEvents", eventBatches)
	ret0, _ := ret[0].(error)
	return ret0
}

// reapplyEvents indicates an expected call of reapplyEvents.
func (mr *MockworkflowExecutionContextMockRecorder) reapplyEvents(eventBatches interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "reapplyEvents", reflect.TypeOf((*MockworkflowExecutionContext)(nil).reapplyEvents), eventBatches)
}

// persistFirstWorkflowEvents mocks base method.
func (m *MockworkflowExecutionContext) persistFirstWorkflowEvents(workflowEvents *persistence.WorkflowEvents) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "persistFirstWorkflowEvents", workflowEvents)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// persistFirstWorkflowEvents indicates an expected call of persistFirstWorkflowEvents.
func (mr *MockworkflowExecutionContextMockRecorder) persistFirstWorkflowEvents(workflowEvents interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "persistFirstWorkflowEvents", reflect.TypeOf((*MockworkflowExecutionContext)(nil).persistFirstWorkflowEvents), workflowEvents)
}

// persistNonFirstWorkflowEvents mocks base method.
func (m *MockworkflowExecutionContext) persistNonFirstWorkflowEvents(workflowEvents *persistence.WorkflowEvents) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "persistNonFirstWorkflowEvents", workflowEvents)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// persistNonFirstWorkflowEvents indicates an expected call of persistNonFirstWorkflowEvents.
func (mr *MockworkflowExecutionContextMockRecorder) persistNonFirstWorkflowEvents(workflowEvents interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "persistNonFirstWorkflowEvents", reflect.TypeOf((*MockworkflowExecutionContext)(nil).persistNonFirstWorkflowEvents), workflowEvents)
}

// createWorkflowExecution mocks base method.
func (m *MockworkflowExecutionContext) createWorkflowExecution(newWorkflow *persistence.WorkflowSnapshot, historySize int64, now time.Time, createMode persistence.CreateWorkflowMode, prevRunID string, prevLastWriteVersion int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "createWorkflowExecution", newWorkflow, historySize, now, createMode, prevRunID, prevLastWriteVersion)
	ret0, _ := ret[0].(error)
	return ret0
}

// createWorkflowExecution indicates an expected call of createWorkflowExecution.
func (mr *MockworkflowExecutionContextMockRecorder) createWorkflowExecution(newWorkflow, historySize, now, createMode, prevRunID, prevLastWriteVersion interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "createWorkflowExecution", reflect.TypeOf((*MockworkflowExecutionContext)(nil).createWorkflowExecution), newWorkflow, historySize, now, createMode, prevRunID, prevLastWriteVersion)
}

// conflictResolveWorkflowExecution mocks base method.
func (m *MockworkflowExecutionContext) conflictResolveWorkflowExecution(now time.Time, conflictResolveMode persistence.ConflictResolveWorkflowMode, resetMutableState mutableState, newContext workflowExecutionContext, newMutableState mutableState, currentContext workflowExecutionContext, currentMutableState mutableState, currentTransactionPolicy *transactionPolicy, workflowCAS *persistence.CurrentWorkflowCAS) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "conflictResolveWorkflowExecution", now, conflictResolveMode, resetMutableState, newContext, newMutableState, currentContext, currentMutableState, currentTransactionPolicy, workflowCAS)
	ret0, _ := ret[0].(error)
	return ret0
}

// conflictResolveWorkflowExecution indicates an expected call of conflictResolveWorkflowExecution.
func (mr *MockworkflowExecutionContextMockRecorder) conflictResolveWorkflowExecution(now, conflictResolveMode, resetMutableState, newContext, newMutableState, currentContext, currentMutableState, currentTransactionPolicy, workflowCAS interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "conflictResolveWorkflowExecution", reflect.TypeOf((*MockworkflowExecutionContext)(nil).conflictResolveWorkflowExecution), now, conflictResolveMode, resetMutableState, newContext, newMutableState, currentContext, currentMutableState, currentTransactionPolicy, workflowCAS)
}

// updateWorkflowExecutionAsActive mocks base method.
func (m *MockworkflowExecutionContext) updateWorkflowExecutionAsActive(now time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflowExecutionAsActive", now)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflowExecutionAsActive indicates an expected call of updateWorkflowExecutionAsActive.
func (mr *MockworkflowExecutionContextMockRecorder) updateWorkflowExecutionAsActive(now interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflowExecutionAsActive", reflect.TypeOf((*MockworkflowExecutionContext)(nil).updateWorkflowExecutionAsActive), now)
}

// updateWorkflowExecutionWithNewAsActive mocks base method.
func (m *MockworkflowExecutionContext) updateWorkflowExecutionWithNewAsActive(now time.Time, newContext workflowExecutionContext, newMutableState mutableState) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflowExecutionWithNewAsActive", now, newContext, newMutableState)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflowExecutionWithNewAsActive indicates an expected call of updateWorkflowExecutionWithNewAsActive.
func (mr *MockworkflowExecutionContextMockRecorder) updateWorkflowExecutionWithNewAsActive(now, newContext, newMutableState interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflowExecutionWithNewAsActive", reflect.TypeOf((*MockworkflowExecutionContext)(nil).updateWorkflowExecutionWithNewAsActive), now, newContext, newMutableState)
}

// updateWorkflowExecutionAsPassive mocks base method.
func (m *MockworkflowExecutionContext) updateWorkflowExecutionAsPassive(now time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflowExecutionAsPassive", now)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflowExecutionAsPassive indicates an expected call of updateWorkflowExecutionAsPassive.
func (mr *MockworkflowExecutionContextMockRecorder) updateWorkflowExecutionAsPassive(now interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflowExecutionAsPassive", reflect.TypeOf((*MockworkflowExecutionContext)(nil).updateWorkflowExecutionAsPassive), now)
}

// updateWorkflowExecutionWithNewAsPassive mocks base method.
func (m *MockworkflowExecutionContext) updateWorkflowExecutionWithNewAsPassive(now time.Time, newContext workflowExecutionContext, newMutableState mutableState) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflowExecutionWithNewAsPassive", now, newContext, newMutableState)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflowExecutionWithNewAsPassive indicates an expected call of updateWorkflowExecutionWithNewAsPassive.
func (mr *MockworkflowExecutionContextMockRecorder) updateWorkflowExecutionWithNewAsPassive(now, newContext, newMutableState interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflowExecutionWithNewAsPassive", reflect.TypeOf((*MockworkflowExecutionContext)(nil).updateWorkflowExecutionWithNewAsPassive), now, newContext, newMutableState)
}

// updateWorkflowExecutionWithNew mocks base method.
func (m *MockworkflowExecutionContext) updateWorkflowExecutionWithNew(now time.Time, updateMode persistence.UpdateWorkflowMode, newContext workflowExecutionContext, newMutableState mutableState, currentWorkflowTransactionPolicy transactionPolicy, newWorkflowTransactionPolicy *transactionPolicy) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "updateWorkflowExecutionWithNew", now, updateMode, newContext, newMutableState, currentWorkflowTransactionPolicy, newWorkflowTransactionPolicy)
	ret0, _ := ret[0].(error)
	return ret0
}

// updateWorkflowExecutionWithNew indicates an expected call of updateWorkflowExecutionWithNew.
func (mr *MockworkflowExecutionContextMockRecorder) updateWorkflowExecutionWithNew(now, updateMode, newContext, newMutableState, currentWorkflowTransactionPolicy, newWorkflowTransactionPolicy interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "updateWorkflowExecutionWithNew", reflect.TypeOf((*MockworkflowExecutionContext)(nil).updateWorkflowExecutionWithNew), now, updateMode, newContext, newMutableState, currentWorkflowTransactionPolicy, newWorkflowTransactionPolicy)
}
