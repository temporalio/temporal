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
// Source: interface.go

// Package frontend is a generated GoMock package.
package frontend

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "go.temporal.io/api/workflowservice/v1"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

// MockHandler is a mock of Handler interface.
type MockHandler struct {
	ctrl     *gomock.Controller
	recorder *MockHandlerMockRecorder
}

// MockHandlerMockRecorder is the mock recorder for MockHandler.
type MockHandlerMockRecorder struct {
	mock *MockHandler
}

// NewMockHandler creates a new mock instance.
func NewMockHandler(ctrl *gomock.Controller) *MockHandler {
	mock := &MockHandler{ctrl: ctrl}
	mock.recorder = &MockHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHandler) EXPECT() *MockHandlerMockRecorder {
	return m.recorder
}

// Check mocks base method.
func (m *MockHandler) Check(arg0 context.Context, arg1 *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Check", arg0, arg1)
	ret0, _ := ret[0].(*grpc_health_v1.HealthCheckResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Check indicates an expected call of Check.
func (mr *MockHandlerMockRecorder) Check(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Check", reflect.TypeOf((*MockHandler)(nil).Check), arg0, arg1)
}

// CountWorkflowExecutions mocks base method.
func (m *MockHandler) CountWorkflowExecutions(arg0 context.Context, arg1 *v1.CountWorkflowExecutionsRequest) (*v1.CountWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.CountWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountWorkflowExecutions indicates an expected call of CountWorkflowExecutions.
func (mr *MockHandlerMockRecorder) CountWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).CountWorkflowExecutions), arg0, arg1)
}

// DeprecateNamespace mocks base method.
func (m *MockHandler) DeprecateNamespace(arg0 context.Context, arg1 *v1.DeprecateNamespaceRequest) (*v1.DeprecateNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeprecateNamespace", arg0, arg1)
	ret0, _ := ret[0].(*v1.DeprecateNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeprecateNamespace indicates an expected call of DeprecateNamespace.
func (mr *MockHandlerMockRecorder) DeprecateNamespace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeprecateNamespace", reflect.TypeOf((*MockHandler)(nil).DeprecateNamespace), arg0, arg1)
}

// DescribeNamespace mocks base method.
func (m *MockHandler) DescribeNamespace(arg0 context.Context, arg1 *v1.DescribeNamespaceRequest) (*v1.DescribeNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeNamespace", arg0, arg1)
	ret0, _ := ret[0].(*v1.DescribeNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeNamespace indicates an expected call of DescribeNamespace.
func (mr *MockHandlerMockRecorder) DescribeNamespace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeNamespace", reflect.TypeOf((*MockHandler)(nil).DescribeNamespace), arg0, arg1)
}

// DescribeTaskQueue mocks base method.
func (m *MockHandler) DescribeTaskQueue(arg0 context.Context, arg1 *v1.DescribeTaskQueueRequest) (*v1.DescribeTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.DescribeTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueue indicates an expected call of DescribeTaskQueue.
func (mr *MockHandlerMockRecorder) DescribeTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueue", reflect.TypeOf((*MockHandler)(nil).DescribeTaskQueue), arg0, arg1)
}

// DescribeWorkflowExecution mocks base method.
func (m *MockHandler) DescribeWorkflowExecution(arg0 context.Context, arg1 *v1.DescribeWorkflowExecutionRequest) (*v1.DescribeWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.DescribeWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeWorkflowExecution indicates an expected call of DescribeWorkflowExecution.
func (mr *MockHandlerMockRecorder) DescribeWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).DescribeWorkflowExecution), arg0, arg1)
}

// GetClusterInfo mocks base method.
func (m *MockHandler) GetClusterInfo(arg0 context.Context, arg1 *v1.GetClusterInfoRequest) (*v1.GetClusterInfoResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterInfo", arg0, arg1)
	ret0, _ := ret[0].(*v1.GetClusterInfoResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterInfo indicates an expected call of GetClusterInfo.
func (mr *MockHandlerMockRecorder) GetClusterInfo(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterInfo", reflect.TypeOf((*MockHandler)(nil).GetClusterInfo), arg0, arg1)
}

// GetConfig mocks base method.
func (m *MockHandler) GetConfig() *Config {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConfig")
	ret0, _ := ret[0].(*Config)
	return ret0
}

// GetConfig indicates an expected call of GetConfig.
func (mr *MockHandlerMockRecorder) GetConfig() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfig", reflect.TypeOf((*MockHandler)(nil).GetConfig))
}

// GetSearchAttributes mocks base method.
func (m *MockHandler) GetSearchAttributes(arg0 context.Context, arg1 *v1.GetSearchAttributesRequest) (*v1.GetSearchAttributesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", arg0, arg1)
	ret0, _ := ret[0].(*v1.GetSearchAttributesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes.
func (mr *MockHandlerMockRecorder) GetSearchAttributes(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockHandler)(nil).GetSearchAttributes), arg0, arg1)
}

// GetWorkflowExecutionHistory mocks base method.
func (m *MockHandler) GetWorkflowExecutionHistory(arg0 context.Context, arg1 *v1.GetWorkflowExecutionHistoryRequest) (*v1.GetWorkflowExecutionHistoryResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowExecutionHistory", arg0, arg1)
	ret0, _ := ret[0].(*v1.GetWorkflowExecutionHistoryResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkflowExecutionHistory indicates an expected call of GetWorkflowExecutionHistory.
func (mr *MockHandlerMockRecorder) GetWorkflowExecutionHistory(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowExecutionHistory", reflect.TypeOf((*MockHandler)(nil).GetWorkflowExecutionHistory), arg0, arg1)
}

// ListArchivedWorkflowExecutions mocks base method.
func (m *MockHandler) ListArchivedWorkflowExecutions(arg0 context.Context, arg1 *v1.ListArchivedWorkflowExecutionsRequest) (*v1.ListArchivedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListArchivedWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListArchivedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListArchivedWorkflowExecutions indicates an expected call of ListArchivedWorkflowExecutions.
func (mr *MockHandlerMockRecorder) ListArchivedWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListArchivedWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).ListArchivedWorkflowExecutions), arg0, arg1)
}

// ListClosedWorkflowExecutions mocks base method.
func (m *MockHandler) ListClosedWorkflowExecutions(arg0 context.Context, arg1 *v1.ListClosedWorkflowExecutionsRequest) (*v1.ListClosedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListClosedWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListClosedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListClosedWorkflowExecutions indicates an expected call of ListClosedWorkflowExecutions.
func (mr *MockHandlerMockRecorder) ListClosedWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListClosedWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).ListClosedWorkflowExecutions), arg0, arg1)
}

// ListNamespaces mocks base method.
func (m *MockHandler) ListNamespaces(arg0 context.Context, arg1 *v1.ListNamespacesRequest) (*v1.ListNamespacesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListNamespaces", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListNamespacesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListNamespaces indicates an expected call of ListNamespaces.
func (mr *MockHandlerMockRecorder) ListNamespaces(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListNamespaces", reflect.TypeOf((*MockHandler)(nil).ListNamespaces), arg0, arg1)
}

// ListOpenWorkflowExecutions mocks base method.
func (m *MockHandler) ListOpenWorkflowExecutions(arg0 context.Context, arg1 *v1.ListOpenWorkflowExecutionsRequest) (*v1.ListOpenWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListOpenWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListOpenWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListOpenWorkflowExecutions indicates an expected call of ListOpenWorkflowExecutions.
func (mr *MockHandlerMockRecorder) ListOpenWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListOpenWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).ListOpenWorkflowExecutions), arg0, arg1)
}

// ListTaskQueuePartitions mocks base method.
func (m *MockHandler) ListTaskQueuePartitions(arg0 context.Context, arg1 *v1.ListTaskQueuePartitionsRequest) (*v1.ListTaskQueuePartitionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTaskQueuePartitions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListTaskQueuePartitionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTaskQueuePartitions indicates an expected call of ListTaskQueuePartitions.
func (mr *MockHandlerMockRecorder) ListTaskQueuePartitions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTaskQueuePartitions", reflect.TypeOf((*MockHandler)(nil).ListTaskQueuePartitions), arg0, arg1)
}

// ListWorkflowExecutions mocks base method.
func (m *MockHandler) ListWorkflowExecutions(arg0 context.Context, arg1 *v1.ListWorkflowExecutionsRequest) (*v1.ListWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListWorkflowExecutions indicates an expected call of ListWorkflowExecutions.
func (mr *MockHandlerMockRecorder) ListWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).ListWorkflowExecutions), arg0, arg1)
}

// PollActivityTaskQueue mocks base method.
func (m *MockHandler) PollActivityTaskQueue(arg0 context.Context, arg1 *v1.PollActivityTaskQueueRequest) (*v1.PollActivityTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollActivityTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.PollActivityTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollActivityTaskQueue indicates an expected call of PollActivityTaskQueue.
func (mr *MockHandlerMockRecorder) PollActivityTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollActivityTaskQueue", reflect.TypeOf((*MockHandler)(nil).PollActivityTaskQueue), arg0, arg1)
}

// PollWorkflowTaskQueue mocks base method.
func (m *MockHandler) PollWorkflowTaskQueue(arg0 context.Context, arg1 *v1.PollWorkflowTaskQueueRequest) (*v1.PollWorkflowTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollWorkflowTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.PollWorkflowTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollWorkflowTaskQueue indicates an expected call of PollWorkflowTaskQueue.
func (mr *MockHandlerMockRecorder) PollWorkflowTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollWorkflowTaskQueue", reflect.TypeOf((*MockHandler)(nil).PollWorkflowTaskQueue), arg0, arg1)
}

// QueryWorkflow mocks base method.
func (m *MockHandler) QueryWorkflow(arg0 context.Context, arg1 *v1.QueryWorkflowRequest) (*v1.QueryWorkflowResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*v1.QueryWorkflowResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow.
func (mr *MockHandlerMockRecorder) QueryWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockHandler)(nil).QueryWorkflow), arg0, arg1)
}

// RecordActivityTaskHeartbeat mocks base method.
func (m *MockHandler) RecordActivityTaskHeartbeat(arg0 context.Context, arg1 *v1.RecordActivityTaskHeartbeatRequest) (*v1.RecordActivityTaskHeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordActivityTaskHeartbeat", arg0, arg1)
	ret0, _ := ret[0].(*v1.RecordActivityTaskHeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RecordActivityTaskHeartbeat indicates an expected call of RecordActivityTaskHeartbeat.
func (mr *MockHandlerMockRecorder) RecordActivityTaskHeartbeat(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityTaskHeartbeat", reflect.TypeOf((*MockHandler)(nil).RecordActivityTaskHeartbeat), arg0, arg1)
}

// RecordActivityTaskHeartbeatById mocks base method.
func (m *MockHandler) RecordActivityTaskHeartbeatById(arg0 context.Context, arg1 *v1.RecordActivityTaskHeartbeatByIdRequest) (*v1.RecordActivityTaskHeartbeatByIdResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordActivityTaskHeartbeatById", arg0, arg1)
	ret0, _ := ret[0].(*v1.RecordActivityTaskHeartbeatByIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RecordActivityTaskHeartbeatById indicates an expected call of RecordActivityTaskHeartbeatById.
func (mr *MockHandlerMockRecorder) RecordActivityTaskHeartbeatById(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityTaskHeartbeatById", reflect.TypeOf((*MockHandler)(nil).RecordActivityTaskHeartbeatById), arg0, arg1)
}

// RegisterNamespace mocks base method.
func (m *MockHandler) RegisterNamespace(arg0 context.Context, arg1 *v1.RegisterNamespaceRequest) (*v1.RegisterNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterNamespace", arg0, arg1)
	ret0, _ := ret[0].(*v1.RegisterNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterNamespace indicates an expected call of RegisterNamespace.
func (mr *MockHandlerMockRecorder) RegisterNamespace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterNamespace", reflect.TypeOf((*MockHandler)(nil).RegisterNamespace), arg0, arg1)
}

// RequestCancelWorkflowExecution mocks base method.
func (m *MockHandler) RequestCancelWorkflowExecution(arg0 context.Context, arg1 *v1.RequestCancelWorkflowExecutionRequest) (*v1.RequestCancelWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RequestCancelWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.RequestCancelWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RequestCancelWorkflowExecution indicates an expected call of RequestCancelWorkflowExecution.
func (mr *MockHandlerMockRecorder) RequestCancelWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RequestCancelWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).RequestCancelWorkflowExecution), arg0, arg1)
}

// ResetStickyTaskQueue mocks base method.
func (m *MockHandler) ResetStickyTaskQueue(arg0 context.Context, arg1 *v1.ResetStickyTaskQueueRequest) (*v1.ResetStickyTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetStickyTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.ResetStickyTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetStickyTaskQueue indicates an expected call of ResetStickyTaskQueue.
func (mr *MockHandlerMockRecorder) ResetStickyTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetStickyTaskQueue", reflect.TypeOf((*MockHandler)(nil).ResetStickyTaskQueue), arg0, arg1)
}

// ResetWorkflowExecution mocks base method.
func (m *MockHandler) ResetWorkflowExecution(arg0 context.Context, arg1 *v1.ResetWorkflowExecutionRequest) (*v1.ResetWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.ResetWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetWorkflowExecution indicates an expected call of ResetWorkflowExecution.
func (mr *MockHandlerMockRecorder) ResetWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).ResetWorkflowExecution), arg0, arg1)
}

// RespondActivityTaskCanceled mocks base method.
func (m *MockHandler) RespondActivityTaskCanceled(arg0 context.Context, arg1 *v1.RespondActivityTaskCanceledRequest) (*v1.RespondActivityTaskCanceledResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCanceled", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskCanceledResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskCanceled indicates an expected call of RespondActivityTaskCanceled.
func (mr *MockHandlerMockRecorder) RespondActivityTaskCanceled(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCanceled", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskCanceled), arg0, arg1)
}

// RespondActivityTaskCanceledById mocks base method.
func (m *MockHandler) RespondActivityTaskCanceledById(arg0 context.Context, arg1 *v1.RespondActivityTaskCanceledByIdRequest) (*v1.RespondActivityTaskCanceledByIdResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCanceledById", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskCanceledByIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskCanceledById indicates an expected call of RespondActivityTaskCanceledById.
func (mr *MockHandlerMockRecorder) RespondActivityTaskCanceledById(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCanceledById", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskCanceledById), arg0, arg1)
}

// RespondActivityTaskCompleted mocks base method.
func (m *MockHandler) RespondActivityTaskCompleted(arg0 context.Context, arg1 *v1.RespondActivityTaskCompletedRequest) (*v1.RespondActivityTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCompleted", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskCompleted indicates an expected call of RespondActivityTaskCompleted.
func (mr *MockHandlerMockRecorder) RespondActivityTaskCompleted(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCompleted", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskCompleted), arg0, arg1)
}

// RespondActivityTaskCompletedById mocks base method.
func (m *MockHandler) RespondActivityTaskCompletedById(arg0 context.Context, arg1 *v1.RespondActivityTaskCompletedByIdRequest) (*v1.RespondActivityTaskCompletedByIdResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCompletedById", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskCompletedByIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskCompletedById indicates an expected call of RespondActivityTaskCompletedById.
func (mr *MockHandlerMockRecorder) RespondActivityTaskCompletedById(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCompletedById", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskCompletedById), arg0, arg1)
}

// RespondActivityTaskFailed mocks base method.
func (m *MockHandler) RespondActivityTaskFailed(arg0 context.Context, arg1 *v1.RespondActivityTaskFailedRequest) (*v1.RespondActivityTaskFailedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskFailed", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskFailedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskFailed indicates an expected call of RespondActivityTaskFailed.
func (mr *MockHandlerMockRecorder) RespondActivityTaskFailed(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskFailed", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskFailed), arg0, arg1)
}

// RespondActivityTaskFailedById mocks base method.
func (m *MockHandler) RespondActivityTaskFailedById(arg0 context.Context, arg1 *v1.RespondActivityTaskFailedByIdRequest) (*v1.RespondActivityTaskFailedByIdResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskFailedById", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondActivityTaskFailedByIdResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondActivityTaskFailedById indicates an expected call of RespondActivityTaskFailedById.
func (mr *MockHandlerMockRecorder) RespondActivityTaskFailedById(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskFailedById", reflect.TypeOf((*MockHandler)(nil).RespondActivityTaskFailedById), arg0, arg1)
}

// RespondQueryTaskCompleted mocks base method.
func (m *MockHandler) RespondQueryTaskCompleted(arg0 context.Context, arg1 *v1.RespondQueryTaskCompletedRequest) (*v1.RespondQueryTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondQueryTaskCompleted", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondQueryTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondQueryTaskCompleted indicates an expected call of RespondQueryTaskCompleted.
func (mr *MockHandlerMockRecorder) RespondQueryTaskCompleted(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondQueryTaskCompleted", reflect.TypeOf((*MockHandler)(nil).RespondQueryTaskCompleted), arg0, arg1)
}

// RespondWorkflowTaskCompleted mocks base method.
func (m *MockHandler) RespondWorkflowTaskCompleted(arg0 context.Context, arg1 *v1.RespondWorkflowTaskCompletedRequest) (*v1.RespondWorkflowTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondWorkflowTaskCompleted", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondWorkflowTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondWorkflowTaskCompleted indicates an expected call of RespondWorkflowTaskCompleted.
func (mr *MockHandlerMockRecorder) RespondWorkflowTaskCompleted(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondWorkflowTaskCompleted", reflect.TypeOf((*MockHandler)(nil).RespondWorkflowTaskCompleted), arg0, arg1)
}

// RespondWorkflowTaskFailed mocks base method.
func (m *MockHandler) RespondWorkflowTaskFailed(arg0 context.Context, arg1 *v1.RespondWorkflowTaskFailedRequest) (*v1.RespondWorkflowTaskFailedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondWorkflowTaskFailed", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondWorkflowTaskFailedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondWorkflowTaskFailed indicates an expected call of RespondWorkflowTaskFailed.
func (mr *MockHandlerMockRecorder) RespondWorkflowTaskFailed(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondWorkflowTaskFailed", reflect.TypeOf((*MockHandler)(nil).RespondWorkflowTaskFailed), arg0, arg1)
}

// ScanWorkflowExecutions mocks base method.
func (m *MockHandler) ScanWorkflowExecutions(arg0 context.Context, arg1 *v1.ScanWorkflowExecutionsRequest) (*v1.ScanWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScanWorkflowExecutions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ScanWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ScanWorkflowExecutions indicates an expected call of ScanWorkflowExecutions.
func (mr *MockHandlerMockRecorder) ScanWorkflowExecutions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScanWorkflowExecutions", reflect.TypeOf((*MockHandler)(nil).ScanWorkflowExecutions), arg0, arg1)
}

// SignalWithStartWorkflowExecution mocks base method.
func (m *MockHandler) SignalWithStartWorkflowExecution(arg0 context.Context, arg1 *v1.SignalWithStartWorkflowExecutionRequest) (*v1.SignalWithStartWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWithStartWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.SignalWithStartWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SignalWithStartWorkflowExecution indicates an expected call of SignalWithStartWorkflowExecution.
func (mr *MockHandlerMockRecorder) SignalWithStartWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWithStartWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).SignalWithStartWorkflowExecution), arg0, arg1)
}

// SignalWorkflowExecution mocks base method.
func (m *MockHandler) SignalWorkflowExecution(arg0 context.Context, arg1 *v1.SignalWorkflowExecutionRequest) (*v1.SignalWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.SignalWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SignalWorkflowExecution indicates an expected call of SignalWorkflowExecution.
func (mr *MockHandlerMockRecorder) SignalWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).SignalWorkflowExecution), arg0, arg1)
}

// Start mocks base method.
func (m *MockHandler) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockHandlerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockHandler)(nil).Start))
}

// StartWorkflowExecution mocks base method.
func (m *MockHandler) StartWorkflowExecution(arg0 context.Context, arg1 *v1.StartWorkflowExecutionRequest) (*v1.StartWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.StartWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartWorkflowExecution indicates an expected call of StartWorkflowExecution.
func (mr *MockHandlerMockRecorder) StartWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).StartWorkflowExecution), arg0, arg1)
}

// Stop mocks base method.
func (m *MockHandler) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockHandlerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockHandler)(nil).Stop))
}

// TerminateWorkflowExecution mocks base method.
func (m *MockHandler) TerminateWorkflowExecution(arg0 context.Context, arg1 *v1.TerminateWorkflowExecutionRequest) (*v1.TerminateWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TerminateWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*v1.TerminateWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TerminateWorkflowExecution indicates an expected call of TerminateWorkflowExecution.
func (mr *MockHandlerMockRecorder) TerminateWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateWorkflowExecution", reflect.TypeOf((*MockHandler)(nil).TerminateWorkflowExecution), arg0, arg1)
}

// UpdateHealthStatus mocks base method.
func (m *MockHandler) UpdateHealthStatus(status HealthStatus) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UpdateHealthStatus", status)
}

// UpdateHealthStatus indicates an expected call of UpdateHealthStatus.
func (mr *MockHandlerMockRecorder) UpdateHealthStatus(status interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateHealthStatus", reflect.TypeOf((*MockHandler)(nil).UpdateHealthStatus), status)
}

// UpdateNamespace mocks base method.
func (m *MockHandler) UpdateNamespace(arg0 context.Context, arg1 *v1.UpdateNamespaceRequest) (*v1.UpdateNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateNamespace", arg0, arg1)
	ret0, _ := ret[0].(*v1.UpdateNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateNamespace indicates an expected call of UpdateNamespace.
func (mr *MockHandlerMockRecorder) UpdateNamespace(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateNamespace", reflect.TypeOf((*MockHandler)(nil).UpdateNamespace), arg0, arg1)
}

// Watch mocks base method.
func (m *MockHandler) Watch(arg0 *grpc_health_v1.HealthCheckRequest, arg1 grpc_health_v1.Health_WatchServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Watch", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Watch indicates an expected call of Watch.
func (mr *MockHandlerMockRecorder) Watch(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Watch", reflect.TypeOf((*MockHandler)(nil).Watch), arg0, arg1)
}
