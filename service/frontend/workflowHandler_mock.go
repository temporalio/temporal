// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

// Code generated by MockGen. DO NOT EDIT.
// Source: ../../.gen/go/temporal/workflowserviceserver/server.go

// Package frontend is a generated GoMock package.
package frontend

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"

	replicator "github.com/temporalio/temporal/.gen/go/replicator"
	shared "github.com/temporalio/temporal/.gen/go/shared"
)

// MockWorkflowHandler is a mock of Interface interface
type MockWorkflowHandler struct {
	ctrl     *gomock.Controller
	recorder *MockWorkflowHandlerMockRecorder
}

// MockWorkflowHandlerMockRecorder is the mock recorder for MockWorkflowHandler
type MockWorkflowHandlerMockRecorder struct {
	mock *MockWorkflowHandler
}

// NewMockWorkflowHandler creates a new mock instance
func NewMockWorkflowHandler(ctrl *gomock.Controller) *MockWorkflowHandler {
	mock := &MockWorkflowHandler{ctrl: ctrl}
	mock.recorder = &MockWorkflowHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWorkflowHandler) EXPECT() *MockWorkflowHandlerMockRecorder {
	return m.recorder
}

// CountWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) CountWorkflowExecutions(ctx context.Context, CountRequest *shared.CountWorkflowExecutionsRequest) (*shared.CountWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountWorkflowExecutions", ctx, CountRequest)
	ret0, _ := ret[0].(*shared.CountWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountWorkflowExecutions indicates an expected call of CountWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) CountWorkflowExecutions(ctx, CountRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).CountWorkflowExecutions), ctx, CountRequest)
}

// DeprecateDomain mocks base method
func (m *MockWorkflowHandler) DeprecateDomain(ctx context.Context, DeprecateRequest *shared.DeprecateDomainRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeprecateDomain", ctx, DeprecateRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeprecateDomain indicates an expected call of DeprecateDomain
func (mr *MockWorkflowHandlerMockRecorder) DeprecateDomain(ctx, DeprecateRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeprecateDomain", reflect.TypeOf((*MockWorkflowHandler)(nil).DeprecateDomain), ctx, DeprecateRequest)
}

// DescribeDomain mocks base method
func (m *MockWorkflowHandler) DescribeDomain(ctx context.Context, DescribeRequest *shared.DescribeDomainRequest) (*shared.DescribeDomainResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeDomain", ctx, DescribeRequest)
	ret0, _ := ret[0].(*shared.DescribeDomainResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeDomain indicates an expected call of DescribeDomain
func (mr *MockWorkflowHandlerMockRecorder) DescribeDomain(ctx, DescribeRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeDomain", reflect.TypeOf((*MockWorkflowHandler)(nil).DescribeDomain), ctx, DescribeRequest)
}

// DescribeTaskList mocks base method
func (m *MockWorkflowHandler) DescribeTaskList(ctx context.Context, Request *shared.DescribeTaskListRequest) (*shared.DescribeTaskListResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskList", ctx, Request)
	ret0, _ := ret[0].(*shared.DescribeTaskListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskList indicates an expected call of DescribeTaskList
func (mr *MockWorkflowHandlerMockRecorder) DescribeTaskList(ctx, Request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskList", reflect.TypeOf((*MockWorkflowHandler)(nil).DescribeTaskList), ctx, Request)
}

// DescribeWorkflowExecution mocks base method
func (m *MockWorkflowHandler) DescribeWorkflowExecution(ctx context.Context, DescribeRequest *shared.DescribeWorkflowExecutionRequest) (*shared.DescribeWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeWorkflowExecution", ctx, DescribeRequest)
	ret0, _ := ret[0].(*shared.DescribeWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeWorkflowExecution indicates an expected call of DescribeWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) DescribeWorkflowExecution(ctx, DescribeRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).DescribeWorkflowExecution), ctx, DescribeRequest)
}

// GetClusterInfo mocks base method
func (m *MockWorkflowHandler) GetClusterInfo(ctx context.Context) (*shared.ClusterInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterInfo", ctx)
	ret0, _ := ret[0].(*shared.ClusterInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterInfo indicates an expected call of GetClusterInfo
func (mr *MockWorkflowHandlerMockRecorder) GetClusterInfo(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterInfo", reflect.TypeOf((*MockWorkflowHandler)(nil).GetClusterInfo), ctx)
}

// GetDomainReplicationMessages mocks base method
func (m *MockWorkflowHandler) GetDomainReplicationMessages(ctx context.Context, Request *replicator.GetDomainReplicationMessagesRequest) (*replicator.GetDomainReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDomainReplicationMessages", ctx, Request)
	ret0, _ := ret[0].(*replicator.GetDomainReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDomainReplicationMessages indicates an expected call of GetDomainReplicationMessages
func (mr *MockWorkflowHandlerMockRecorder) GetDomainReplicationMessages(ctx, Request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDomainReplicationMessages", reflect.TypeOf((*MockWorkflowHandler)(nil).GetDomainReplicationMessages), ctx, Request)
}

// GetReplicationMessages mocks base method
func (m *MockWorkflowHandler) GetReplicationMessages(ctx context.Context, Request *replicator.GetReplicationMessagesRequest) (*replicator.GetReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReplicationMessages", ctx, Request)
	ret0, _ := ret[0].(*replicator.GetReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReplicationMessages indicates an expected call of GetReplicationMessages
func (mr *MockWorkflowHandlerMockRecorder) GetReplicationMessages(ctx, Request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReplicationMessages", reflect.TypeOf((*MockWorkflowHandler)(nil).GetReplicationMessages), ctx, Request)
}

// GetSearchAttributes mocks base method
func (m *MockWorkflowHandler) GetSearchAttributes(ctx context.Context) (*shared.GetSearchAttributesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", ctx)
	ret0, _ := ret[0].(*shared.GetSearchAttributesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes
func (mr *MockWorkflowHandlerMockRecorder) GetSearchAttributes(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockWorkflowHandler)(nil).GetSearchAttributes), ctx)
}

// GetWorkflowExecutionHistory mocks base method
func (m *MockWorkflowHandler) GetWorkflowExecutionHistory(ctx context.Context, GetRequest *shared.GetWorkflowExecutionHistoryRequest) (*shared.GetWorkflowExecutionHistoryResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowExecutionHistory", ctx, GetRequest)
	ret0, _ := ret[0].(*shared.GetWorkflowExecutionHistoryResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkflowExecutionHistory indicates an expected call of GetWorkflowExecutionHistory
func (mr *MockWorkflowHandlerMockRecorder) GetWorkflowExecutionHistory(ctx, GetRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowExecutionHistory", reflect.TypeOf((*MockWorkflowHandler)(nil).GetWorkflowExecutionHistory), ctx, GetRequest)
}

// ListArchivedWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) ListArchivedWorkflowExecutions(ctx context.Context, ListRequest *shared.ListArchivedWorkflowExecutionsRequest) (*shared.ListArchivedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListArchivedWorkflowExecutions", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListArchivedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListArchivedWorkflowExecutions indicates an expected call of ListArchivedWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) ListArchivedWorkflowExecutions(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListArchivedWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).ListArchivedWorkflowExecutions), ctx, ListRequest)
}

// ListClosedWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) ListClosedWorkflowExecutions(ctx context.Context, ListRequest *shared.ListClosedWorkflowExecutionsRequest) (*shared.ListClosedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListClosedWorkflowExecutions", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListClosedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListClosedWorkflowExecutions indicates an expected call of ListClosedWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) ListClosedWorkflowExecutions(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListClosedWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).ListClosedWorkflowExecutions), ctx, ListRequest)
}

// ListDomains mocks base method
func (m *MockWorkflowHandler) ListDomains(ctx context.Context, ListRequest *shared.ListDomainsRequest) (*shared.ListDomainsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListDomains", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListDomainsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListDomains indicates an expected call of ListDomains
func (mr *MockWorkflowHandlerMockRecorder) ListDomains(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListDomains", reflect.TypeOf((*MockWorkflowHandler)(nil).ListDomains), ctx, ListRequest)
}

// ListOpenWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context, ListRequest *shared.ListOpenWorkflowExecutionsRequest) (*shared.ListOpenWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListOpenWorkflowExecutions", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListOpenWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListOpenWorkflowExecutions indicates an expected call of ListOpenWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) ListOpenWorkflowExecutions(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListOpenWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).ListOpenWorkflowExecutions), ctx, ListRequest)
}

// ListWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) ListWorkflowExecutions(ctx context.Context, ListRequest *shared.ListWorkflowExecutionsRequest) (*shared.ListWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListWorkflowExecutions", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListWorkflowExecutions indicates an expected call of ListWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) ListWorkflowExecutions(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).ListWorkflowExecutions), ctx, ListRequest)
}

// PollForActivityTask mocks base method
func (m *MockWorkflowHandler) PollForActivityTask(ctx context.Context, PollRequest *shared.PollForActivityTaskRequest) (*shared.PollForActivityTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollForActivityTask", ctx, PollRequest)
	ret0, _ := ret[0].(*shared.PollForActivityTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollForActivityTask indicates an expected call of PollForActivityTask
func (mr *MockWorkflowHandlerMockRecorder) PollForActivityTask(ctx, PollRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollForActivityTask", reflect.TypeOf((*MockWorkflowHandler)(nil).PollForActivityTask), ctx, PollRequest)
}

// PollForDecisionTask mocks base method
func (m *MockWorkflowHandler) PollForDecisionTask(ctx context.Context, PollRequest *shared.PollForDecisionTaskRequest) (*shared.PollForDecisionTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollForDecisionTask", ctx, PollRequest)
	ret0, _ := ret[0].(*shared.PollForDecisionTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollForDecisionTask indicates an expected call of PollForDecisionTask
func (mr *MockWorkflowHandlerMockRecorder) PollForDecisionTask(ctx, PollRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollForDecisionTask", reflect.TypeOf((*MockWorkflowHandler)(nil).PollForDecisionTask), ctx, PollRequest)
}

// QueryWorkflow mocks base method
func (m *MockWorkflowHandler) QueryWorkflow(ctx context.Context, QueryRequest *shared.QueryWorkflowRequest) (*shared.QueryWorkflowResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkflow", ctx, QueryRequest)
	ret0, _ := ret[0].(*shared.QueryWorkflowResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow
func (mr *MockWorkflowHandlerMockRecorder) QueryWorkflow(ctx, QueryRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockWorkflowHandler)(nil).QueryWorkflow), ctx, QueryRequest)
}

// ReapplyEvents mocks base method
func (m *MockWorkflowHandler) ReapplyEvents(ctx context.Context, ReapplyEventsRequest *shared.ReapplyEventsRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReapplyEvents", ctx, ReapplyEventsRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReapplyEvents indicates an expected call of ReapplyEvents
func (mr *MockWorkflowHandlerMockRecorder) ReapplyEvents(ctx, ReapplyEventsRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReapplyEvents", reflect.TypeOf((*MockWorkflowHandler)(nil).ReapplyEvents), ctx, ReapplyEventsRequest)
}

// RecordActivityTaskHeartbeat mocks base method
func (m *MockWorkflowHandler) RecordActivityTaskHeartbeat(ctx context.Context, HeartbeatRequest *shared.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordActivityTaskHeartbeat", ctx, HeartbeatRequest)
	ret0, _ := ret[0].(*shared.RecordActivityTaskHeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RecordActivityTaskHeartbeat indicates an expected call of RecordActivityTaskHeartbeat
func (mr *MockWorkflowHandlerMockRecorder) RecordActivityTaskHeartbeat(ctx, HeartbeatRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityTaskHeartbeat", reflect.TypeOf((*MockWorkflowHandler)(nil).RecordActivityTaskHeartbeat), ctx, HeartbeatRequest)
}

// RecordActivityTaskHeartbeatByID mocks base method
func (m *MockWorkflowHandler) RecordActivityTaskHeartbeatByID(ctx context.Context, HeartbeatRequest *shared.RecordActivityTaskHeartbeatByIDRequest) (*shared.RecordActivityTaskHeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordActivityTaskHeartbeatByID", ctx, HeartbeatRequest)
	ret0, _ := ret[0].(*shared.RecordActivityTaskHeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RecordActivityTaskHeartbeatByID indicates an expected call of RecordActivityTaskHeartbeatByID
func (mr *MockWorkflowHandlerMockRecorder) RecordActivityTaskHeartbeatByID(ctx, HeartbeatRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityTaskHeartbeatByID", reflect.TypeOf((*MockWorkflowHandler)(nil).RecordActivityTaskHeartbeatByID), ctx, HeartbeatRequest)
}

// RegisterDomain mocks base method
func (m *MockWorkflowHandler) RegisterDomain(ctx context.Context, RegisterRequest *shared.RegisterDomainRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterDomain", ctx, RegisterRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RegisterDomain indicates an expected call of RegisterDomain
func (mr *MockWorkflowHandlerMockRecorder) RegisterDomain(ctx, RegisterRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterDomain", reflect.TypeOf((*MockWorkflowHandler)(nil).RegisterDomain), ctx, RegisterRequest)
}

// RequestCancelWorkflowExecution mocks base method
func (m *MockWorkflowHandler) RequestCancelWorkflowExecution(ctx context.Context, CancelRequest *shared.RequestCancelWorkflowExecutionRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RequestCancelWorkflowExecution", ctx, CancelRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RequestCancelWorkflowExecution indicates an expected call of RequestCancelWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) RequestCancelWorkflowExecution(ctx, CancelRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RequestCancelWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).RequestCancelWorkflowExecution), ctx, CancelRequest)
}

// ResetStickyTaskList mocks base method
func (m *MockWorkflowHandler) ResetStickyTaskList(ctx context.Context, ResetRequest *shared.ResetStickyTaskListRequest) (*shared.ResetStickyTaskListResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetStickyTaskList", ctx, ResetRequest)
	ret0, _ := ret[0].(*shared.ResetStickyTaskListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetStickyTaskList indicates an expected call of ResetStickyTaskList
func (mr *MockWorkflowHandlerMockRecorder) ResetStickyTaskList(ctx, ResetRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetStickyTaskList", reflect.TypeOf((*MockWorkflowHandler)(nil).ResetStickyTaskList), ctx, ResetRequest)
}

// ResetWorkflowExecution mocks base method
func (m *MockWorkflowHandler) ResetWorkflowExecution(ctx context.Context, ResetRequest *shared.ResetWorkflowExecutionRequest) (*shared.ResetWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetWorkflowExecution", ctx, ResetRequest)
	ret0, _ := ret[0].(*shared.ResetWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetWorkflowExecution indicates an expected call of ResetWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) ResetWorkflowExecution(ctx, ResetRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).ResetWorkflowExecution), ctx, ResetRequest)
}

// RespondActivityTaskCanceled mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskCanceled(ctx context.Context, CanceledRequest *shared.RespondActivityTaskCanceledRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCanceled", ctx, CanceledRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskCanceled indicates an expected call of RespondActivityTaskCanceled
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskCanceled(ctx, CanceledRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCanceled", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskCanceled), ctx, CanceledRequest)
}

// RespondActivityTaskCanceledByID mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskCanceledByID(ctx context.Context, CanceledRequest *shared.RespondActivityTaskCanceledByIDRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCanceledByID", ctx, CanceledRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskCanceledByID indicates an expected call of RespondActivityTaskCanceledByID
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskCanceledByID(ctx, CanceledRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCanceledByID", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskCanceledByID), ctx, CanceledRequest)
}

// RespondActivityTaskCompleted mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondActivityTaskCompletedRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCompleted", ctx, CompleteRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskCompleted indicates an expected call of RespondActivityTaskCompleted
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskCompleted(ctx, CompleteRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCompleted", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskCompleted), ctx, CompleteRequest)
}

// RespondActivityTaskCompletedByID mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskCompletedByID(ctx context.Context, CompleteRequest *shared.RespondActivityTaskCompletedByIDRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskCompletedByID", ctx, CompleteRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskCompletedByID indicates an expected call of RespondActivityTaskCompletedByID
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskCompletedByID(ctx, CompleteRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskCompletedByID", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskCompletedByID), ctx, CompleteRequest)
}

// RespondActivityTaskFailed mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskFailed(ctx context.Context, FailRequest *shared.RespondActivityTaskFailedRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskFailed", ctx, FailRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskFailed indicates an expected call of RespondActivityTaskFailed
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskFailed(ctx, FailRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskFailed", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskFailed), ctx, FailRequest)
}

// RespondActivityTaskFailedByID mocks base method
func (m *MockWorkflowHandler) RespondActivityTaskFailedByID(ctx context.Context, FailRequest *shared.RespondActivityTaskFailedByIDRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondActivityTaskFailedByID", ctx, FailRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondActivityTaskFailedByID indicates an expected call of RespondActivityTaskFailedByID
func (mr *MockWorkflowHandlerMockRecorder) RespondActivityTaskFailedByID(ctx, FailRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondActivityTaskFailedByID", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondActivityTaskFailedByID), ctx, FailRequest)
}

// RespondDecisionTaskCompleted mocks base method
func (m *MockWorkflowHandler) RespondDecisionTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondDecisionTaskCompletedRequest) (*shared.RespondDecisionTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondDecisionTaskCompleted", ctx, CompleteRequest)
	ret0, _ := ret[0].(*shared.RespondDecisionTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondDecisionTaskCompleted indicates an expected call of RespondDecisionTaskCompleted
func (mr *MockWorkflowHandlerMockRecorder) RespondDecisionTaskCompleted(ctx, CompleteRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondDecisionTaskCompleted", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondDecisionTaskCompleted), ctx, CompleteRequest)
}

// RespondDecisionTaskFailed mocks base method
func (m *MockWorkflowHandler) RespondDecisionTaskFailed(ctx context.Context, FailedRequest *shared.RespondDecisionTaskFailedRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondDecisionTaskFailed", ctx, FailedRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondDecisionTaskFailed indicates an expected call of RespondDecisionTaskFailed
func (mr *MockWorkflowHandlerMockRecorder) RespondDecisionTaskFailed(ctx, FailedRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondDecisionTaskFailed", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondDecisionTaskFailed), ctx, FailedRequest)
}

// RespondQueryTaskCompleted mocks base method
func (m *MockWorkflowHandler) RespondQueryTaskCompleted(ctx context.Context, CompleteRequest *shared.RespondQueryTaskCompletedRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondQueryTaskCompleted", ctx, CompleteRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// RespondQueryTaskCompleted indicates an expected call of RespondQueryTaskCompleted
func (mr *MockWorkflowHandlerMockRecorder) RespondQueryTaskCompleted(ctx, CompleteRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondQueryTaskCompleted", reflect.TypeOf((*MockWorkflowHandler)(nil).RespondQueryTaskCompleted), ctx, CompleteRequest)
}

// ScanWorkflowExecutions mocks base method
func (m *MockWorkflowHandler) ScanWorkflowExecutions(ctx context.Context, ListRequest *shared.ListWorkflowExecutionsRequest) (*shared.ListWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScanWorkflowExecutions", ctx, ListRequest)
	ret0, _ := ret[0].(*shared.ListWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ScanWorkflowExecutions indicates an expected call of ScanWorkflowExecutions
func (mr *MockWorkflowHandlerMockRecorder) ScanWorkflowExecutions(ctx, ListRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScanWorkflowExecutions", reflect.TypeOf((*MockWorkflowHandler)(nil).ScanWorkflowExecutions), ctx, ListRequest)
}

// SignalWithStartWorkflowExecution mocks base method
func (m *MockWorkflowHandler) SignalWithStartWorkflowExecution(ctx context.Context, SignalWithStartRequest *shared.SignalWithStartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWithStartWorkflowExecution", ctx, SignalWithStartRequest)
	ret0, _ := ret[0].(*shared.StartWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SignalWithStartWorkflowExecution indicates an expected call of SignalWithStartWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) SignalWithStartWorkflowExecution(ctx, SignalWithStartRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWithStartWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).SignalWithStartWorkflowExecution), ctx, SignalWithStartRequest)
}

// SignalWorkflowExecution mocks base method
func (m *MockWorkflowHandler) SignalWorkflowExecution(ctx context.Context, SignalRequest *shared.SignalWorkflowExecutionRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWorkflowExecution", ctx, SignalRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// SignalWorkflowExecution indicates an expected call of SignalWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) SignalWorkflowExecution(ctx, SignalRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).SignalWorkflowExecution), ctx, SignalRequest)
}

// StartWorkflowExecution mocks base method
func (m *MockWorkflowHandler) StartWorkflowExecution(ctx context.Context, StartRequest *shared.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartWorkflowExecution", ctx, StartRequest)
	ret0, _ := ret[0].(*shared.StartWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartWorkflowExecution indicates an expected call of StartWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) StartWorkflowExecution(ctx, StartRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).StartWorkflowExecution), ctx, StartRequest)
}

// TerminateWorkflowExecution mocks base method
func (m *MockWorkflowHandler) TerminateWorkflowExecution(ctx context.Context, TerminateRequest *shared.TerminateWorkflowExecutionRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TerminateWorkflowExecution", ctx, TerminateRequest)
	ret0, _ := ret[0].(error)
	return ret0
}

// TerminateWorkflowExecution indicates an expected call of TerminateWorkflowExecution
func (mr *MockWorkflowHandlerMockRecorder) TerminateWorkflowExecution(ctx, TerminateRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateWorkflowExecution", reflect.TypeOf((*MockWorkflowHandler)(nil).TerminateWorkflowExecution), ctx, TerminateRequest)
}

// UpdateDomain mocks base method
func (m *MockWorkflowHandler) UpdateDomain(ctx context.Context, UpdateRequest *shared.UpdateDomainRequest) (*shared.UpdateDomainResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateDomain", ctx, UpdateRequest)
	ret0, _ := ret[0].(*shared.UpdateDomainResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateDomain indicates an expected call of UpdateDomain
func (mr *MockWorkflowHandlerMockRecorder) UpdateDomain(ctx, UpdateRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateDomain", reflect.TypeOf((*MockWorkflowHandler)(nil).UpdateDomain), ctx, UpdateRequest)
}
