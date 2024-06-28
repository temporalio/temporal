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
// Source: go.temporal.io/sdk/client (interfaces: Client)

// Package mocksdk is a generated GoMock package.
package mocksdk

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	enums "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	client "go.temporal.io/sdk/client"
	converter "go.temporal.io/sdk/converter"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// CancelWorkflow mocks base method.
func (m *MockClient) CancelWorkflow(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelWorkflow", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelWorkflow indicates an expected call of CancelWorkflow.
func (mr *MockClientMockRecorder) CancelWorkflow(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelWorkflow", reflect.TypeOf((*MockClient)(nil).CancelWorkflow), arg0, arg1, arg2)
}

// CheckHealth mocks base method.
func (m *MockClient) CheckHealth(arg0 context.Context, arg1 *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckHealth", arg0, arg1)
	ret0, _ := ret[0].(*client.CheckHealthResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckHealth indicates an expected call of CheckHealth.
func (mr *MockClientMockRecorder) CheckHealth(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckHealth", reflect.TypeOf((*MockClient)(nil).CheckHealth), arg0, arg1)
}

// Close mocks base method.
func (m *MockClient) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// CompleteActivity mocks base method.
func (m *MockClient) CompleteActivity(arg0 context.Context, arg1 []byte, arg2 interface{}, arg3 error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteActivity", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompleteActivity indicates an expected call of CompleteActivity.
func (mr *MockClientMockRecorder) CompleteActivity(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteActivity", reflect.TypeOf((*MockClient)(nil).CompleteActivity), arg0, arg1, arg2, arg3)
}

// CompleteActivityByID mocks base method.
func (m *MockClient) CompleteActivityByID(arg0 context.Context, arg1, arg2, arg3, arg4 string, arg5 interface{}, arg6 error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteActivityByID", arg0, arg1, arg2, arg3, arg4, arg5, arg6)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompleteActivityByID indicates an expected call of CompleteActivityByID.
func (mr *MockClientMockRecorder) CompleteActivityByID(arg0, arg1, arg2, arg3, arg4, arg5, arg6 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteActivityByID", reflect.TypeOf((*MockClient)(nil).CompleteActivityByID), arg0, arg1, arg2, arg3, arg4, arg5, arg6)
}

// CountWorkflow mocks base method.
func (m *MockClient) CountWorkflow(arg0 context.Context, arg1 *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.CountWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountWorkflow indicates an expected call of CountWorkflow.
func (mr *MockClientMockRecorder) CountWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountWorkflow", reflect.TypeOf((*MockClient)(nil).CountWorkflow), arg0, arg1)
}

// DescribeTaskQueue mocks base method.
func (m *MockClient) DescribeTaskQueue(arg0 context.Context, arg1 string, arg2 enums.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueue", arg0, arg1, arg2)
	ret0, _ := ret[0].(*workflowservice.DescribeTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueue indicates an expected call of DescribeTaskQueue.
func (mr *MockClientMockRecorder) DescribeTaskQueue(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueue", reflect.TypeOf((*MockClient)(nil).DescribeTaskQueue), arg0, arg1, arg2)
}

// DescribeTaskQueueEnhanced mocks base method.
func (m *MockClient) DescribeTaskQueueEnhanced(arg0 context.Context, arg1 client.DescribeTaskQueueEnhancedOptions) (client.TaskQueueDescription, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueueEnhanced", arg0, arg1)
	ret0, _ := ret[0].(client.TaskQueueDescription)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueueEnhanced indicates an expected call of DescribeTaskQueueEnhanced.
func (mr *MockClientMockRecorder) DescribeTaskQueueEnhanced(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueueEnhanced", reflect.TypeOf((*MockClient)(nil).DescribeTaskQueueEnhanced), arg0, arg1)
}

// DescribeWorkflowExecution mocks base method.
func (m *MockClient) DescribeWorkflowExecution(arg0 context.Context, arg1, arg2 string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeWorkflowExecution", arg0, arg1, arg2)
	ret0, _ := ret[0].(*workflowservice.DescribeWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeWorkflowExecution indicates an expected call of DescribeWorkflowExecution.
func (mr *MockClientMockRecorder) DescribeWorkflowExecution(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeWorkflowExecution", reflect.TypeOf((*MockClient)(nil).DescribeWorkflowExecution), arg0, arg1, arg2)
}

// ExecuteWorkflow mocks base method.
func (m *MockClient) ExecuteWorkflow(arg0 context.Context, arg1 client.StartWorkflowOptions, arg2 interface{}, arg3 ...interface{}) (client.WorkflowRun, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2}
	for _, a := range arg3 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteWorkflow", varargs...)
	ret0, _ := ret[0].(client.WorkflowRun)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteWorkflow indicates an expected call of ExecuteWorkflow.
func (mr *MockClientMockRecorder) ExecuteWorkflow(arg0, arg1, arg2 interface{}, arg3 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2}, arg3...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteWorkflow", reflect.TypeOf((*MockClient)(nil).ExecuteWorkflow), varargs...)
}

// GetSearchAttributes mocks base method.
func (m *MockClient) GetSearchAttributes(arg0 context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", arg0)
	ret0, _ := ret[0].(*workflowservice.GetSearchAttributesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes.
func (mr *MockClientMockRecorder) GetSearchAttributes(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockClient)(nil).GetSearchAttributes), arg0)
}

// GetWorkerBuildIdCompatibility mocks base method.
func (m *MockClient) GetWorkerBuildIdCompatibility(arg0 context.Context, arg1 *client.GetWorkerBuildIdCompatibilityOptions) (*client.WorkerBuildIDVersionSets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerBuildIdCompatibility", arg0, arg1)
	ret0, _ := ret[0].(*client.WorkerBuildIDVersionSets)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerBuildIdCompatibility indicates an expected call of GetWorkerBuildIdCompatibility.
func (mr *MockClientMockRecorder) GetWorkerBuildIdCompatibility(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerBuildIdCompatibility", reflect.TypeOf((*MockClient)(nil).GetWorkerBuildIdCompatibility), arg0, arg1)
}

// GetWorkerTaskReachability mocks base method.
func (m *MockClient) GetWorkerTaskReachability(arg0 context.Context, arg1 *client.GetWorkerTaskReachabilityOptions) (*client.WorkerTaskReachability, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerTaskReachability", arg0, arg1)
	ret0, _ := ret[0].(*client.WorkerTaskReachability)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerTaskReachability indicates an expected call of GetWorkerTaskReachability.
func (mr *MockClientMockRecorder) GetWorkerTaskReachability(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerTaskReachability", reflect.TypeOf((*MockClient)(nil).GetWorkerTaskReachability), arg0, arg1)
}

// GetWorkerVersioningRules mocks base method.
func (m *MockClient) GetWorkerVersioningRules(arg0 context.Context, arg1 client.GetWorkerVersioningOptions) (*client.WorkerVersioningRules, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerVersioningRules", arg0, arg1)
	ret0, _ := ret[0].(*client.WorkerVersioningRules)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerVersioningRules indicates an expected call of GetWorkerVersioningRules.
func (mr *MockClientMockRecorder) GetWorkerVersioningRules(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerVersioningRules", reflect.TypeOf((*MockClient)(nil).GetWorkerVersioningRules), arg0, arg1)
}

// GetWorkflow mocks base method.
func (m *MockClient) GetWorkflow(arg0 context.Context, arg1, arg2 string) client.WorkflowRun {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflow", arg0, arg1, arg2)
	ret0, _ := ret[0].(client.WorkflowRun)
	return ret0
}

// GetWorkflow indicates an expected call of GetWorkflow.
func (mr *MockClientMockRecorder) GetWorkflow(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflow", reflect.TypeOf((*MockClient)(nil).GetWorkflow), arg0, arg1, arg2)
}

// GetWorkflowHistory mocks base method.
func (m *MockClient) GetWorkflowHistory(arg0 context.Context, arg1, arg2 string, arg3 bool, arg4 enums.HistoryEventFilterType) client.HistoryEventIterator {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowHistory", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(client.HistoryEventIterator)
	return ret0
}

// GetWorkflowHistory indicates an expected call of GetWorkflowHistory.
func (mr *MockClientMockRecorder) GetWorkflowHistory(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowHistory", reflect.TypeOf((*MockClient)(nil).GetWorkflowHistory), arg0, arg1, arg2, arg3, arg4)
}

// GetWorkflowUpdateHandle mocks base method.
func (m *MockClient) GetWorkflowUpdateHandle(arg0 client.GetWorkflowUpdateHandleOptions) client.WorkflowUpdateHandle {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowUpdateHandle", arg0)
	ret0, _ := ret[0].(client.WorkflowUpdateHandle)
	return ret0
}

// GetWorkflowUpdateHandle indicates an expected call of GetWorkflowUpdateHandle.
func (mr *MockClientMockRecorder) GetWorkflowUpdateHandle(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowUpdateHandle", reflect.TypeOf((*MockClient)(nil).GetWorkflowUpdateHandle), arg0)
}

// ListArchivedWorkflow mocks base method.
func (m *MockClient) ListArchivedWorkflow(arg0 context.Context, arg1 *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListArchivedWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ListArchivedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListArchivedWorkflow indicates an expected call of ListArchivedWorkflow.
func (mr *MockClientMockRecorder) ListArchivedWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListArchivedWorkflow", reflect.TypeOf((*MockClient)(nil).ListArchivedWorkflow), arg0, arg1)
}

// ListClosedWorkflow mocks base method.
func (m *MockClient) ListClosedWorkflow(arg0 context.Context, arg1 *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListClosedWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ListClosedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListClosedWorkflow indicates an expected call of ListClosedWorkflow.
func (mr *MockClientMockRecorder) ListClosedWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListClosedWorkflow", reflect.TypeOf((*MockClient)(nil).ListClosedWorkflow), arg0, arg1)
}

// ListOpenWorkflow mocks base method.
func (m *MockClient) ListOpenWorkflow(arg0 context.Context, arg1 *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListOpenWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ListOpenWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListOpenWorkflow indicates an expected call of ListOpenWorkflow.
func (mr *MockClientMockRecorder) ListOpenWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListOpenWorkflow", reflect.TypeOf((*MockClient)(nil).ListOpenWorkflow), arg0, arg1)
}

// ListWorkflow mocks base method.
func (m *MockClient) ListWorkflow(arg0 context.Context, arg1 *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ListWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListWorkflow indicates an expected call of ListWorkflow.
func (mr *MockClientMockRecorder) ListWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListWorkflow", reflect.TypeOf((*MockClient)(nil).ListWorkflow), arg0, arg1)
}

// OperatorService mocks base method.
func (m *MockClient) OperatorService() operatorservice.OperatorServiceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperatorService")
	ret0, _ := ret[0].(operatorservice.OperatorServiceClient)
	return ret0
}

// OperatorService indicates an expected call of OperatorService.
func (mr *MockClientMockRecorder) OperatorService() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperatorService", reflect.TypeOf((*MockClient)(nil).OperatorService))
}

// QueryWorkflow mocks base method.
func (m *MockClient) QueryWorkflow(arg0 context.Context, arg1, arg2, arg3 string, arg4 ...interface{}) (converter.EncodedValue, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3}
	for _, a := range arg4 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryWorkflow", varargs...)
	ret0, _ := ret[0].(converter.EncodedValue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow.
func (mr *MockClientMockRecorder) QueryWorkflow(arg0, arg1, arg2, arg3 interface{}, arg4 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3}, arg4...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockClient)(nil).QueryWorkflow), varargs...)
}

// QueryWorkflowWithOptions mocks base method.
func (m *MockClient) QueryWorkflowWithOptions(arg0 context.Context, arg1 *client.QueryWorkflowWithOptionsRequest) (*client.QueryWorkflowWithOptionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkflowWithOptions", arg0, arg1)
	ret0, _ := ret[0].(*client.QueryWorkflowWithOptionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflowWithOptions indicates an expected call of QueryWorkflowWithOptions.
func (mr *MockClientMockRecorder) QueryWorkflowWithOptions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflowWithOptions", reflect.TypeOf((*MockClient)(nil).QueryWorkflowWithOptions), arg0, arg1)
}

// RecordActivityHeartbeat mocks base method.
func (m *MockClient) RecordActivityHeartbeat(arg0 context.Context, arg1 []byte, arg2 ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RecordActivityHeartbeat", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecordActivityHeartbeat indicates an expected call of RecordActivityHeartbeat.
func (mr *MockClientMockRecorder) RecordActivityHeartbeat(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityHeartbeat", reflect.TypeOf((*MockClient)(nil).RecordActivityHeartbeat), varargs...)
}

// RecordActivityHeartbeatByID mocks base method.
func (m *MockClient) RecordActivityHeartbeatByID(arg0 context.Context, arg1, arg2, arg3, arg4 string, arg5 ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3, arg4}
	for _, a := range arg5 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RecordActivityHeartbeatByID", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecordActivityHeartbeatByID indicates an expected call of RecordActivityHeartbeatByID.
func (mr *MockClientMockRecorder) RecordActivityHeartbeatByID(arg0, arg1, arg2, arg3, arg4 interface{}, arg5 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3, arg4}, arg5...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityHeartbeatByID", reflect.TypeOf((*MockClient)(nil).RecordActivityHeartbeatByID), varargs...)
}

// ResetWorkflowExecution mocks base method.
func (m *MockClient) ResetWorkflowExecution(arg0 context.Context, arg1 *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetWorkflowExecution", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ResetWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetWorkflowExecution indicates an expected call of ResetWorkflowExecution.
func (mr *MockClientMockRecorder) ResetWorkflowExecution(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetWorkflowExecution", reflect.TypeOf((*MockClient)(nil).ResetWorkflowExecution), arg0, arg1)
}

// ScanWorkflow mocks base method.
func (m *MockClient) ScanWorkflow(arg0 context.Context, arg1 *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScanWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*workflowservice.ScanWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ScanWorkflow indicates an expected call of ScanWorkflow.
func (mr *MockClientMockRecorder) ScanWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScanWorkflow", reflect.TypeOf((*MockClient)(nil).ScanWorkflow), arg0, arg1)
}

// ScheduleClient mocks base method.
func (m *MockClient) ScheduleClient() client.ScheduleClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScheduleClient")
	ret0, _ := ret[0].(client.ScheduleClient)
	return ret0
}

// ScheduleClient indicates an expected call of ScheduleClient.
func (mr *MockClientMockRecorder) ScheduleClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScheduleClient", reflect.TypeOf((*MockClient)(nil).ScheduleClient))
}

// SignalWithStartWorkflow mocks base method.
func (m *MockClient) SignalWithStartWorkflow(arg0 context.Context, arg1, arg2 string, arg3 interface{}, arg4 client.StartWorkflowOptions, arg5 interface{}, arg6 ...interface{}) (client.WorkflowRun, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3, arg4, arg5}
	for _, a := range arg6 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SignalWithStartWorkflow", varargs...)
	ret0, _ := ret[0].(client.WorkflowRun)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SignalWithStartWorkflow indicates an expected call of SignalWithStartWorkflow.
func (mr *MockClientMockRecorder) SignalWithStartWorkflow(arg0, arg1, arg2, arg3, arg4, arg5 interface{}, arg6 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3, arg4, arg5}, arg6...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWithStartWorkflow", reflect.TypeOf((*MockClient)(nil).SignalWithStartWorkflow), varargs...)
}

// SignalWorkflow mocks base method.
func (m *MockClient) SignalWorkflow(arg0 context.Context, arg1, arg2, arg3 string, arg4 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWorkflow", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// SignalWorkflow indicates an expected call of SignalWorkflow.
func (mr *MockClientMockRecorder) SignalWorkflow(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWorkflow", reflect.TypeOf((*MockClient)(nil).SignalWorkflow), arg0, arg1, arg2, arg3, arg4)
}

// TerminateWorkflow mocks base method.
func (m *MockClient) TerminateWorkflow(arg0 context.Context, arg1, arg2, arg3 string, arg4 ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3}
	for _, a := range arg4 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "TerminateWorkflow", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// TerminateWorkflow indicates an expected call of TerminateWorkflow.
func (mr *MockClientMockRecorder) TerminateWorkflow(arg0, arg1, arg2, arg3 interface{}, arg4 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3}, arg4...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateWorkflow", reflect.TypeOf((*MockClient)(nil).TerminateWorkflow), varargs...)
}

// UpdateWorkerBuildIdCompatibility mocks base method.
func (m *MockClient) UpdateWorkerBuildIdCompatibility(arg0 context.Context, arg1 *client.UpdateWorkerBuildIdCompatibilityOptions) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkerBuildIdCompatibility", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateWorkerBuildIdCompatibility indicates an expected call of UpdateWorkerBuildIdCompatibility.
func (mr *MockClientMockRecorder) UpdateWorkerBuildIdCompatibility(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerBuildIdCompatibility", reflect.TypeOf((*MockClient)(nil).UpdateWorkerBuildIdCompatibility), arg0, arg1)
}

// UpdateWorkerVersioningRules mocks base method.
func (m *MockClient) UpdateWorkerVersioningRules(arg0 context.Context, arg1 client.UpdateWorkerVersioningRulesOptions) (*client.WorkerVersioningRules, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkerVersioningRules", arg0, arg1)
	ret0, _ := ret[0].(*client.WorkerVersioningRules)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkerVersioningRules indicates an expected call of UpdateWorkerVersioningRules.
func (mr *MockClientMockRecorder) UpdateWorkerVersioningRules(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerVersioningRules", reflect.TypeOf((*MockClient)(nil).UpdateWorkerVersioningRules), arg0, arg1)
}

// UpdateWorkflow mocks base method.
func (m *MockClient) UpdateWorkflow(arg0 context.Context, arg1 client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkflow", arg0, arg1)
	ret0, _ := ret[0].(client.WorkflowUpdateHandle)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkflow indicates an expected call of UpdateWorkflow.
func (mr *MockClientMockRecorder) UpdateWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkflow", reflect.TypeOf((*MockClient)(nil).UpdateWorkflow), arg0, arg1)
}

// WorkflowService mocks base method.
func (m *MockClient) WorkflowService() workflowservice.WorkflowServiceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WorkflowService")
	ret0, _ := ret[0].(workflowservice.WorkflowServiceClient)
	return ret0
}

// WorkflowService indicates an expected call of WorkflowService.
func (mr *MockClientMockRecorder) WorkflowService() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WorkflowService", reflect.TypeOf((*MockClient)(nil).WorkflowService))
}
