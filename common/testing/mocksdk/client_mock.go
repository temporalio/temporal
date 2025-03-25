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
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package mocksdk go.temporal.io/sdk/client Client
//

// Package mocksdk is a generated GoMock package.
package mocksdk

import (
	context "context"
	reflect "reflect"

	enums "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	client "go.temporal.io/sdk/client"
	converter "go.temporal.io/sdk/converter"
	gomock "go.uber.org/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
	isgomock struct{}
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
func (m *MockClient) CancelWorkflow(ctx context.Context, workflowID, runID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelWorkflow", ctx, workflowID, runID)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelWorkflow indicates an expected call of CancelWorkflow.
func (mr *MockClientMockRecorder) CancelWorkflow(ctx, workflowID, runID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelWorkflow", reflect.TypeOf((*MockClient)(nil).CancelWorkflow), ctx, workflowID, runID)
}

// CheckHealth mocks base method.
func (m *MockClient) CheckHealth(ctx context.Context, request *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckHealth", ctx, request)
	ret0, _ := ret[0].(*client.CheckHealthResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckHealth indicates an expected call of CheckHealth.
func (mr *MockClientMockRecorder) CheckHealth(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckHealth", reflect.TypeOf((*MockClient)(nil).CheckHealth), ctx, request)
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
func (m *MockClient) CompleteActivity(ctx context.Context, taskToken []byte, result any, err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteActivity", ctx, taskToken, result, err)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompleteActivity indicates an expected call of CompleteActivity.
func (mr *MockClientMockRecorder) CompleteActivity(ctx, taskToken, result, err any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteActivity", reflect.TypeOf((*MockClient)(nil).CompleteActivity), ctx, taskToken, result, err)
}

// CompleteActivityByID mocks base method.
func (m *MockClient) CompleteActivityByID(ctx context.Context, namespace, workflowID, runID, activityID string, result any, err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteActivityByID", ctx, namespace, workflowID, runID, activityID, result, err)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompleteActivityByID indicates an expected call of CompleteActivityByID.
func (mr *MockClientMockRecorder) CompleteActivityByID(ctx, namespace, workflowID, runID, activityID, result, err any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteActivityByID", reflect.TypeOf((*MockClient)(nil).CompleteActivityByID), ctx, namespace, workflowID, runID, activityID, result, err)
}

// CountWorkflow mocks base method.
func (m *MockClient) CountWorkflow(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.CountWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountWorkflow indicates an expected call of CountWorkflow.
func (mr *MockClientMockRecorder) CountWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountWorkflow", reflect.TypeOf((*MockClient)(nil).CountWorkflow), ctx, request)
}

// DeploymentClient mocks base method.
func (m *MockClient) DeploymentClient() client.DeploymentClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeploymentClient")
	ret0, _ := ret[0].(client.DeploymentClient)
	return ret0
}

// DeploymentClient indicates an expected call of DeploymentClient.
func (mr *MockClientMockRecorder) DeploymentClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeploymentClient", reflect.TypeOf((*MockClient)(nil).DeploymentClient))
}

// DescribeTaskQueue mocks base method.
func (m *MockClient) DescribeTaskQueue(ctx context.Context, taskqueue string, taskqueueType enums.TaskQueueType) (*workflowservice.DescribeTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueue", ctx, taskqueue, taskqueueType)
	ret0, _ := ret[0].(*workflowservice.DescribeTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueue indicates an expected call of DescribeTaskQueue.
func (mr *MockClientMockRecorder) DescribeTaskQueue(ctx, taskqueue, taskqueueType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueue", reflect.TypeOf((*MockClient)(nil).DescribeTaskQueue), ctx, taskqueue, taskqueueType)
}

// DescribeTaskQueueEnhanced mocks base method.
func (m *MockClient) DescribeTaskQueueEnhanced(ctx context.Context, options client.DescribeTaskQueueEnhancedOptions) (client.TaskQueueDescription, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueueEnhanced", ctx, options)
	ret0, _ := ret[0].(client.TaskQueueDescription)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueueEnhanced indicates an expected call of DescribeTaskQueueEnhanced.
func (mr *MockClientMockRecorder) DescribeTaskQueueEnhanced(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueueEnhanced", reflect.TypeOf((*MockClient)(nil).DescribeTaskQueueEnhanced), ctx, options)
}

// DescribeWorkflowExecution mocks base method.
func (m *MockClient) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeWorkflowExecution", ctx, workflowID, runID)
	ret0, _ := ret[0].(*workflowservice.DescribeWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeWorkflowExecution indicates an expected call of DescribeWorkflowExecution.
func (mr *MockClientMockRecorder) DescribeWorkflowExecution(ctx, workflowID, runID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeWorkflowExecution", reflect.TypeOf((*MockClient)(nil).DescribeWorkflowExecution), ctx, workflowID, runID)
}

// ExecuteWorkflow mocks base method.
func (m *MockClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, options, workflow}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteWorkflow", varargs...)
	ret0, _ := ret[0].(client.WorkflowRun)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteWorkflow indicates an expected call of ExecuteWorkflow.
func (mr *MockClientMockRecorder) ExecuteWorkflow(ctx, options, workflow any, args ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, options, workflow}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteWorkflow", reflect.TypeOf((*MockClient)(nil).ExecuteWorkflow), varargs...)
}

// GetSearchAttributes mocks base method.
func (m *MockClient) GetSearchAttributes(ctx context.Context) (*workflowservice.GetSearchAttributesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", ctx)
	ret0, _ := ret[0].(*workflowservice.GetSearchAttributesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes.
func (mr *MockClientMockRecorder) GetSearchAttributes(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockClient)(nil).GetSearchAttributes), ctx)
}

// GetWorkerBuildIdCompatibility mocks base method.
func (m *MockClient) GetWorkerBuildIdCompatibility(ctx context.Context, options *client.GetWorkerBuildIdCompatibilityOptions) (*client.WorkerBuildIDVersionSets, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerBuildIdCompatibility", ctx, options)
	ret0, _ := ret[0].(*client.WorkerBuildIDVersionSets)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerBuildIdCompatibility indicates an expected call of GetWorkerBuildIdCompatibility.
func (mr *MockClientMockRecorder) GetWorkerBuildIdCompatibility(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerBuildIdCompatibility", reflect.TypeOf((*MockClient)(nil).GetWorkerBuildIdCompatibility), ctx, options)
}

// GetWorkerTaskReachability mocks base method.
func (m *MockClient) GetWorkerTaskReachability(ctx context.Context, options *client.GetWorkerTaskReachabilityOptions) (*client.WorkerTaskReachability, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerTaskReachability", ctx, options)
	ret0, _ := ret[0].(*client.WorkerTaskReachability)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerTaskReachability indicates an expected call of GetWorkerTaskReachability.
func (mr *MockClientMockRecorder) GetWorkerTaskReachability(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerTaskReachability", reflect.TypeOf((*MockClient)(nil).GetWorkerTaskReachability), ctx, options)
}

// GetWorkerVersioningRules mocks base method.
func (m *MockClient) GetWorkerVersioningRules(ctx context.Context, options client.GetWorkerVersioningOptions) (*client.WorkerVersioningRules, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerVersioningRules", ctx, options)
	ret0, _ := ret[0].(*client.WorkerVersioningRules)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerVersioningRules indicates an expected call of GetWorkerVersioningRules.
func (mr *MockClientMockRecorder) GetWorkerVersioningRules(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerVersioningRules", reflect.TypeOf((*MockClient)(nil).GetWorkerVersioningRules), ctx, options)
}

// GetWorkflow mocks base method.
func (m *MockClient) GetWorkflow(ctx context.Context, workflowID, runID string) client.WorkflowRun {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflow", ctx, workflowID, runID)
	ret0, _ := ret[0].(client.WorkflowRun)
	return ret0
}

// GetWorkflow indicates an expected call of GetWorkflow.
func (mr *MockClientMockRecorder) GetWorkflow(ctx, workflowID, runID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflow", reflect.TypeOf((*MockClient)(nil).GetWorkflow), ctx, workflowID, runID)
}

// GetWorkflowHistory mocks base method.
func (m *MockClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool, filterType enums.HistoryEventFilterType) client.HistoryEventIterator {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowHistory", ctx, workflowID, runID, isLongPoll, filterType)
	ret0, _ := ret[0].(client.HistoryEventIterator)
	return ret0
}

// GetWorkflowHistory indicates an expected call of GetWorkflowHistory.
func (mr *MockClientMockRecorder) GetWorkflowHistory(ctx, workflowID, runID, isLongPoll, filterType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowHistory", reflect.TypeOf((*MockClient)(nil).GetWorkflowHistory), ctx, workflowID, runID, isLongPoll, filterType)
}

// GetWorkflowUpdateHandle mocks base method.
func (m *MockClient) GetWorkflowUpdateHandle(ref client.GetWorkflowUpdateHandleOptions) client.WorkflowUpdateHandle {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowUpdateHandle", ref)
	ret0, _ := ret[0].(client.WorkflowUpdateHandle)
	return ret0
}

// GetWorkflowUpdateHandle indicates an expected call of GetWorkflowUpdateHandle.
func (mr *MockClientMockRecorder) GetWorkflowUpdateHandle(ref any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowUpdateHandle", reflect.TypeOf((*MockClient)(nil).GetWorkflowUpdateHandle), ref)
}

// ListArchivedWorkflow mocks base method.
func (m *MockClient) ListArchivedWorkflow(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (*workflowservice.ListArchivedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListArchivedWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ListArchivedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListArchivedWorkflow indicates an expected call of ListArchivedWorkflow.
func (mr *MockClientMockRecorder) ListArchivedWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListArchivedWorkflow", reflect.TypeOf((*MockClient)(nil).ListArchivedWorkflow), ctx, request)
}

// ListClosedWorkflow mocks base method.
func (m *MockClient) ListClosedWorkflow(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (*workflowservice.ListClosedWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListClosedWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ListClosedWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListClosedWorkflow indicates an expected call of ListClosedWorkflow.
func (mr *MockClientMockRecorder) ListClosedWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListClosedWorkflow", reflect.TypeOf((*MockClient)(nil).ListClosedWorkflow), ctx, request)
}

// ListOpenWorkflow mocks base method.
func (m *MockClient) ListOpenWorkflow(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListOpenWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ListOpenWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListOpenWorkflow indicates an expected call of ListOpenWorkflow.
func (mr *MockClientMockRecorder) ListOpenWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListOpenWorkflow", reflect.TypeOf((*MockClient)(nil).ListOpenWorkflow), ctx, request)
}

// ListWorkflow mocks base method.
func (m *MockClient) ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ListWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListWorkflow indicates an expected call of ListWorkflow.
func (mr *MockClientMockRecorder) ListWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListWorkflow", reflect.TypeOf((*MockClient)(nil).ListWorkflow), ctx, request)
}

// NewWithStartWorkflowOperation mocks base method.
func (m *MockClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, workflow any, args ...any) client.WithStartWorkflowOperation {
	m.ctrl.T.Helper()
	varargs := []any{options, workflow}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "NewWithStartWorkflowOperation", varargs...)
	ret0, _ := ret[0].(client.WithStartWorkflowOperation)
	return ret0
}

// NewWithStartWorkflowOperation indicates an expected call of NewWithStartWorkflowOperation.
func (mr *MockClientMockRecorder) NewWithStartWorkflowOperation(options, workflow any, args ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{options, workflow}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewWithStartWorkflowOperation", reflect.TypeOf((*MockClient)(nil).NewWithStartWorkflowOperation), varargs...)
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
func (m *MockClient) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, workflowID, runID, queryType}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryWorkflow", varargs...)
	ret0, _ := ret[0].(converter.EncodedValue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow.
func (mr *MockClientMockRecorder) QueryWorkflow(ctx, workflowID, runID, queryType any, args ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, workflowID, runID, queryType}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockClient)(nil).QueryWorkflow), varargs...)
}

// QueryWorkflowWithOptions mocks base method.
func (m *MockClient) QueryWorkflowWithOptions(ctx context.Context, request *client.QueryWorkflowWithOptionsRequest) (*client.QueryWorkflowWithOptionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkflowWithOptions", ctx, request)
	ret0, _ := ret[0].(*client.QueryWorkflowWithOptionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflowWithOptions indicates an expected call of QueryWorkflowWithOptions.
func (mr *MockClientMockRecorder) QueryWorkflowWithOptions(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflowWithOptions", reflect.TypeOf((*MockClient)(nil).QueryWorkflowWithOptions), ctx, request)
}

// RecordActivityHeartbeat mocks base method.
func (m *MockClient) RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx, taskToken}
	for _, a := range details {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RecordActivityHeartbeat", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecordActivityHeartbeat indicates an expected call of RecordActivityHeartbeat.
func (mr *MockClientMockRecorder) RecordActivityHeartbeat(ctx, taskToken any, details ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, taskToken}, details...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityHeartbeat", reflect.TypeOf((*MockClient)(nil).RecordActivityHeartbeat), varargs...)
}

// RecordActivityHeartbeatByID mocks base method.
func (m *MockClient) RecordActivityHeartbeatByID(ctx context.Context, namespace, workflowID, runID, activityID string, details ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx, namespace, workflowID, runID, activityID}
	for _, a := range details {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RecordActivityHeartbeatByID", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecordActivityHeartbeatByID indicates an expected call of RecordActivityHeartbeatByID.
func (mr *MockClientMockRecorder) RecordActivityHeartbeatByID(ctx, namespace, workflowID, runID, activityID any, details ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, namespace, workflowID, runID, activityID}, details...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordActivityHeartbeatByID", reflect.TypeOf((*MockClient)(nil).RecordActivityHeartbeatByID), varargs...)
}

// ResetWorkflowExecution mocks base method.
func (m *MockClient) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResetWorkflowExecution", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ResetWorkflowExecutionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResetWorkflowExecution indicates an expected call of ResetWorkflowExecution.
func (mr *MockClientMockRecorder) ResetWorkflowExecution(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetWorkflowExecution", reflect.TypeOf((*MockClient)(nil).ResetWorkflowExecution), ctx, request)
}

// ScanWorkflow mocks base method.
func (m *MockClient) ScanWorkflow(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (*workflowservice.ScanWorkflowExecutionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScanWorkflow", ctx, request)
	ret0, _ := ret[0].(*workflowservice.ScanWorkflowExecutionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ScanWorkflow indicates an expected call of ScanWorkflow.
func (mr *MockClientMockRecorder) ScanWorkflow(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScanWorkflow", reflect.TypeOf((*MockClient)(nil).ScanWorkflow), ctx, request)
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
func (m *MockClient) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any, options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, workflowID, signalName, signalArg, options, workflow}
	for _, a := range workflowArgs {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SignalWithStartWorkflow", varargs...)
	ret0, _ := ret[0].(client.WorkflowRun)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SignalWithStartWorkflow indicates an expected call of SignalWithStartWorkflow.
func (mr *MockClientMockRecorder) SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow any, workflowArgs ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, workflowID, signalName, signalArg, options, workflow}, workflowArgs...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWithStartWorkflow", reflect.TypeOf((*MockClient)(nil).SignalWithStartWorkflow), varargs...)
}

// SignalWorkflow mocks base method.
func (m *MockClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SignalWorkflow", ctx, workflowID, runID, signalName, arg)
	ret0, _ := ret[0].(error)
	return ret0
}

// SignalWorkflow indicates an expected call of SignalWorkflow.
func (mr *MockClientMockRecorder) SignalWorkflow(ctx, workflowID, runID, signalName, arg any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SignalWorkflow", reflect.TypeOf((*MockClient)(nil).SignalWorkflow), ctx, workflowID, runID, signalName, arg)
}

// TerminateWorkflow mocks base method.
func (m *MockClient) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx, workflowID, runID, reason}
	for _, a := range details {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "TerminateWorkflow", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// TerminateWorkflow indicates an expected call of TerminateWorkflow.
func (mr *MockClientMockRecorder) TerminateWorkflow(ctx, workflowID, runID, reason any, details ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, workflowID, runID, reason}, details...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateWorkflow", reflect.TypeOf((*MockClient)(nil).TerminateWorkflow), varargs...)
}

// UpdateWithStartWorkflow mocks base method.
func (m *MockClient) UpdateWithStartWorkflow(ctx context.Context, options client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWithStartWorkflow", ctx, options)
	ret0, _ := ret[0].(client.WorkflowUpdateHandle)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWithStartWorkflow indicates an expected call of UpdateWithStartWorkflow.
func (mr *MockClientMockRecorder) UpdateWithStartWorkflow(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWithStartWorkflow", reflect.TypeOf((*MockClient)(nil).UpdateWithStartWorkflow), ctx, options)
}

// UpdateWorkerBuildIdCompatibility mocks base method.
func (m *MockClient) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *client.UpdateWorkerBuildIdCompatibilityOptions) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkerBuildIdCompatibility", ctx, options)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateWorkerBuildIdCompatibility indicates an expected call of UpdateWorkerBuildIdCompatibility.
func (mr *MockClientMockRecorder) UpdateWorkerBuildIdCompatibility(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerBuildIdCompatibility", reflect.TypeOf((*MockClient)(nil).UpdateWorkerBuildIdCompatibility), ctx, options)
}

// UpdateWorkerVersioningRules mocks base method.
func (m *MockClient) UpdateWorkerVersioningRules(ctx context.Context, options client.UpdateWorkerVersioningRulesOptions) (*client.WorkerVersioningRules, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkerVersioningRules", ctx, options)
	ret0, _ := ret[0].(*client.WorkerVersioningRules)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkerVersioningRules indicates an expected call of UpdateWorkerVersioningRules.
func (mr *MockClientMockRecorder) UpdateWorkerVersioningRules(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerVersioningRules", reflect.TypeOf((*MockClient)(nil).UpdateWorkerVersioningRules), ctx, options)
}

// UpdateWorkflow mocks base method.
func (m *MockClient) UpdateWorkflow(ctx context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkflow", ctx, options)
	ret0, _ := ret[0].(client.WorkflowUpdateHandle)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkflow indicates an expected call of UpdateWorkflow.
func (mr *MockClientMockRecorder) UpdateWorkflow(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkflow", reflect.TypeOf((*MockClient)(nil).UpdateWorkflow), ctx, options)
}

// UpdateWorkflowExecutionOptions mocks base method.
func (m *MockClient) UpdateWorkflowExecutionOptions(ctx context.Context, options client.UpdateWorkflowExecutionOptionsRequest) (client.WorkflowExecutionOptions, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkflowExecutionOptions", ctx, options)
	ret0, _ := ret[0].(client.WorkflowExecutionOptions)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkflowExecutionOptions indicates an expected call of UpdateWorkflowExecutionOptions.
func (mr *MockClientMockRecorder) UpdateWorkflowExecutionOptions(ctx, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkflowExecutionOptions", reflect.TypeOf((*MockClient)(nil).UpdateWorkflowExecutionOptions), ctx, options)
}

// WorkerDeploymentClient mocks base method.
func (m *MockClient) WorkerDeploymentClient() client.WorkerDeploymentClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WorkerDeploymentClient")
	ret0, _ := ret[0].(client.WorkerDeploymentClient)
	return ret0
}

// WorkerDeploymentClient indicates an expected call of WorkerDeploymentClient.
func (mr *MockClientMockRecorder) WorkerDeploymentClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WorkerDeploymentClient", reflect.TypeOf((*MockClient)(nil).WorkerDeploymentClient))
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
