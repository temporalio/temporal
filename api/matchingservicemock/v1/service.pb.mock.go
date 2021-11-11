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
// Source: matchingservice/v1/service.pb.go

// Package matchingservicemock is a generated GoMock package.
package matchingservicemock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "go.temporal.io/server/api/matchingservice/v1"
	grpc "google.golang.org/grpc"
)

// MockMatchingServiceClient is a mock of MatchingServiceClient interface.
type MockMatchingServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockMatchingServiceClientMockRecorder
}

// MockMatchingServiceClientMockRecorder is the mock recorder for MockMatchingServiceClient.
type MockMatchingServiceClientMockRecorder struct {
	mock *MockMatchingServiceClient
}

// NewMockMatchingServiceClient creates a new mock instance.
func NewMockMatchingServiceClient(ctrl *gomock.Controller) *MockMatchingServiceClient {
	mock := &MockMatchingServiceClient{ctrl: ctrl}
	mock.recorder = &MockMatchingServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMatchingServiceClient) EXPECT() *MockMatchingServiceClientMockRecorder {
	return m.recorder
}

// AddActivityTask mocks base method.
func (m *MockMatchingServiceClient) AddActivityTask(ctx context.Context, in *v1.AddActivityTaskRequest, opts ...grpc.CallOption) (*v1.AddActivityTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AddActivityTask", varargs...)
	ret0, _ := ret[0].(*v1.AddActivityTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddActivityTask indicates an expected call of AddActivityTask.
func (mr *MockMatchingServiceClientMockRecorder) AddActivityTask(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddActivityTask", reflect.TypeOf((*MockMatchingServiceClient)(nil).AddActivityTask), varargs...)
}

// AddWorkflowTask mocks base method.
func (m *MockMatchingServiceClient) AddWorkflowTask(ctx context.Context, in *v1.AddWorkflowTaskRequest, opts ...grpc.CallOption) (*v1.AddWorkflowTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AddWorkflowTask", varargs...)
	ret0, _ := ret[0].(*v1.AddWorkflowTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTask indicates an expected call of AddWorkflowTask.
func (mr *MockMatchingServiceClientMockRecorder) AddWorkflowTask(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTask", reflect.TypeOf((*MockMatchingServiceClient)(nil).AddWorkflowTask), varargs...)
}

// CancelOutstandingPoll mocks base method.
func (m *MockMatchingServiceClient) CancelOutstandingPoll(ctx context.Context, in *v1.CancelOutstandingPollRequest, opts ...grpc.CallOption) (*v1.CancelOutstandingPollResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CancelOutstandingPoll", varargs...)
	ret0, _ := ret[0].(*v1.CancelOutstandingPollResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CancelOutstandingPoll indicates an expected call of CancelOutstandingPoll.
func (mr *MockMatchingServiceClientMockRecorder) CancelOutstandingPoll(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelOutstandingPoll", reflect.TypeOf((*MockMatchingServiceClient)(nil).CancelOutstandingPoll), varargs...)
}

// DescribeTaskQueue mocks base method.
func (m *MockMatchingServiceClient) DescribeTaskQueue(ctx context.Context, in *v1.DescribeTaskQueueRequest, opts ...grpc.CallOption) (*v1.DescribeTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DescribeTaskQueue", varargs...)
	ret0, _ := ret[0].(*v1.DescribeTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueue indicates an expected call of DescribeTaskQueue.
func (mr *MockMatchingServiceClientMockRecorder) DescribeTaskQueue(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueue", reflect.TypeOf((*MockMatchingServiceClient)(nil).DescribeTaskQueue), varargs...)
}

// ListTaskQueuePartitions mocks base method.
func (m *MockMatchingServiceClient) ListTaskQueuePartitions(ctx context.Context, in *v1.ListTaskQueuePartitionsRequest, opts ...grpc.CallOption) (*v1.ListTaskQueuePartitionsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListTaskQueuePartitions", varargs...)
	ret0, _ := ret[0].(*v1.ListTaskQueuePartitionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTaskQueuePartitions indicates an expected call of ListTaskQueuePartitions.
func (mr *MockMatchingServiceClientMockRecorder) ListTaskQueuePartitions(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTaskQueuePartitions", reflect.TypeOf((*MockMatchingServiceClient)(nil).ListTaskQueuePartitions), varargs...)
}

// PollActivityTaskQueue mocks base method.
func (m *MockMatchingServiceClient) PollActivityTaskQueue(ctx context.Context, in *v1.PollActivityTaskQueueRequest, opts ...grpc.CallOption) (*v1.PollActivityTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PollActivityTaskQueue", varargs...)
	ret0, _ := ret[0].(*v1.PollActivityTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollActivityTaskQueue indicates an expected call of PollActivityTaskQueue.
func (mr *MockMatchingServiceClientMockRecorder) PollActivityTaskQueue(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollActivityTaskQueue", reflect.TypeOf((*MockMatchingServiceClient)(nil).PollActivityTaskQueue), varargs...)
}

// PollWorkflowTaskQueue mocks base method.
func (m *MockMatchingServiceClient) PollWorkflowTaskQueue(ctx context.Context, in *v1.PollWorkflowTaskQueueRequest, opts ...grpc.CallOption) (*v1.PollWorkflowTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PollWorkflowTaskQueue", varargs...)
	ret0, _ := ret[0].(*v1.PollWorkflowTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollWorkflowTaskQueue indicates an expected call of PollWorkflowTaskQueue.
func (mr *MockMatchingServiceClientMockRecorder) PollWorkflowTaskQueue(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollWorkflowTaskQueue", reflect.TypeOf((*MockMatchingServiceClient)(nil).PollWorkflowTaskQueue), varargs...)
}

// QueryWorkflow mocks base method.
func (m *MockMatchingServiceClient) QueryWorkflow(ctx context.Context, in *v1.QueryWorkflowRequest, opts ...grpc.CallOption) (*v1.QueryWorkflowResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryWorkflow", varargs...)
	ret0, _ := ret[0].(*v1.QueryWorkflowResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow.
func (mr *MockMatchingServiceClientMockRecorder) QueryWorkflow(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockMatchingServiceClient)(nil).QueryWorkflow), varargs...)
}

// RespondQueryTaskCompleted mocks base method.
func (m *MockMatchingServiceClient) RespondQueryTaskCompleted(ctx context.Context, in *v1.RespondQueryTaskCompletedRequest, opts ...grpc.CallOption) (*v1.RespondQueryTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RespondQueryTaskCompleted", varargs...)
	ret0, _ := ret[0].(*v1.RespondQueryTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondQueryTaskCompleted indicates an expected call of RespondQueryTaskCompleted.
func (mr *MockMatchingServiceClientMockRecorder) RespondQueryTaskCompleted(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondQueryTaskCompleted", reflect.TypeOf((*MockMatchingServiceClient)(nil).RespondQueryTaskCompleted), varargs...)
}

// MockMatchingServiceServer is a mock of MatchingServiceServer interface.
type MockMatchingServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockMatchingServiceServerMockRecorder
}

// MockMatchingServiceServerMockRecorder is the mock recorder for MockMatchingServiceServer.
type MockMatchingServiceServerMockRecorder struct {
	mock *MockMatchingServiceServer
}

// NewMockMatchingServiceServer creates a new mock instance.
func NewMockMatchingServiceServer(ctrl *gomock.Controller) *MockMatchingServiceServer {
	mock := &MockMatchingServiceServer{ctrl: ctrl}
	mock.recorder = &MockMatchingServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMatchingServiceServer) EXPECT() *MockMatchingServiceServerMockRecorder {
	return m.recorder
}

// AddActivityTask mocks base method.
func (m *MockMatchingServiceServer) AddActivityTask(arg0 context.Context, arg1 *v1.AddActivityTaskRequest) (*v1.AddActivityTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddActivityTask", arg0, arg1)
	ret0, _ := ret[0].(*v1.AddActivityTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddActivityTask indicates an expected call of AddActivityTask.
func (mr *MockMatchingServiceServerMockRecorder) AddActivityTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddActivityTask", reflect.TypeOf((*MockMatchingServiceServer)(nil).AddActivityTask), arg0, arg1)
}

// AddWorkflowTask mocks base method.
func (m *MockMatchingServiceServer) AddWorkflowTask(arg0 context.Context, arg1 *v1.AddWorkflowTaskRequest) (*v1.AddWorkflowTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddWorkflowTask", arg0, arg1)
	ret0, _ := ret[0].(*v1.AddWorkflowTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddWorkflowTask indicates an expected call of AddWorkflowTask.
func (mr *MockMatchingServiceServerMockRecorder) AddWorkflowTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddWorkflowTask", reflect.TypeOf((*MockMatchingServiceServer)(nil).AddWorkflowTask), arg0, arg1)
}

// CancelOutstandingPoll mocks base method.
func (m *MockMatchingServiceServer) CancelOutstandingPoll(arg0 context.Context, arg1 *v1.CancelOutstandingPollRequest) (*v1.CancelOutstandingPollResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelOutstandingPoll", arg0, arg1)
	ret0, _ := ret[0].(*v1.CancelOutstandingPollResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CancelOutstandingPoll indicates an expected call of CancelOutstandingPoll.
func (mr *MockMatchingServiceServerMockRecorder) CancelOutstandingPoll(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelOutstandingPoll", reflect.TypeOf((*MockMatchingServiceServer)(nil).CancelOutstandingPoll), arg0, arg1)
}

// DescribeTaskQueue mocks base method.
func (m *MockMatchingServiceServer) DescribeTaskQueue(arg0 context.Context, arg1 *v1.DescribeTaskQueueRequest) (*v1.DescribeTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.DescribeTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeTaskQueue indicates an expected call of DescribeTaskQueue.
func (mr *MockMatchingServiceServerMockRecorder) DescribeTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeTaskQueue", reflect.TypeOf((*MockMatchingServiceServer)(nil).DescribeTaskQueue), arg0, arg1)
}

// ListTaskQueuePartitions mocks base method.
func (m *MockMatchingServiceServer) ListTaskQueuePartitions(arg0 context.Context, arg1 *v1.ListTaskQueuePartitionsRequest) (*v1.ListTaskQueuePartitionsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListTaskQueuePartitions", arg0, arg1)
	ret0, _ := ret[0].(*v1.ListTaskQueuePartitionsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListTaskQueuePartitions indicates an expected call of ListTaskQueuePartitions.
func (mr *MockMatchingServiceServerMockRecorder) ListTaskQueuePartitions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListTaskQueuePartitions", reflect.TypeOf((*MockMatchingServiceServer)(nil).ListTaskQueuePartitions), arg0, arg1)
}

// PollActivityTaskQueue mocks base method.
func (m *MockMatchingServiceServer) PollActivityTaskQueue(arg0 context.Context, arg1 *v1.PollActivityTaskQueueRequest) (*v1.PollActivityTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollActivityTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.PollActivityTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollActivityTaskQueue indicates an expected call of PollActivityTaskQueue.
func (mr *MockMatchingServiceServerMockRecorder) PollActivityTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollActivityTaskQueue", reflect.TypeOf((*MockMatchingServiceServer)(nil).PollActivityTaskQueue), arg0, arg1)
}

// PollWorkflowTaskQueue mocks base method.
func (m *MockMatchingServiceServer) PollWorkflowTaskQueue(arg0 context.Context, arg1 *v1.PollWorkflowTaskQueueRequest) (*v1.PollWorkflowTaskQueueResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollWorkflowTaskQueue", arg0, arg1)
	ret0, _ := ret[0].(*v1.PollWorkflowTaskQueueResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PollWorkflowTaskQueue indicates an expected call of PollWorkflowTaskQueue.
func (mr *MockMatchingServiceServerMockRecorder) PollWorkflowTaskQueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollWorkflowTaskQueue", reflect.TypeOf((*MockMatchingServiceServer)(nil).PollWorkflowTaskQueue), arg0, arg1)
}

// QueryWorkflow mocks base method.
func (m *MockMatchingServiceServer) QueryWorkflow(arg0 context.Context, arg1 *v1.QueryWorkflowRequest) (*v1.QueryWorkflowResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkflow", arg0, arg1)
	ret0, _ := ret[0].(*v1.QueryWorkflowResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkflow indicates an expected call of QueryWorkflow.
func (mr *MockMatchingServiceServerMockRecorder) QueryWorkflow(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkflow", reflect.TypeOf((*MockMatchingServiceServer)(nil).QueryWorkflow), arg0, arg1)
}

// RespondQueryTaskCompleted mocks base method.
func (m *MockMatchingServiceServer) RespondQueryTaskCompleted(arg0 context.Context, arg1 *v1.RespondQueryTaskCompletedRequest) (*v1.RespondQueryTaskCompletedResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RespondQueryTaskCompleted", arg0, arg1)
	ret0, _ := ret[0].(*v1.RespondQueryTaskCompletedResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RespondQueryTaskCompleted indicates an expected call of RespondQueryTaskCompleted.
func (mr *MockMatchingServiceServerMockRecorder) RespondQueryTaskCompleted(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RespondQueryTaskCompleted", reflect.TypeOf((*MockMatchingServiceServer)(nil).RespondQueryTaskCompleted), arg0, arg1)
}
