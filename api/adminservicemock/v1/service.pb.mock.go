// Code generated by MockGen. DO NOT EDIT.
// Source: adminservice/v1/service.pb.go

// Package adminservicemock is a generated GoMock package.
package adminservicemock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	adminservice "go.temporal.io/server/api/adminservice/v1"
	grpc "google.golang.org/grpc"
)

// MockAdminServiceClient is a mock of AdminServiceClient interface.
type MockAdminServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockAdminServiceClientMockRecorder
}

// MockAdminServiceClientMockRecorder is the mock recorder for MockAdminServiceClient.
type MockAdminServiceClientMockRecorder struct {
	mock *MockAdminServiceClient
}

// NewMockAdminServiceClient creates a new mock instance.
func NewMockAdminServiceClient(ctrl *gomock.Controller) *MockAdminServiceClient {
	mock := &MockAdminServiceClient{ctrl: ctrl}
	mock.recorder = &MockAdminServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAdminServiceClient) EXPECT() *MockAdminServiceClientMockRecorder {
	return m.recorder
}

// AddSearchAttribute mocks base method.
func (m *MockAdminServiceClient) AddSearchAttribute(ctx context.Context, in *adminservice.AddSearchAttributeRequest, opts ...grpc.CallOption) (*adminservice.AddSearchAttributeResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AddSearchAttribute", varargs...)
	ret0, _ := ret[0].(*adminservice.AddSearchAttributeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddSearchAttribute indicates an expected call of AddSearchAttribute.
func (mr *MockAdminServiceClientMockRecorder) AddSearchAttribute(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddSearchAttribute", reflect.TypeOf((*MockAdminServiceClient)(nil).AddSearchAttribute), varargs...)
}

// CloseShard mocks base method.
func (m *MockAdminServiceClient) CloseShard(ctx context.Context, in *adminservice.CloseShardRequest, opts ...grpc.CallOption) (*adminservice.CloseShardResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CloseShard", varargs...)
	ret0, _ := ret[0].(*adminservice.CloseShardResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CloseShard indicates an expected call of CloseShard.
func (mr *MockAdminServiceClientMockRecorder) CloseShard(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseShard", reflect.TypeOf((*MockAdminServiceClient)(nil).CloseShard), varargs...)
}

// DescribeCluster mocks base method.
func (m *MockAdminServiceClient) DescribeCluster(ctx context.Context, in *adminservice.DescribeClusterRequest, opts ...grpc.CallOption) (*adminservice.DescribeClusterResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DescribeCluster", varargs...)
	ret0, _ := ret[0].(*adminservice.DescribeClusterResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeCluster indicates an expected call of DescribeCluster.
func (mr *MockAdminServiceClientMockRecorder) DescribeCluster(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeCluster", reflect.TypeOf((*MockAdminServiceClient)(nil).DescribeCluster), varargs...)
}

// DescribeHistoryHost mocks base method.
func (m *MockAdminServiceClient) DescribeHistoryHost(ctx context.Context, in *adminservice.DescribeHistoryHostRequest, opts ...grpc.CallOption) (*adminservice.DescribeHistoryHostResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DescribeHistoryHost", varargs...)
	ret0, _ := ret[0].(*adminservice.DescribeHistoryHostResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeHistoryHost indicates an expected call of DescribeHistoryHost.
func (mr *MockAdminServiceClientMockRecorder) DescribeHistoryHost(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeHistoryHost", reflect.TypeOf((*MockAdminServiceClient)(nil).DescribeHistoryHost), varargs...)
}

// DescribeMutableState mocks base method.
func (m *MockAdminServiceClient) DescribeMutableState(ctx context.Context, in *adminservice.DescribeMutableStateRequest, opts ...grpc.CallOption) (*adminservice.DescribeMutableStateResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DescribeMutableState", varargs...)
	ret0, _ := ret[0].(*adminservice.DescribeMutableStateResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeMutableState indicates an expected call of DescribeMutableState.
func (mr *MockAdminServiceClientMockRecorder) DescribeMutableState(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeMutableState", reflect.TypeOf((*MockAdminServiceClient)(nil).DescribeMutableState), varargs...)
}

// GetDLQMessages mocks base method.
func (m *MockAdminServiceClient) GetDLQMessages(ctx context.Context, in *adminservice.GetDLQMessagesRequest, opts ...grpc.CallOption) (*adminservice.GetDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetDLQMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.GetDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDLQMessages indicates an expected call of GetDLQMessages.
func (mr *MockAdminServiceClientMockRecorder) GetDLQMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDLQMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).GetDLQMessages), varargs...)
}

// GetDLQReplicationMessages mocks base method.
func (m *MockAdminServiceClient) GetDLQReplicationMessages(ctx context.Context, in *adminservice.GetDLQReplicationMessagesRequest, opts ...grpc.CallOption) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetDLQReplicationMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.GetDLQReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDLQReplicationMessages indicates an expected call of GetDLQReplicationMessages.
func (mr *MockAdminServiceClientMockRecorder) GetDLQReplicationMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDLQReplicationMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).GetDLQReplicationMessages), varargs...)
}

// GetNamespaceReplicationMessages mocks base method.
func (m *MockAdminServiceClient) GetNamespaceReplicationMessages(ctx context.Context, in *adminservice.GetNamespaceReplicationMessagesRequest, opts ...grpc.CallOption) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetNamespaceReplicationMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.GetNamespaceReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceReplicationMessages indicates an expected call of GetNamespaceReplicationMessages.
func (mr *MockAdminServiceClientMockRecorder) GetNamespaceReplicationMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceReplicationMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).GetNamespaceReplicationMessages), varargs...)
}

// GetReplicationMessages mocks base method.
func (m *MockAdminServiceClient) GetReplicationMessages(ctx context.Context, in *adminservice.GetReplicationMessagesRequest, opts ...grpc.CallOption) (*adminservice.GetReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetReplicationMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.GetReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReplicationMessages indicates an expected call of GetReplicationMessages.
func (mr *MockAdminServiceClientMockRecorder) GetReplicationMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReplicationMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).GetReplicationMessages), varargs...)
}

// GetWorkflowExecutionRawHistoryV2 mocks base method.
func (m *MockAdminServiceClient) GetWorkflowExecutionRawHistoryV2(ctx context.Context, in *adminservice.GetWorkflowExecutionRawHistoryV2Request, opts ...grpc.CallOption) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetWorkflowExecutionRawHistoryV2", varargs...)
	ret0, _ := ret[0].(*adminservice.GetWorkflowExecutionRawHistoryV2Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkflowExecutionRawHistoryV2 indicates an expected call of GetWorkflowExecutionRawHistoryV2.
func (mr *MockAdminServiceClientMockRecorder) GetWorkflowExecutionRawHistoryV2(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowExecutionRawHistoryV2", reflect.TypeOf((*MockAdminServiceClient)(nil).GetWorkflowExecutionRawHistoryV2), varargs...)
}

// MergeDLQMessages mocks base method.
func (m *MockAdminServiceClient) MergeDLQMessages(ctx context.Context, in *adminservice.MergeDLQMessagesRequest, opts ...grpc.CallOption) (*adminservice.MergeDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "MergeDLQMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.MergeDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MergeDLQMessages indicates an expected call of MergeDLQMessages.
func (mr *MockAdminServiceClientMockRecorder) MergeDLQMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MergeDLQMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).MergeDLQMessages), varargs...)
}

// PurgeDLQMessages mocks base method.
func (m *MockAdminServiceClient) PurgeDLQMessages(ctx context.Context, in *adminservice.PurgeDLQMessagesRequest, opts ...grpc.CallOption) (*adminservice.PurgeDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PurgeDLQMessages", varargs...)
	ret0, _ := ret[0].(*adminservice.PurgeDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeDLQMessages indicates an expected call of PurgeDLQMessages.
func (mr *MockAdminServiceClientMockRecorder) PurgeDLQMessages(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeDLQMessages", reflect.TypeOf((*MockAdminServiceClient)(nil).PurgeDLQMessages), varargs...)
}

// ReapplyEvents mocks base method.
func (m *MockAdminServiceClient) ReapplyEvents(ctx context.Context, in *adminservice.ReapplyEventsRequest, opts ...grpc.CallOption) (*adminservice.ReapplyEventsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ReapplyEvents", varargs...)
	ret0, _ := ret[0].(*adminservice.ReapplyEventsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReapplyEvents indicates an expected call of ReapplyEvents.
func (mr *MockAdminServiceClientMockRecorder) ReapplyEvents(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReapplyEvents", reflect.TypeOf((*MockAdminServiceClient)(nil).ReapplyEvents), varargs...)
}

// RefreshWorkflowTasks mocks base method.
func (m *MockAdminServiceClient) RefreshWorkflowTasks(ctx context.Context, in *adminservice.RefreshWorkflowTasksRequest, opts ...grpc.CallOption) (*adminservice.RefreshWorkflowTasksResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RefreshWorkflowTasks", varargs...)
	ret0, _ := ret[0].(*adminservice.RefreshWorkflowTasksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RefreshWorkflowTasks indicates an expected call of RefreshWorkflowTasks.
func (mr *MockAdminServiceClientMockRecorder) RefreshWorkflowTasks(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RefreshWorkflowTasks", reflect.TypeOf((*MockAdminServiceClient)(nil).RefreshWorkflowTasks), varargs...)
}

// RemoveTask mocks base method.
func (m *MockAdminServiceClient) RemoveTask(ctx context.Context, in *adminservice.RemoveTaskRequest, opts ...grpc.CallOption) (*adminservice.RemoveTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RemoveTask", varargs...)
	ret0, _ := ret[0].(*adminservice.RemoveTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemoveTask indicates an expected call of RemoveTask.
func (mr *MockAdminServiceClientMockRecorder) RemoveTask(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveTask", reflect.TypeOf((*MockAdminServiceClient)(nil).RemoveTask), varargs...)
}

// ResendReplicationTasks mocks base method.
func (m *MockAdminServiceClient) ResendReplicationTasks(ctx context.Context, in *adminservice.ResendReplicationTasksRequest, opts ...grpc.CallOption) (*adminservice.ResendReplicationTasksResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ResendReplicationTasks", varargs...)
	ret0, _ := ret[0].(*adminservice.ResendReplicationTasksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResendReplicationTasks indicates an expected call of ResendReplicationTasks.
func (mr *MockAdminServiceClientMockRecorder) ResendReplicationTasks(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResendReplicationTasks", reflect.TypeOf((*MockAdminServiceClient)(nil).ResendReplicationTasks), varargs...)
}

// MockAdminServiceServer is a mock of AdminServiceServer interface.
type MockAdminServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockAdminServiceServerMockRecorder
}

// MockAdminServiceServerMockRecorder is the mock recorder for MockAdminServiceServer.
type MockAdminServiceServerMockRecorder struct {
	mock *MockAdminServiceServer
}

// NewMockAdminServiceServer creates a new mock instance.
func NewMockAdminServiceServer(ctrl *gomock.Controller) *MockAdminServiceServer {
	mock := &MockAdminServiceServer{ctrl: ctrl}
	mock.recorder = &MockAdminServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAdminServiceServer) EXPECT() *MockAdminServiceServerMockRecorder {
	return m.recorder
}

// AddSearchAttribute mocks base method.
func (m *MockAdminServiceServer) AddSearchAttribute(arg0 context.Context, arg1 *adminservice.AddSearchAttributeRequest) (*adminservice.AddSearchAttributeResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddSearchAttribute", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.AddSearchAttributeResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddSearchAttribute indicates an expected call of AddSearchAttribute.
func (mr *MockAdminServiceServerMockRecorder) AddSearchAttribute(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddSearchAttribute", reflect.TypeOf((*MockAdminServiceServer)(nil).AddSearchAttribute), arg0, arg1)
}

// CloseShard mocks base method.
func (m *MockAdminServiceServer) CloseShard(arg0 context.Context, arg1 *adminservice.CloseShardRequest) (*adminservice.CloseShardResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseShard", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.CloseShardResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CloseShard indicates an expected call of CloseShard.
func (mr *MockAdminServiceServerMockRecorder) CloseShard(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseShard", reflect.TypeOf((*MockAdminServiceServer)(nil).CloseShard), arg0, arg1)
}

// DescribeCluster mocks base method.
func (m *MockAdminServiceServer) DescribeCluster(arg0 context.Context, arg1 *adminservice.DescribeClusterRequest) (*adminservice.DescribeClusterResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeCluster", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.DescribeClusterResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeCluster indicates an expected call of DescribeCluster.
func (mr *MockAdminServiceServerMockRecorder) DescribeCluster(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeCluster", reflect.TypeOf((*MockAdminServiceServer)(nil).DescribeCluster), arg0, arg1)
}

// DescribeHistoryHost mocks base method.
func (m *MockAdminServiceServer) DescribeHistoryHost(arg0 context.Context, arg1 *adminservice.DescribeHistoryHostRequest) (*adminservice.DescribeHistoryHostResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeHistoryHost", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.DescribeHistoryHostResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeHistoryHost indicates an expected call of DescribeHistoryHost.
func (mr *MockAdminServiceServerMockRecorder) DescribeHistoryHost(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeHistoryHost", reflect.TypeOf((*MockAdminServiceServer)(nil).DescribeHistoryHost), arg0, arg1)
}

// DescribeMutableState mocks base method.
func (m *MockAdminServiceServer) DescribeMutableState(arg0 context.Context, arg1 *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeMutableState", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.DescribeMutableStateResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeMutableState indicates an expected call of DescribeMutableState.
func (mr *MockAdminServiceServerMockRecorder) DescribeMutableState(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeMutableState", reflect.TypeOf((*MockAdminServiceServer)(nil).DescribeMutableState), arg0, arg1)
}

// GetDLQMessages mocks base method.
func (m *MockAdminServiceServer) GetDLQMessages(arg0 context.Context, arg1 *adminservice.GetDLQMessagesRequest) (*adminservice.GetDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDLQMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.GetDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDLQMessages indicates an expected call of GetDLQMessages.
func (mr *MockAdminServiceServerMockRecorder) GetDLQMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDLQMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).GetDLQMessages), arg0, arg1)
}

// GetDLQReplicationMessages mocks base method.
func (m *MockAdminServiceServer) GetDLQReplicationMessages(arg0 context.Context, arg1 *adminservice.GetDLQReplicationMessagesRequest) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDLQReplicationMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.GetDLQReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDLQReplicationMessages indicates an expected call of GetDLQReplicationMessages.
func (mr *MockAdminServiceServerMockRecorder) GetDLQReplicationMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDLQReplicationMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).GetDLQReplicationMessages), arg0, arg1)
}

// GetNamespaceReplicationMessages mocks base method.
func (m *MockAdminServiceServer) GetNamespaceReplicationMessages(arg0 context.Context, arg1 *adminservice.GetNamespaceReplicationMessagesRequest) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceReplicationMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.GetNamespaceReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceReplicationMessages indicates an expected call of GetNamespaceReplicationMessages.
func (mr *MockAdminServiceServerMockRecorder) GetNamespaceReplicationMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceReplicationMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).GetNamespaceReplicationMessages), arg0, arg1)
}

// GetReplicationMessages mocks base method.
func (m *MockAdminServiceServer) GetReplicationMessages(arg0 context.Context, arg1 *adminservice.GetReplicationMessagesRequest) (*adminservice.GetReplicationMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReplicationMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.GetReplicationMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReplicationMessages indicates an expected call of GetReplicationMessages.
func (mr *MockAdminServiceServerMockRecorder) GetReplicationMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReplicationMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).GetReplicationMessages), arg0, arg1)
}

// GetWorkflowExecutionRawHistoryV2 mocks base method.
func (m *MockAdminServiceServer) GetWorkflowExecutionRawHistoryV2(arg0 context.Context, arg1 *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkflowExecutionRawHistoryV2", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.GetWorkflowExecutionRawHistoryV2Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkflowExecutionRawHistoryV2 indicates an expected call of GetWorkflowExecutionRawHistoryV2.
func (mr *MockAdminServiceServerMockRecorder) GetWorkflowExecutionRawHistoryV2(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkflowExecutionRawHistoryV2", reflect.TypeOf((*MockAdminServiceServer)(nil).GetWorkflowExecutionRawHistoryV2), arg0, arg1)
}

// MergeDLQMessages mocks base method.
func (m *MockAdminServiceServer) MergeDLQMessages(arg0 context.Context, arg1 *adminservice.MergeDLQMessagesRequest) (*adminservice.MergeDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MergeDLQMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.MergeDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MergeDLQMessages indicates an expected call of MergeDLQMessages.
func (mr *MockAdminServiceServerMockRecorder) MergeDLQMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MergeDLQMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).MergeDLQMessages), arg0, arg1)
}

// PurgeDLQMessages mocks base method.
func (m *MockAdminServiceServer) PurgeDLQMessages(arg0 context.Context, arg1 *adminservice.PurgeDLQMessagesRequest) (*adminservice.PurgeDLQMessagesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PurgeDLQMessages", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.PurgeDLQMessagesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeDLQMessages indicates an expected call of PurgeDLQMessages.
func (mr *MockAdminServiceServerMockRecorder) PurgeDLQMessages(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeDLQMessages", reflect.TypeOf((*MockAdminServiceServer)(nil).PurgeDLQMessages), arg0, arg1)
}

// ReapplyEvents mocks base method.
func (m *MockAdminServiceServer) ReapplyEvents(arg0 context.Context, arg1 *adminservice.ReapplyEventsRequest) (*adminservice.ReapplyEventsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReapplyEvents", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.ReapplyEventsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReapplyEvents indicates an expected call of ReapplyEvents.
func (mr *MockAdminServiceServerMockRecorder) ReapplyEvents(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReapplyEvents", reflect.TypeOf((*MockAdminServiceServer)(nil).ReapplyEvents), arg0, arg1)
}

// RefreshWorkflowTasks mocks base method.
func (m *MockAdminServiceServer) RefreshWorkflowTasks(arg0 context.Context, arg1 *adminservice.RefreshWorkflowTasksRequest) (*adminservice.RefreshWorkflowTasksResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RefreshWorkflowTasks", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.RefreshWorkflowTasksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RefreshWorkflowTasks indicates an expected call of RefreshWorkflowTasks.
func (mr *MockAdminServiceServerMockRecorder) RefreshWorkflowTasks(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RefreshWorkflowTasks", reflect.TypeOf((*MockAdminServiceServer)(nil).RefreshWorkflowTasks), arg0, arg1)
}

// RemoveTask mocks base method.
func (m *MockAdminServiceServer) RemoveTask(arg0 context.Context, arg1 *adminservice.RemoveTaskRequest) (*adminservice.RemoveTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveTask", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.RemoveTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemoveTask indicates an expected call of RemoveTask.
func (mr *MockAdminServiceServerMockRecorder) RemoveTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveTask", reflect.TypeOf((*MockAdminServiceServer)(nil).RemoveTask), arg0, arg1)
}

// ResendReplicationTasks mocks base method.
func (m *MockAdminServiceServer) ResendReplicationTasks(arg0 context.Context, arg1 *adminservice.ResendReplicationTasksRequest) (*adminservice.ResendReplicationTasksResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResendReplicationTasks", arg0, arg1)
	ret0, _ := ret[0].(*adminservice.ResendReplicationTasksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResendReplicationTasks indicates an expected call of ResendReplicationTasks.
func (mr *MockAdminServiceServerMockRecorder) ResendReplicationTasks(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResendReplicationTasks", reflect.TypeOf((*MockAdminServiceServer)(nil).ResendReplicationTasks), arg0, arg1)
}
