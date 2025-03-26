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
// Source: rpc.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../LICENSE -package common -source rpc.go -destination rpc_mock.go
//

// Package common is a generated GoMock package.
package common

import (
	net "net"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
)

// MockRPCFactory is a mock of RPCFactory interface.
type MockRPCFactory struct {
	ctrl     *gomock.Controller
	recorder *MockRPCFactoryMockRecorder
	isgomock struct{}
}

// MockRPCFactoryMockRecorder is the mock recorder for MockRPCFactory.
type MockRPCFactoryMockRecorder struct {
	mock *MockRPCFactory
}

// NewMockRPCFactory creates a new mock instance.
func NewMockRPCFactory(ctrl *gomock.Controller) *MockRPCFactory {
	mock := &MockRPCFactory{ctrl: ctrl}
	mock.recorder = &MockRPCFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRPCFactory) EXPECT() *MockRPCFactoryMockRecorder {
	return m.recorder
}

// CreateInternodeGRPCConnection mocks base method.
func (m *MockRPCFactory) CreateInternodeGRPCConnection(rpcAddress string, dialOptions ...grpc.DialOption) *grpc.ClientConn {
	m.ctrl.T.Helper()
	varargs := []any{rpcAddress}
	for _, a := range dialOptions {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateInternodeGRPCConnection", varargs...)
	ret0, _ := ret[0].(*grpc.ClientConn)
	return ret0
}

// CreateInternodeGRPCConnection indicates an expected call of CreateInternodeGRPCConnection.
func (mr *MockRPCFactoryMockRecorder) CreateInternodeGRPCConnection(rpcAddress any, dialOptions ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{rpcAddress}, dialOptions...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateInternodeGRPCConnection", reflect.TypeOf((*MockRPCFactory)(nil).CreateInternodeGRPCConnection), varargs...)
}

// CreateLocalFrontendGRPCConnection mocks base method.
func (m *MockRPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateLocalFrontendGRPCConnection")
	ret0, _ := ret[0].(*grpc.ClientConn)
	return ret0
}

// CreateLocalFrontendGRPCConnection indicates an expected call of CreateLocalFrontendGRPCConnection.
func (mr *MockRPCFactoryMockRecorder) CreateLocalFrontendGRPCConnection() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateLocalFrontendGRPCConnection", reflect.TypeOf((*MockRPCFactory)(nil).CreateLocalFrontendGRPCConnection))
}

// CreateLocalFrontendHTTPClient mocks base method.
func (m *MockRPCFactory) CreateLocalFrontendHTTPClient() (*FrontendHTTPClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateLocalFrontendHTTPClient")
	ret0, _ := ret[0].(*FrontendHTTPClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateLocalFrontendHTTPClient indicates an expected call of CreateLocalFrontendHTTPClient.
func (mr *MockRPCFactoryMockRecorder) CreateLocalFrontendHTTPClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateLocalFrontendHTTPClient", reflect.TypeOf((*MockRPCFactory)(nil).CreateLocalFrontendHTTPClient))
}

// CreateRemoteFrontendGRPCConnection mocks base method.
func (m *MockRPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string, dialOptions ...grpc.DialOption) *grpc.ClientConn {
	m.ctrl.T.Helper()
	varargs := []any{rpcAddress}
	for _, a := range dialOptions {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateRemoteFrontendGRPCConnection", varargs...)
	ret0, _ := ret[0].(*grpc.ClientConn)
	return ret0
}

// CreateRemoteFrontendGRPCConnection indicates an expected call of CreateRemoteFrontendGRPCConnection.
func (mr *MockRPCFactoryMockRecorder) CreateRemoteFrontendGRPCConnection(rpcAddress any, dialOptions ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{rpcAddress}, dialOptions...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateRemoteFrontendGRPCConnection", reflect.TypeOf((*MockRPCFactory)(nil).CreateRemoteFrontendGRPCConnection), varargs...)
}

// GetFrontendGRPCServerOptions mocks base method.
func (m *MockRPCFactory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFrontendGRPCServerOptions")
	ret0, _ := ret[0].([]grpc.ServerOption)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFrontendGRPCServerOptions indicates an expected call of GetFrontendGRPCServerOptions.
func (mr *MockRPCFactoryMockRecorder) GetFrontendGRPCServerOptions() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFrontendGRPCServerOptions", reflect.TypeOf((*MockRPCFactory)(nil).GetFrontendGRPCServerOptions))
}

// GetGRPCListener mocks base method.
func (m *MockRPCFactory) GetGRPCListener() net.Listener {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetGRPCListener")
	ret0, _ := ret[0].(net.Listener)
	return ret0
}

// GetGRPCListener indicates an expected call of GetGRPCListener.
func (mr *MockRPCFactoryMockRecorder) GetGRPCListener() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetGRPCListener", reflect.TypeOf((*MockRPCFactory)(nil).GetGRPCListener))
}

// GetInternodeGRPCServerOptions mocks base method.
func (m *MockRPCFactory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetInternodeGRPCServerOptions")
	ret0, _ := ret[0].([]grpc.ServerOption)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetInternodeGRPCServerOptions indicates an expected call of GetInternodeGRPCServerOptions.
func (mr *MockRPCFactoryMockRecorder) GetInternodeGRPCServerOptions() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetInternodeGRPCServerOptions", reflect.TypeOf((*MockRPCFactory)(nil).GetInternodeGRPCServerOptions))
}
