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
// Source: connections.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package history -source connections.go -destination connections_mock.go
//

// Package history is a generated GoMock package.
package history

import (
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

// CreateHistoryGRPCConnection mocks base method.
func (m *MockRPCFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateHistoryGRPCConnection", rpcAddress)
	ret0, _ := ret[0].(*grpc.ClientConn)
	return ret0
}

// CreateHistoryGRPCConnection indicates an expected call of CreateHistoryGRPCConnection.
func (mr *MockRPCFactoryMockRecorder) CreateHistoryGRPCConnection(rpcAddress any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateHistoryGRPCConnection", reflect.TypeOf((*MockRPCFactory)(nil).CreateHistoryGRPCConnection), rpcAddress)
}

// MockconnectionPool is a mock of connectionPool interface.
type MockconnectionPool struct {
	ctrl     *gomock.Controller
	recorder *MockconnectionPoolMockRecorder
	isgomock struct{}
}

// MockconnectionPoolMockRecorder is the mock recorder for MockconnectionPool.
type MockconnectionPoolMockRecorder struct {
	mock *MockconnectionPool
}

// NewMockconnectionPool creates a new mock instance.
func NewMockconnectionPool(ctrl *gomock.Controller) *MockconnectionPool {
	mock := &MockconnectionPool{ctrl: ctrl}
	mock.recorder = &MockconnectionPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockconnectionPool) EXPECT() *MockconnectionPoolMockRecorder {
	return m.recorder
}

// getAllClientConns mocks base method.
func (m *MockconnectionPool) getAllClientConns() []clientConnection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getAllClientConns")
	ret0, _ := ret[0].([]clientConnection)
	return ret0
}

// getAllClientConns indicates an expected call of getAllClientConns.
func (mr *MockconnectionPoolMockRecorder) getAllClientConns() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getAllClientConns", reflect.TypeOf((*MockconnectionPool)(nil).getAllClientConns))
}

// getOrCreateClientConn mocks base method.
func (m *MockconnectionPool) getOrCreateClientConn(addr rpcAddress) clientConnection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "getOrCreateClientConn", addr)
	ret0, _ := ret[0].(clientConnection)
	return ret0
}

// getOrCreateClientConn indicates an expected call of getOrCreateClientConn.
func (mr *MockconnectionPoolMockRecorder) getOrCreateClientConn(addr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "getOrCreateClientConn", reflect.TypeOf((*MockconnectionPool)(nil).getOrCreateClientConn), addr)
}

// resetConnectBackoff mocks base method.
func (m *MockconnectionPool) resetConnectBackoff(arg0 clientConnection) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "resetConnectBackoff", arg0)
}

// resetConnectBackoff indicates an expected call of resetConnectBackoff.
func (mr *MockconnectionPoolMockRecorder) resetConnectBackoff(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "resetConnectBackoff", reflect.TypeOf((*MockconnectionPool)(nil).resetConnectBackoff), arg0)
}
