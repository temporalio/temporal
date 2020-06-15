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
// Source: handler.go

// Package namespace is a generated GoMock package.
package namespace

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	workflowservice "go.temporal.io/temporal-proto/workflowservice/v1"
	reflect "reflect"
)

// MockHandler is a mock of Handler interface
type MockHandler struct {
	ctrl     *gomock.Controller
	recorder *MockHandlerMockRecorder
}

// MockHandlerMockRecorder is the mock recorder for MockHandler
type MockHandlerMockRecorder struct {
	mock *MockHandler
}

// NewMockHandler creates a new mock instance
func NewMockHandler(ctrl *gomock.Controller) *MockHandler {
	mock := &MockHandler{ctrl: ctrl}
	mock.recorder = &MockHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockHandler) EXPECT() *MockHandlerMockRecorder {
	return m.recorder
}

// DeprecateNamespace mocks base method
func (m *MockHandler) DeprecateNamespace(ctx context.Context, deprecateRequest *workflowservice.DeprecateNamespaceRequest) (*workflowservice.DeprecateNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeprecateNamespace", ctx, deprecateRequest)
	ret0, _ := ret[0].(*workflowservice.DeprecateNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeprecateNamespace indicates an expected call of DeprecateNamespace
func (mr *MockHandlerMockRecorder) DeprecateNamespace(ctx, deprecateRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeprecateNamespace", reflect.TypeOf((*MockHandler)(nil).DeprecateNamespace), ctx, deprecateRequest)
}

// DescribeNamespace mocks base method
func (m *MockHandler) DescribeNamespace(ctx context.Context, describeRequest *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DescribeNamespace", ctx, describeRequest)
	ret0, _ := ret[0].(*workflowservice.DescribeNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DescribeNamespace indicates an expected call of DescribeNamespace
func (mr *MockHandlerMockRecorder) DescribeNamespace(ctx, describeRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DescribeNamespace", reflect.TypeOf((*MockHandler)(nil).DescribeNamespace), ctx, describeRequest)
}

// ListNamespaces mocks base method
func (m *MockHandler) ListNamespaces(ctx context.Context, listRequest *workflowservice.ListNamespacesRequest) (*workflowservice.ListNamespacesResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListNamespaces", ctx, listRequest)
	ret0, _ := ret[0].(*workflowservice.ListNamespacesResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListNamespaces indicates an expected call of ListNamespaces
func (mr *MockHandlerMockRecorder) ListNamespaces(ctx, listRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListNamespaces", reflect.TypeOf((*MockHandler)(nil).ListNamespaces), ctx, listRequest)
}

// RegisterNamespace mocks base method
func (m *MockHandler) RegisterNamespace(ctx context.Context, registerRequest *workflowservice.RegisterNamespaceRequest) (*workflowservice.RegisterNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterNamespace", ctx, registerRequest)
	ret0, _ := ret[0].(*workflowservice.RegisterNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterNamespace indicates an expected call of RegisterNamespace
func (mr *MockHandlerMockRecorder) RegisterNamespace(ctx, registerRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterNamespace", reflect.TypeOf((*MockHandler)(nil).RegisterNamespace), ctx, registerRequest)
}

// UpdateNamespace mocks base method
func (m *MockHandler) UpdateNamespace(ctx context.Context, updateRequest *workflowservice.UpdateNamespaceRequest) (*workflowservice.UpdateNamespaceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateNamespace", ctx, updateRequest)
	ret0, _ := ret[0].(*workflowservice.UpdateNamespaceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateNamespace indicates an expected call of UpdateNamespace
func (mr *MockHandlerMockRecorder) UpdateNamespace(ctx, updateRequest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateNamespace", reflect.TypeOf((*MockHandler)(nil).UpdateNamespace), ctx, updateRequest)
}
