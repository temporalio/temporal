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
// Source: clientBean.go

// Package client is a generated GoMock package.
package client

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	history "go.temporal.io/server/client/history"
	matching "go.temporal.io/server/client/matching"
)

// MockBean is a mock of Bean interface.
type MockBean struct {
	ctrl     *gomock.Controller
	recorder *MockBeanMockRecorder
}

// MockBeanMockRecorder is the mock recorder for MockBean.
type MockBeanMockRecorder struct {
	mock *MockBean
}

// NewMockBean creates a new mock instance.
func NewMockBean(ctrl *gomock.Controller) *MockBean {
	mock := &MockBean{ctrl: ctrl}
	mock.recorder = &MockBeanMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBean) EXPECT() *MockBeanMockRecorder {
	return m.recorder
}

// GetFrontendClient mocks base method.
func (m *MockBean) GetFrontendClient() workflowservice.WorkflowServiceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFrontendClient")
	ret0, _ := ret[0].(workflowservice.WorkflowServiceClient)
	return ret0
}

// GetFrontendClient indicates an expected call of GetFrontendClient.
func (mr *MockBeanMockRecorder) GetFrontendClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFrontendClient", reflect.TypeOf((*MockBean)(nil).GetFrontendClient))
}

// GetHistoryClient mocks base method.
func (m *MockBean) GetHistoryClient() history.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHistoryClient")
	ret0, _ := ret[0].(history.Client)
	return ret0
}

// GetHistoryClient indicates an expected call of GetHistoryClient.
func (mr *MockBeanMockRecorder) GetHistoryClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHistoryClient", reflect.TypeOf((*MockBean)(nil).GetHistoryClient))
}

// GetMatchingClient mocks base method.
func (m *MockBean) GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matching.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMatchingClient", namespaceIDToName)
	ret0, _ := ret[0].(matching.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMatchingClient indicates an expected call of GetMatchingClient.
func (mr *MockBeanMockRecorder) GetMatchingClient(namespaceIDToName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMatchingClient", reflect.TypeOf((*MockBean)(nil).GetMatchingClient), namespaceIDToName)
}

// GetRemoteAdminClient mocks base method.
func (m *MockBean) GetRemoteAdminClient(cluster string) adminservice.AdminServiceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRemoteAdminClient", cluster)
	ret0, _ := ret[0].(adminservice.AdminServiceClient)
	return ret0
}

// GetRemoteAdminClient indicates an expected call of GetRemoteAdminClient.
func (mr *MockBeanMockRecorder) GetRemoteAdminClient(cluster interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRemoteAdminClient", reflect.TypeOf((*MockBean)(nil).GetRemoteAdminClient), cluster)
}

// GetRemoteFrontendClient mocks base method.
func (m *MockBean) GetRemoteFrontendClient(cluster string) workflowservice.WorkflowServiceClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRemoteFrontendClient", cluster)
	ret0, _ := ret[0].(workflowservice.WorkflowServiceClient)
	return ret0
}

// GetRemoteFrontendClient indicates an expected call of GetRemoteFrontendClient.
func (mr *MockBeanMockRecorder) GetRemoteFrontendClient(cluster interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRemoteFrontendClient", reflect.TypeOf((*MockBean)(nil).GetRemoteFrontendClient), cluster)
}

// SetFrontendClient mocks base method.
func (m *MockBean) SetFrontendClient(client workflowservice.WorkflowServiceClient) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetFrontendClient", client)
}

// SetFrontendClient indicates an expected call of SetFrontendClient.
func (mr *MockBeanMockRecorder) SetFrontendClient(client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetFrontendClient", reflect.TypeOf((*MockBean)(nil).SetFrontendClient), client)
}

// SetHistoryClient mocks base method.
func (m *MockBean) SetHistoryClient(client history.Client) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetHistoryClient", client)
}

// SetHistoryClient indicates an expected call of SetHistoryClient.
func (mr *MockBeanMockRecorder) SetHistoryClient(client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHistoryClient", reflect.TypeOf((*MockBean)(nil).SetHistoryClient), client)
}

// SetMatchingClient mocks base method.
func (m *MockBean) SetMatchingClient(client matching.Client) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetMatchingClient", client)
}

// SetMatchingClient indicates an expected call of SetMatchingClient.
func (mr *MockBeanMockRecorder) SetMatchingClient(client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMatchingClient", reflect.TypeOf((*MockBean)(nil).SetMatchingClient), client)
}

// SetRemoteAdminClient mocks base method.
func (m *MockBean) SetRemoteAdminClient(cluster string, client adminservice.AdminServiceClient) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetRemoteAdminClient", cluster, client)
}

// SetRemoteAdminClient indicates an expected call of SetRemoteAdminClient.
func (mr *MockBeanMockRecorder) SetRemoteAdminClient(cluster, client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRemoteAdminClient", reflect.TypeOf((*MockBean)(nil).SetRemoteAdminClient), cluster, client)
}

// SetRemoteFrontendClient mocks base method.
func (m *MockBean) SetRemoteFrontendClient(cluster string, client workflowservice.WorkflowServiceClient) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetRemoteFrontendClient", cluster, client)
}

// SetRemoteFrontendClient indicates an expected call of SetRemoteFrontendClient.
func (mr *MockBeanMockRecorder) SetRemoteFrontendClient(cluster, client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetRemoteFrontendClient", reflect.TypeOf((*MockBean)(nil).SetRemoteFrontendClient), cluster, client)
}
