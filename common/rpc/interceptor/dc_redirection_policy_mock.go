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
// Source: dc_redirection_policy.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../LICENSE -package interceptor -source dc_redirection_policy.go -destination dc_redirection_policy_mock.go
//

// Package interceptor is a generated GoMock package.
package interceptor

import (
	context "context"
	reflect "reflect"

	namespace "go.temporal.io/server/common/namespace"
	gomock "go.uber.org/mock/gomock"
)

// MockDCRedirectionPolicy is a mock of DCRedirectionPolicy interface.
type MockDCRedirectionPolicy struct {
	ctrl     *gomock.Controller
	recorder *MockDCRedirectionPolicyMockRecorder
}

// MockDCRedirectionPolicyMockRecorder is the mock recorder for MockDCRedirectionPolicy.
type MockDCRedirectionPolicyMockRecorder struct {
	mock *MockDCRedirectionPolicy
}

// NewMockDCRedirectionPolicy creates a new mock instance.
func NewMockDCRedirectionPolicy(ctrl *gomock.Controller) *MockDCRedirectionPolicy {
	mock := &MockDCRedirectionPolicy{ctrl: ctrl}
	mock.recorder = &MockDCRedirectionPolicyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDCRedirectionPolicy) EXPECT() *MockDCRedirectionPolicyMockRecorder {
	return m.recorder
}

// WithNamespaceIDRedirect mocks base method.
func (m *MockDCRedirectionPolicy) WithNamespaceIDRedirect(ctx context.Context, namespaceID namespace.ID, apiName string, call func(string) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithNamespaceIDRedirect", ctx, namespaceID, apiName, call)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithNamespaceIDRedirect indicates an expected call of WithNamespaceIDRedirect.
func (mr *MockDCRedirectionPolicyMockRecorder) WithNamespaceIDRedirect(ctx, namespaceID, apiName, call any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithNamespaceIDRedirect", reflect.TypeOf((*MockDCRedirectionPolicy)(nil).WithNamespaceIDRedirect), ctx, namespaceID, apiName, call)
}

// WithNamespaceRedirect mocks base method.
func (m *MockDCRedirectionPolicy) WithNamespaceRedirect(ctx context.Context, namespace namespace.Name, apiName string, call func(string) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithNamespaceRedirect", ctx, namespace, apiName, call)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithNamespaceRedirect indicates an expected call of WithNamespaceRedirect.
func (mr *MockDCRedirectionPolicyMockRecorder) WithNamespaceRedirect(ctx, namespace, apiName, call any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithNamespaceRedirect", reflect.TypeOf((*MockDCRedirectionPolicy)(nil).WithNamespaceRedirect), ctx, namespace, apiName, call)
}
