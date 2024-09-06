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
// Source: claim_mapper.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package authorization -source claim_mapper.go -destination claim_mapper_mock.go
//

// Package authorization is a generated GoMock package.
package authorization

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockClaimMapper is a mock of ClaimMapper interface.
type MockClaimMapper struct {
	ctrl     *gomock.Controller
	recorder *MockClaimMapperMockRecorder
}

// MockClaimMapperMockRecorder is the mock recorder for MockClaimMapper.
type MockClaimMapperMockRecorder struct {
	mock *MockClaimMapper
}

// NewMockClaimMapper creates a new mock instance.
func NewMockClaimMapper(ctrl *gomock.Controller) *MockClaimMapper {
	mock := &MockClaimMapper{ctrl: ctrl}
	mock.recorder = &MockClaimMapperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClaimMapper) EXPECT() *MockClaimMapperMockRecorder {
	return m.recorder
}

// GetClaims mocks base method.
func (m *MockClaimMapper) GetClaims(authInfo *AuthInfo) (*Claims, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClaims", authInfo)
	ret0, _ := ret[0].(*Claims)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClaims indicates an expected call of GetClaims.
func (mr *MockClaimMapperMockRecorder) GetClaims(authInfo any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClaims", reflect.TypeOf((*MockClaimMapper)(nil).GetClaims), authInfo)
}

// MockClaimMapperWithAuthInfoRequired is a mock of ClaimMapperWithAuthInfoRequired interface.
type MockClaimMapperWithAuthInfoRequired struct {
	ctrl     *gomock.Controller
	recorder *MockClaimMapperWithAuthInfoRequiredMockRecorder
}

// MockClaimMapperWithAuthInfoRequiredMockRecorder is the mock recorder for MockClaimMapperWithAuthInfoRequired.
type MockClaimMapperWithAuthInfoRequiredMockRecorder struct {
	mock *MockClaimMapperWithAuthInfoRequired
}

// NewMockClaimMapperWithAuthInfoRequired creates a new mock instance.
func NewMockClaimMapperWithAuthInfoRequired(ctrl *gomock.Controller) *MockClaimMapperWithAuthInfoRequired {
	mock := &MockClaimMapperWithAuthInfoRequired{ctrl: ctrl}
	mock.recorder = &MockClaimMapperWithAuthInfoRequiredMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClaimMapperWithAuthInfoRequired) EXPECT() *MockClaimMapperWithAuthInfoRequiredMockRecorder {
	return m.recorder
}

// AuthInfoRequired mocks base method.
func (m *MockClaimMapperWithAuthInfoRequired) AuthInfoRequired() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AuthInfoRequired")
	ret0, _ := ret[0].(bool)
	return ret0
}

// AuthInfoRequired indicates an expected call of AuthInfoRequired.
func (mr *MockClaimMapperWithAuthInfoRequiredMockRecorder) AuthInfoRequired() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AuthInfoRequired", reflect.TypeOf((*MockClaimMapperWithAuthInfoRequired)(nil).AuthInfoRequired))
}
