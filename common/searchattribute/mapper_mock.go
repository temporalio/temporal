// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
// Source: mapper.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package searchattribute -source mapper.go -destination mapper_mock.go
//

// Package searchattribute is a generated GoMock package.
package searchattribute

import (
	reflect "reflect"

	namespace "go.temporal.io/server/common/namespace"
	gomock "go.uber.org/mock/gomock"
)

// MockMapper is a mock of Mapper interface.
type MockMapper struct {
	ctrl     *gomock.Controller
	recorder *MockMapperMockRecorder
}

// MockMapperMockRecorder is the mock recorder for MockMapper.
type MockMapperMockRecorder struct {
	mock *MockMapper
}

// NewMockMapper creates a new mock instance.
func NewMockMapper(ctrl *gomock.Controller) *MockMapper {
	mock := &MockMapper{ctrl: ctrl}
	mock.recorder = &MockMapperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMapper) EXPECT() *MockMapperMockRecorder {
	return m.recorder
}

// GetAlias mocks base method.
func (m *MockMapper) GetAlias(fieldName, namespace string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAlias", fieldName, namespace)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAlias indicates an expected call of GetAlias.
func (mr *MockMapperMockRecorder) GetAlias(fieldName, namespace any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAlias", reflect.TypeOf((*MockMapper)(nil).GetAlias), fieldName, namespace)
}

// GetFieldName mocks base method.
func (m *MockMapper) GetFieldName(alias, namespace string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFieldName", alias, namespace)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFieldName indicates an expected call of GetFieldName.
func (mr *MockMapperMockRecorder) GetFieldName(alias, namespace any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFieldName", reflect.TypeOf((*MockMapper)(nil).GetFieldName), alias, namespace)
}

// MockMapperProvider is a mock of MapperProvider interface.
type MockMapperProvider struct {
	ctrl     *gomock.Controller
	recorder *MockMapperProviderMockRecorder
}

// MockMapperProviderMockRecorder is the mock recorder for MockMapperProvider.
type MockMapperProviderMockRecorder struct {
	mock *MockMapperProvider
}

// NewMockMapperProvider creates a new mock instance.
func NewMockMapperProvider(ctrl *gomock.Controller) *MockMapperProvider {
	mock := &MockMapperProvider{ctrl: ctrl}
	mock.recorder = &MockMapperProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMapperProvider) EXPECT() *MockMapperProviderMockRecorder {
	return m.recorder
}

// GetMapper mocks base method.
func (m *MockMapperProvider) GetMapper(nsName namespace.Name) (Mapper, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMapper", nsName)
	ret0, _ := ret[0].(Mapper)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMapper indicates an expected call of GetMapper.
func (mr *MockMapperProviderMockRecorder) GetMapper(nsName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMapper", reflect.TypeOf((*MockMapperProvider)(nil).GetMapper), nsName)
}
