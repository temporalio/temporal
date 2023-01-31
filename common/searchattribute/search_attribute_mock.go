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
// Source: search_attirbute.go

// Package searchattribute is a generated GoMock package.
package searchattribute

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "go.temporal.io/api/common/v1"
	v10 "go.temporal.io/api/enums/v1"
)

// MockProvider is a mock of Provider interface.
type MockProvider struct {
	ctrl     *gomock.Controller
	recorder *MockProviderMockRecorder
}

// MockProviderMockRecorder is the mock recorder for MockProvider.
type MockProviderMockRecorder struct {
	mock *MockProvider
}

// NewMockProvider creates a new mock instance.
func NewMockProvider(ctrl *gomock.Controller) *MockProvider {
	mock := &MockProvider{ctrl: ctrl}
	mock.recorder = &MockProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProvider) EXPECT() *MockProviderMockRecorder {
	return m.recorder
}

// AliasFields mocks base method.
func (m *MockProvider) AliasFields(mapper Mapper, searchAttributes *v1.SearchAttributes, namespace string) (*v1.SearchAttributes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AliasFields", mapper, searchAttributes, namespace)
	ret0, _ := ret[0].(*v1.SearchAttributes)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AliasFields indicates an expected call of AliasFields.
func (mr *MockProviderMockRecorder) AliasFields(mapper, searchAttributes, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AliasFields", reflect.TypeOf((*MockProvider)(nil).AliasFields), mapper, searchAttributes, namespace)
}

// GetMapper mocks base method.
func (m *MockProvider) GetMapper(mapper Mapper, namespace string) (Mapper, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMapper", mapper, namespace)
	ret0, _ := ret[0].(Mapper)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMapper indicates an expected call of GetMapper.
func (mr *MockProviderMockRecorder) GetMapper(mapper, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMapper", reflect.TypeOf((*MockProvider)(nil).GetMapper), mapper, namespace)
}

// GetSearchAttributes mocks base method.
func (m *MockProvider) GetSearchAttributes(indexName string, forceRefreshCache bool) (NameTypeMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", indexName, forceRefreshCache)
	ret0, _ := ret[0].(NameTypeMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes.
func (mr *MockProviderMockRecorder) GetSearchAttributes(indexName, forceRefreshCache interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockProvider)(nil).GetSearchAttributes), indexName, forceRefreshCache)
}

// UnaliasFields mocks base method.
func (m *MockProvider) UnaliasFields(mapper Mapper, searchAttributes *v1.SearchAttributes, namespace string) (*v1.SearchAttributes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnaliasFields", mapper, searchAttributes, namespace)
	ret0, _ := ret[0].(*v1.SearchAttributes)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnaliasFields indicates an expected call of UnaliasFields.
func (mr *MockProviderMockRecorder) UnaliasFields(mapper, searchAttributes, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnaliasFields", reflect.TypeOf((*MockProvider)(nil).UnaliasFields), mapper, searchAttributes, namespace)
}

// MockManager is a mock of Manager interface.
type MockManager struct {
	ctrl     *gomock.Controller
	recorder *MockManagerMockRecorder
}

// MockManagerMockRecorder is the mock recorder for MockManager.
type MockManagerMockRecorder struct {
	mock *MockManager
}

// NewMockManager creates a new mock instance.
func NewMockManager(ctrl *gomock.Controller) *MockManager {
	mock := &MockManager{ctrl: ctrl}
	mock.recorder = &MockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockManager) EXPECT() *MockManagerMockRecorder {
	return m.recorder
}

// AliasFields mocks base method.
func (m *MockManager) AliasFields(mapper Mapper, searchAttributes *v1.SearchAttributes, namespace string) (*v1.SearchAttributes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AliasFields", mapper, searchAttributes, namespace)
	ret0, _ := ret[0].(*v1.SearchAttributes)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AliasFields indicates an expected call of AliasFields.
func (mr *MockManagerMockRecorder) AliasFields(mapper, searchAttributes, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AliasFields", reflect.TypeOf((*MockManager)(nil).AliasFields), mapper, searchAttributes, namespace)
}

// GetMapper mocks base method.
func (m *MockManager) GetMapper(mapper Mapper, namespace string) (Mapper, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMapper", mapper, namespace)
	ret0, _ := ret[0].(Mapper)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMapper indicates an expected call of GetMapper.
func (mr *MockManagerMockRecorder) GetMapper(mapper, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMapper", reflect.TypeOf((*MockManager)(nil).GetMapper), mapper, namespace)
}

// GetSearchAttributes mocks base method.
func (m *MockManager) GetSearchAttributes(indexName string, forceRefreshCache bool) (NameTypeMap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSearchAttributes", indexName, forceRefreshCache)
	ret0, _ := ret[0].(NameTypeMap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSearchAttributes indicates an expected call of GetSearchAttributes.
func (mr *MockManagerMockRecorder) GetSearchAttributes(indexName, forceRefreshCache interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSearchAttributes", reflect.TypeOf((*MockManager)(nil).GetSearchAttributes), indexName, forceRefreshCache)
}

// SaveSearchAttributes mocks base method.
func (m *MockManager) SaveSearchAttributes(ctx context.Context, indexName string, newCustomSearchAttributes map[string]v10.IndexedValueType) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveSearchAttributes", ctx, indexName, newCustomSearchAttributes)
	ret0, _ := ret[0].(error)
	return ret0
}

// SaveSearchAttributes indicates an expected call of SaveSearchAttributes.
func (mr *MockManagerMockRecorder) SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveSearchAttributes", reflect.TypeOf((*MockManager)(nil).SaveSearchAttributes), ctx, indexName, newCustomSearchAttributes)
}

// UnaliasFields mocks base method.
func (m *MockManager) UnaliasFields(mapper Mapper, searchAttributes *v1.SearchAttributes, namespace string) (*v1.SearchAttributes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnaliasFields", mapper, searchAttributes, namespace)
	ret0, _ := ret[0].(*v1.SearchAttributes)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnaliasFields indicates an expected call of UnaliasFields.
func (mr *MockManagerMockRecorder) UnaliasFields(mapper, searchAttributes, namespace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnaliasFields", reflect.TypeOf((*MockManager)(nil).UnaliasFields), mapper, searchAttributes, namespace)
}
