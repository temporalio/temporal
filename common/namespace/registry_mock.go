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
// Source: registry.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package namespace -source registry.go -destination registry_mock.go
//

// Package namespace is a generated GoMock package.
package namespace

import (
	reflect "reflect"

	pingable "go.temporal.io/server/common/pingable"
	gomock "go.uber.org/mock/gomock"
)

// MockRegistry is a mock of Registry interface.
type MockRegistry struct {
	ctrl     *gomock.Controller
	recorder *MockRegistryMockRecorder
}

// MockRegistryMockRecorder is the mock recorder for MockRegistry.
type MockRegistryMockRecorder struct {
	mock *MockRegistry
}

// NewMockRegistry creates a new mock instance.
func NewMockRegistry(ctrl *gomock.Controller) *MockRegistry {
	mock := &MockRegistry{ctrl: ctrl}
	mock.recorder = &MockRegistryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRegistry) EXPECT() *MockRegistryMockRecorder {
	return m.recorder
}

// GetCustomSearchAttributesMapper mocks base method.
func (m *MockRegistry) GetCustomSearchAttributesMapper(name Name) (CustomSearchAttributesMapper, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCustomSearchAttributesMapper", name)
	ret0, _ := ret[0].(CustomSearchAttributesMapper)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCustomSearchAttributesMapper indicates an expected call of GetCustomSearchAttributesMapper.
func (mr *MockRegistryMockRecorder) GetCustomSearchAttributesMapper(name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCustomSearchAttributesMapper", reflect.TypeOf((*MockRegistry)(nil).GetCustomSearchAttributesMapper), name)
}

// GetNamespace mocks base method.
func (m *MockRegistry) GetNamespace(name Name) (*Namespace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespace", name)
	ret0, _ := ret[0].(*Namespace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespace indicates an expected call of GetNamespace.
func (mr *MockRegistryMockRecorder) GetNamespace(name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespace", reflect.TypeOf((*MockRegistry)(nil).GetNamespace), name)
}

// GetNamespaceByID mocks base method.
func (m *MockRegistry) GetNamespaceByID(id ID) (*Namespace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceByID", id)
	ret0, _ := ret[0].(*Namespace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceByID indicates an expected call of GetNamespaceByID.
func (mr *MockRegistryMockRecorder) GetNamespaceByID(id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceByID", reflect.TypeOf((*MockRegistry)(nil).GetNamespaceByID), id)
}

// GetNamespaceByIDWithOptions mocks base method.
func (m *MockRegistry) GetNamespaceByIDWithOptions(id ID, opts GetNamespaceOptions) (*Namespace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceByIDWithOptions", id, opts)
	ret0, _ := ret[0].(*Namespace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceByIDWithOptions indicates an expected call of GetNamespaceByIDWithOptions.
func (mr *MockRegistryMockRecorder) GetNamespaceByIDWithOptions(id, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceByIDWithOptions", reflect.TypeOf((*MockRegistry)(nil).GetNamespaceByIDWithOptions), id, opts)
}

// GetNamespaceID mocks base method.
func (m *MockRegistry) GetNamespaceID(name Name) (ID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceID", name)
	ret0, _ := ret[0].(ID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceID indicates an expected call of GetNamespaceID.
func (mr *MockRegistryMockRecorder) GetNamespaceID(name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceID", reflect.TypeOf((*MockRegistry)(nil).GetNamespaceID), name)
}

// GetNamespaceName mocks base method.
func (m *MockRegistry) GetNamespaceName(id ID) (Name, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceName", id)
	ret0, _ := ret[0].(Name)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceName indicates an expected call of GetNamespaceName.
func (mr *MockRegistryMockRecorder) GetNamespaceName(id any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceName", reflect.TypeOf((*MockRegistry)(nil).GetNamespaceName), id)
}

// GetNamespaceWithOptions mocks base method.
func (m *MockRegistry) GetNamespaceWithOptions(name Name, opts GetNamespaceOptions) (*Namespace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNamespaceWithOptions", name, opts)
	ret0, _ := ret[0].(*Namespace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNamespaceWithOptions indicates an expected call of GetNamespaceWithOptions.
func (mr *MockRegistryMockRecorder) GetNamespaceWithOptions(name, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNamespaceWithOptions", reflect.TypeOf((*MockRegistry)(nil).GetNamespaceWithOptions), name, opts)
}

// GetPingChecks mocks base method.
func (m *MockRegistry) GetPingChecks() []pingable.Check {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPingChecks")
	ret0, _ := ret[0].([]pingable.Check)
	return ret0
}

// GetPingChecks indicates an expected call of GetPingChecks.
func (mr *MockRegistryMockRecorder) GetPingChecks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPingChecks", reflect.TypeOf((*MockRegistry)(nil).GetPingChecks))
}

// GetRegistrySize mocks base method.
func (m *MockRegistry) GetRegistrySize() (int64, int64) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRegistrySize")
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(int64)
	return ret0, ret1
}

// GetRegistrySize indicates an expected call of GetRegistrySize.
func (mr *MockRegistryMockRecorder) GetRegistrySize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRegistrySize", reflect.TypeOf((*MockRegistry)(nil).GetRegistrySize))
}

// RefreshNamespaceById mocks base method.
func (m *MockRegistry) RefreshNamespaceById(namespaceId ID) (*Namespace, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RefreshNamespaceById", namespaceId)
	ret0, _ := ret[0].(*Namespace)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RefreshNamespaceById indicates an expected call of RefreshNamespaceById.
func (mr *MockRegistryMockRecorder) RefreshNamespaceById(namespaceId any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RefreshNamespaceById", reflect.TypeOf((*MockRegistry)(nil).RefreshNamespaceById), namespaceId)
}

// RegisterStateChangeCallback mocks base method.
func (m *MockRegistry) RegisterStateChangeCallback(key any, cb StateChangeCallbackFn) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterStateChangeCallback", key, cb)
}

// RegisterStateChangeCallback indicates an expected call of RegisterStateChangeCallback.
func (mr *MockRegistryMockRecorder) RegisterStateChangeCallback(key, cb any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterStateChangeCallback", reflect.TypeOf((*MockRegistry)(nil).RegisterStateChangeCallback), key, cb)
}

// Start mocks base method.
func (m *MockRegistry) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockRegistryMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockRegistry)(nil).Start))
}

// Stop mocks base method.
func (m *MockRegistry) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockRegistryMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockRegistry)(nil).Stop))
}

// UnregisterStateChangeCallback mocks base method.
func (m *MockRegistry) UnregisterStateChangeCallback(key any) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UnregisterStateChangeCallback", key)
}

// UnregisterStateChangeCallback indicates an expected call of UnregisterStateChangeCallback.
func (mr *MockRegistryMockRecorder) UnregisterStateChangeCallback(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterStateChangeCallback", reflect.TypeOf((*MockRegistry)(nil).UnregisterStateChangeCallback), key)
}
