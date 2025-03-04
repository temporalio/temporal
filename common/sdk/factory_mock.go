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
//

// Code generated by MockGen. DO NOT EDIT.
// Source: factory.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../LICENSE -package sdk -source factory.go -destination factory_mock.go
//

// Package sdk is a generated GoMock package.
package sdk

import (
	reflect "reflect"

	client "go.temporal.io/sdk/client"
	worker "go.temporal.io/sdk/worker"
	gomock "go.uber.org/mock/gomock"
)

// MockClientFactory is a mock of ClientFactory interface.
type MockClientFactory struct {
	ctrl     *gomock.Controller
	recorder *MockClientFactoryMockRecorder
}

// MockClientFactoryMockRecorder is the mock recorder for MockClientFactory.
type MockClientFactoryMockRecorder struct {
	mock *MockClientFactory
}

// NewMockClientFactory creates a new mock instance.
func NewMockClientFactory(ctrl *gomock.Controller) *MockClientFactory {
	mock := &MockClientFactory{ctrl: ctrl}
	mock.recorder = &MockClientFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClientFactory) EXPECT() *MockClientFactoryMockRecorder {
	return m.recorder
}

// GetSystemClient mocks base method.
func (m *MockClientFactory) GetSystemClient() client.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSystemClient")
	ret0, _ := ret[0].(client.Client)
	return ret0
}

// GetSystemClient indicates an expected call of GetSystemClient.
func (mr *MockClientFactoryMockRecorder) GetSystemClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSystemClient", reflect.TypeOf((*MockClientFactory)(nil).GetSystemClient))
}

// NewClient mocks base method.
func (m *MockClientFactory) NewClient(options client.Options) client.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewClient", options)
	ret0, _ := ret[0].(client.Client)
	return ret0
}

// NewClient indicates an expected call of NewClient.
func (mr *MockClientFactoryMockRecorder) NewClient(options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewClient", reflect.TypeOf((*MockClientFactory)(nil).NewClient), options)
}

// NewWorker mocks base method.
func (m *MockClientFactory) NewWorker(client client.Client, taskQueue string, options worker.Options) worker.Worker {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewWorker", client, taskQueue, options)
	ret0, _ := ret[0].(worker.Worker)
	return ret0
}

// NewWorker indicates an expected call of NewWorker.
func (mr *MockClientFactoryMockRecorder) NewWorker(client, taskQueue, options any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewWorker", reflect.TypeOf((*MockClientFactory)(nil).NewWorker), client, taskQueue, options)
}
