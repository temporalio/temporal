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
// Source: common/persistence/client/store.go

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	config "go.temporal.io/server/common/config"
	log "go.temporal.io/server/common/log"
	metrics "go.temporal.io/server/common/metrics"
	persistence "go.temporal.io/server/common/persistence"
	client "go.temporal.io/server/common/persistence/client"
	resolver "go.temporal.io/server/common/resolver"
)

// MockDataStoreFactory is a mock of DataStoreFactory interface.
type MockDataStoreFactory struct {
	ctrl     *gomock.Controller
	recorder *MockDataStoreFactoryMockRecorder
}

// MockDataStoreFactoryMockRecorder is the mock recorder for MockDataStoreFactory.
type MockDataStoreFactoryMockRecorder struct {
	mock *MockDataStoreFactory
}

// NewMockDataStoreFactory creates a new mock instance.
func NewMockDataStoreFactory(ctrl *gomock.Controller) *MockDataStoreFactory {
	mock := &MockDataStoreFactory{ctrl: ctrl}
	mock.recorder = &MockDataStoreFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDataStoreFactory) EXPECT() *MockDataStoreFactoryMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockDataStoreFactory) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockDataStoreFactoryMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDataStoreFactory)(nil).Close))
}

// NewClusterMetadataStore mocks base method.
func (m *MockDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewClusterMetadataStore")
	ret0, _ := ret[0].(persistence.ClusterMetadataStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewClusterMetadataStore indicates an expected call of NewClusterMetadataStore.
func (mr *MockDataStoreFactoryMockRecorder) NewClusterMetadataStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewClusterMetadataStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewClusterMetadataStore))
}

// NewExecutionStore mocks base method.
func (m *MockDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewExecutionStore")
	ret0, _ := ret[0].(persistence.ExecutionStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewExecutionStore indicates an expected call of NewExecutionStore.
func (mr *MockDataStoreFactoryMockRecorder) NewExecutionStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewExecutionStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewExecutionStore))
}

// NewMetadataStore mocks base method.
func (m *MockDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewMetadataStore")
	ret0, _ := ret[0].(persistence.MetadataStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewMetadataStore indicates an expected call of NewMetadataStore.
func (mr *MockDataStoreFactoryMockRecorder) NewMetadataStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewMetadataStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewMetadataStore))
}

// NewNexusEndpointStore mocks base method.
func (m *MockDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewNexusEndpointStore")
	ret0, _ := ret[0].(persistence.NexusEndpointStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewNexusEndpointStore indicates an expected call of NewNexusEndpointStore.
func (mr *MockDataStoreFactoryMockRecorder) NewNexusEndpointStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewNexusEndpointStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewNexusEndpointStore))
}

// NewQueue mocks base method.
func (m *MockDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewQueue", queueType)
	ret0, _ := ret[0].(persistence.Queue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewQueue indicates an expected call of NewQueue.
func (mr *MockDataStoreFactoryMockRecorder) NewQueue(queueType interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewQueue", reflect.TypeOf((*MockDataStoreFactory)(nil).NewQueue), queueType)
}

// NewQueueV2 mocks base method.
func (m *MockDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewQueueV2")
	ret0, _ := ret[0].(persistence.QueueV2)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewQueueV2 indicates an expected call of NewQueueV2.
func (mr *MockDataStoreFactoryMockRecorder) NewQueueV2() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewQueueV2", reflect.TypeOf((*MockDataStoreFactory)(nil).NewQueueV2))
}

// NewShardStore mocks base method.
func (m *MockDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewShardStore")
	ret0, _ := ret[0].(persistence.ShardStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewShardStore indicates an expected call of NewShardStore.
func (mr *MockDataStoreFactoryMockRecorder) NewShardStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewShardStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewShardStore))
}

// NewTaskStore mocks base method.
func (m *MockDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewTaskStore")
	ret0, _ := ret[0].(persistence.TaskStore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewTaskStore indicates an expected call of NewTaskStore.
func (mr *MockDataStoreFactoryMockRecorder) NewTaskStore() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewTaskStore", reflect.TypeOf((*MockDataStoreFactory)(nil).NewTaskStore))
}

// MockAbstractDataStoreFactory is a mock of AbstractDataStoreFactory interface.
type MockAbstractDataStoreFactory struct {
	ctrl     *gomock.Controller
	recorder *MockAbstractDataStoreFactoryMockRecorder
}

// MockAbstractDataStoreFactoryMockRecorder is the mock recorder for MockAbstractDataStoreFactory.
type MockAbstractDataStoreFactoryMockRecorder struct {
	mock *MockAbstractDataStoreFactory
}

// NewMockAbstractDataStoreFactory creates a new mock instance.
func NewMockAbstractDataStoreFactory(ctrl *gomock.Controller) *MockAbstractDataStoreFactory {
	mock := &MockAbstractDataStoreFactory{ctrl: ctrl}
	mock.recorder = &MockAbstractDataStoreFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAbstractDataStoreFactory) EXPECT() *MockAbstractDataStoreFactoryMockRecorder {
	return m.recorder
}

// NewFactory mocks base method.
func (m *MockAbstractDataStoreFactory) NewFactory(cfg config.CustomDatastoreConfig, r resolver.ServiceResolver, clusterName string, logger log.Logger, metricsHandler metrics.Handler) client.DataStoreFactory {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewFactory", cfg, r, clusterName, logger, metricsHandler)
	ret0, _ := ret[0].(client.DataStoreFactory)
	return ret0
}

// NewFactory indicates an expected call of NewFactory.
func (mr *MockAbstractDataStoreFactoryMockRecorder) NewFactory(cfg, r, clusterName, logger, metricsHandler interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewFactory", reflect.TypeOf((*MockAbstractDataStoreFactory)(nil).NewFactory), cfg, r, clusterName, logger, metricsHandler)
}
