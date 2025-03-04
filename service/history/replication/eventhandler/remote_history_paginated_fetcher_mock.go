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
// Source: remote_history_paginated_fetcher.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../../../../LICENSE -package eventhandler -source remote_history_paginated_fetcher.go -destination remote_history_paginated_fetcher_mock.go
//

// Package eventhandler is a generated GoMock package.
package eventhandler

import (
	context "context"
	reflect "reflect"

	collection "go.temporal.io/server/common/collection"
	namespace "go.temporal.io/server/common/namespace"
	gomock "go.uber.org/mock/gomock"
)

// MockHistoryPaginatedFetcher is a mock of HistoryPaginatedFetcher interface.
type MockHistoryPaginatedFetcher struct {
	ctrl     *gomock.Controller
	recorder *MockHistoryPaginatedFetcherMockRecorder
}

// MockHistoryPaginatedFetcherMockRecorder is the mock recorder for MockHistoryPaginatedFetcher.
type MockHistoryPaginatedFetcherMockRecorder struct {
	mock *MockHistoryPaginatedFetcher
}

// NewMockHistoryPaginatedFetcher creates a new mock instance.
func NewMockHistoryPaginatedFetcher(ctrl *gomock.Controller) *MockHistoryPaginatedFetcher {
	mock := &MockHistoryPaginatedFetcher{ctrl: ctrl}
	mock.recorder = &MockHistoryPaginatedFetcherMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHistoryPaginatedFetcher) EXPECT() *MockHistoryPaginatedFetcherMockRecorder {
	return m.recorder
}

// GetSingleWorkflowHistoryPaginatedIteratorExclusive mocks base method.
func (m *MockHistoryPaginatedFetcher) GetSingleWorkflowHistoryPaginatedIteratorExclusive(ctx context.Context, remoteClusterName string, namespaceID namespace.ID, workflowID, runID string, startEventID, startEventVersion, endEventID, endEventVersion int64) collection.Iterator[*HistoryBatch] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSingleWorkflowHistoryPaginatedIteratorExclusive", ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
	ret0, _ := ret[0].(collection.Iterator[*HistoryBatch])
	return ret0
}

// GetSingleWorkflowHistoryPaginatedIteratorExclusive indicates an expected call of GetSingleWorkflowHistoryPaginatedIteratorExclusive.
func (mr *MockHistoryPaginatedFetcherMockRecorder) GetSingleWorkflowHistoryPaginatedIteratorExclusive(ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSingleWorkflowHistoryPaginatedIteratorExclusive", reflect.TypeOf((*MockHistoryPaginatedFetcher)(nil).GetSingleWorkflowHistoryPaginatedIteratorExclusive), ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
}

// GetSingleWorkflowHistoryPaginatedIteratorInclusive mocks base method.
func (m *MockHistoryPaginatedFetcher) GetSingleWorkflowHistoryPaginatedIteratorInclusive(ctx context.Context, remoteClusterName string, namespaceID namespace.ID, workflowID, runID string, startEventID, startEventVersion, endEventID, endEventVersion int64) collection.Iterator[*HistoryBatch] {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSingleWorkflowHistoryPaginatedIteratorInclusive", ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
	ret0, _ := ret[0].(collection.Iterator[*HistoryBatch])
	return ret0
}

// GetSingleWorkflowHistoryPaginatedIteratorInclusive indicates an expected call of GetSingleWorkflowHistoryPaginatedIteratorInclusive.
func (mr *MockHistoryPaginatedFetcherMockRecorder) GetSingleWorkflowHistoryPaginatedIteratorInclusive(ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSingleWorkflowHistoryPaginatedIteratorInclusive", reflect.TypeOf((*MockHistoryPaginatedFetcher)(nil).GetSingleWorkflowHistoryPaginatedIteratorInclusive), ctx, remoteClusterName, namespaceID, workflowID, runID, startEventID, startEventVersion, endEventID, endEventVersion)
}
