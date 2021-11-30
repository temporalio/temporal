// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package simple

import (
	"encoding/json"
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
)

type (
	simpleStore struct {
		store store.VisibilityStore
	}

	nextPageToken struct {
		IsForOpen bool
		Token     []byte
	}

	listRequest interface {
		SetToken(token []byte)
		GetToken() []byte
		SetPageSize(pageSize int)
		GetPageSize() int
	}
)

var _ store.VisibilityStore = (*simpleStore)(nil)

func NewVisibilityStore(store store.VisibilityStore) store.VisibilityStore {
	return &simpleStore{
		store: store,
	}
}

func (s *simpleStore) Close() {
	s.store.Close()
}

func (s *simpleStore) GetName() string {
	return s.store.GetName()
}

func (s *simpleStore) RecordWorkflowExecutionStarted(request *store.InternalRecordWorkflowExecutionStartedRequest) error {
	return s.store.RecordWorkflowExecutionStarted(request)
}

func (s *simpleStore) RecordWorkflowExecutionClosed(request *store.InternalRecordWorkflowExecutionClosedRequest) error {
	return s.store.RecordWorkflowExecutionClosed(request)
}

func (s *simpleStore) UpsertWorkflowExecution(request *store.InternalUpsertWorkflowExecutionRequest) error {
	return s.store.UpsertWorkflowExecution(request)
}

func (s *simpleStore) DeleteWorkflowExecution(request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	return s.store.DeleteWorkflowExecution(request)
}

func (s *simpleStore) ListOpenWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutions(request)
}

func (s *simpleStore) ListClosedWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutions(request)
}

func (s *simpleStore) ListOpenWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutionsByType(request)
}

func (s *simpleStore) ListClosedWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByType(request)
}

func (s *simpleStore) ListOpenWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (s *simpleStore) ListClosedWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (s *simpleStore) ListClosedWorkflowExecutionsByStatus(request *manager.ListClosedWorkflowExecutionsByStatusRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByStatus(request)
}

func (s *simpleStore) ScanWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ScanWorkflowExecutions(request)
}

func (s *simpleStore) CountWorkflowExecutions(request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
	return s.store.CountWorkflowExecutions(request)
}

func (s *simpleStore) ListWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*store.InternalListWorkflowExecutionsResponse, error) {
	converter := newQueryConverter()
	filter, err := converter.GetFilter(request.Query)
	if err != nil {
		return nil, err
	}

	baseReq := &manager.ListWorkflowExecutionsRequest{
		NamespaceID:       request.NamespaceID,
		Namespace:         request.Namespace,
		PageSize:          request.PageSize,
		NextPageToken:     request.NextPageToken,
		EarliestStartTime: *filter.MinTime,
		LatestStartTime:   *filter.MaxTime,
	}

	// Only a limited query patterns are supported due to the way we set up
	// visibility tables in Cassandra.
	// Check validation logic in query interceptor for details.
	if filter.WorkflowID != nil {
		request := &manager.ListWorkflowExecutionsByWorkflowIDRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowID:                    *filter.WorkflowID,
		}
		return s.listWorkflowExecutionsHelper(
			request,
			s.listOpenWorkflowExecutionsByWorkflowID,
			s.listClosedWorkflowExecutionsByWorkflowID)
	} else if filter.WorkflowTypeName != nil {
		request := &manager.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              *filter.WorkflowTypeName,
		}
		return s.listWorkflowExecutionsHelper(
			request,
			s.listOpenWorkflowExecutionsByType,
			s.listClosedWorkflowExecutionsByType)
	} else if filter.Status != int32(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED) {
		if filter.Status == int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
			return s.ListOpenWorkflowExecutions(baseReq)
		} else {
			request := &manager.ListClosedWorkflowExecutionsByStatusRequest{
				ListWorkflowExecutionsRequest: baseReq,
				Status:                        enumspb.WorkflowExecutionStatus(filter.Status),
			}
			return s.ListClosedWorkflowExecutionsByStatus(request)
		}
	} else {
		return s.listWorkflowExecutionsHelper(
			baseReq,
			s.listOpenWorkflowExecutions,
			s.listClosedWorkflowExecutions)
	}
}

func (s *simpleStore) listWorkflowExecutionsHelper(
	request listRequest,
	listOpenFunc func(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error),
	listCloseFunc func(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error),
) (*store.InternalListWorkflowExecutionsResponse, error) {

	var token nextPageToken
	if len(request.GetToken()) == 0 {
		token = nextPageToken{
			IsForOpen: true,
		}
	} else {
		err := json.Unmarshal(request.GetToken(), &token)
		if err != nil {
			return nil, fmt.Errorf("invalid next page token: %v", err)
		}
		request.SetToken(token.Token)
	}

	resp := &store.InternalListWorkflowExecutionsResponse{}

	if token.IsForOpen {
		listOpenResp, err := listOpenFunc(request)
		if err != nil {
			return nil, err
		}

		if len(listOpenResp.Executions) > 0 {
			request.SetPageSize(request.GetPageSize() - len(listOpenResp.Executions))
			resp.Executions = append(resp.Executions, listOpenResp.Executions...)
		}

		if request.GetPageSize() == 0 {
			token.Token = listOpenResp.NextPageToken

			token, err := json.Marshal(token)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal next page token: %v", err)
			}

			resp.NextPageToken = token
			return resp, nil
		} else {
			token.IsForOpen = false
			request.SetToken(nil)
		}
	}

	listCloseResp, err := listCloseFunc(request)
	if err != nil {
		return nil, err
	}
	resp.Executions = append(resp.Executions, listCloseResp.Executions...)

	if listCloseResp.NextPageToken == nil {
		resp.NextPageToken = nil
	} else {
		token.Token = listCloseResp.NextPageToken
		token, err := json.Marshal(token)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal next page token: %v", err)
		}

		resp.NextPageToken = token
	}

	return resp, nil
}

func (s *simpleStore) listOpenWorkflowExecutionsByWorkflowID(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByWorkflowIDRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutionsByWorkflowID", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutionsByWorkflowID(actualRequest)
}

func (s *simpleStore) listOpenWorkflowExecutionsByType(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByTypeRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutionsByType", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutionsByType(actualRequest)
}

func (s *simpleStore) listOpenWorkflowExecutions(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutions", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutions(actualRequest)
}

func (s *simpleStore) listClosedWorkflowExecutionsByWorkflowID(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByWorkflowIDRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutionsByWorkflowID", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutionsByWorkflowID(actualRequest)
}

func (s *simpleStore) listClosedWorkflowExecutionsByType(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByTypeRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutionsByType", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutionsByType(actualRequest)
}

func (s *simpleStore) listClosedWorkflowExecutions(request listRequest) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutions", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutions(actualRequest)
}
