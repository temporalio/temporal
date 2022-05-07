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

package standard

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
)

type (
	standardStore struct {
		store store.VisibilityStore
	}

	// We wrap the token with a boolean to indicate if it is from list open workflows or list closed workflows,
	// so we know where to continue from for the next call.
	nextPageToken struct {
		ForOpenWorkflows bool `json:"isOpen"`
		Token            []byte
	}

	listRequest interface {
		OverrideToken(token []byte)
		GetToken() []byte
		OverridePageSize(pageSize int)
		GetPageSize() int
	}
)

var _ store.VisibilityStore = (*standardStore)(nil)
var _ listRequest = (*manager.ListWorkflowExecutionsRequest)(nil)

func NewVisibilityStore(store store.VisibilityStore) store.VisibilityStore {
	return &standardStore{
		store: store,
	}
}

func (s *standardStore) Close() {
	s.store.Close()
}

func (s *standardStore) GetName() string {
	return s.store.GetName()
}

func (s *standardStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	return s.store.RecordWorkflowExecutionStarted(ctx, request)
}

func (s *standardStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	return s.store.RecordWorkflowExecutionClosed(ctx, request)
}

func (s *standardStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	return s.store.UpsertWorkflowExecution(ctx, request)
}

func (s *standardStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	return s.store.DeleteWorkflowExecution(ctx, request)
}

func (s *standardStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutions(ctx, request)
}

func (s *standardStore) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutions(ctx, request)
}

func (s *standardStore) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutionsByType(ctx, request)
}

func (s *standardStore) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByType(ctx, request)
}

func (s *standardStore) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
}

func (s *standardStore) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
}

func (s *standardStore) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ListClosedWorkflowExecutionsByStatus(ctx, request)
}

func (s *standardStore) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return s.store.ScanWorkflowExecutions(ctx, request)
}

func (s *standardStore) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return s.store.CountWorkflowExecutions(ctx, request)
}

func (s *standardStore) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
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
			ctx,
			request,
			s.listOpenWorkflowExecutionsByWorkflowID,
			s.listClosedWorkflowExecutionsByWorkflowID)
	} else if filter.WorkflowTypeName != nil {
		request := &manager.ListWorkflowExecutionsByTypeRequest{
			ListWorkflowExecutionsRequest: baseReq,
			WorkflowTypeName:              *filter.WorkflowTypeName,
		}
		return s.listWorkflowExecutionsHelper(
			ctx,
			request,
			s.listOpenWorkflowExecutionsByType,
			s.listClosedWorkflowExecutionsByType)
	} else if filter.Status != int32(enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED) {
		if filter.Status == int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING) {
			return s.ListOpenWorkflowExecutions(ctx, baseReq)
		} else {
			request := &manager.ListClosedWorkflowExecutionsByStatusRequest{
				ListWorkflowExecutionsRequest: baseReq,
				Status:                        enumspb.WorkflowExecutionStatus(filter.Status),
			}
			return s.ListClosedWorkflowExecutionsByStatus(ctx, request)
		}
	} else {
		return s.listWorkflowExecutionsHelper(
			ctx,
			baseReq,
			s.listOpenWorkflowExecutions,
			s.listClosedWorkflowExecutions)
	}
}

func (s *standardStore) listWorkflowExecutionsHelper(
	ctx context.Context,
	request listRequest,
	listOpenFunc func(ctx context.Context, request listRequest) (*store.InternalListWorkflowExecutionsResponse, error),
	listCloseFunc func(ctx context.Context, request listRequest) (*store.InternalListWorkflowExecutionsResponse, error),
) (*store.InternalListWorkflowExecutionsResponse, error) {

	var token nextPageToken
	if len(request.GetToken()) == 0 {
		token = nextPageToken{
			ForOpenWorkflows: true,
		}
	} else {
		err := json.Unmarshal(request.GetToken(), &token)
		if err != nil {
			return nil, fmt.Errorf("invalid next page token: %v", err)
		}
		request.OverrideToken(token.Token)
	}

	resp := &store.InternalListWorkflowExecutionsResponse{}

	if token.ForOpenWorkflows {
		listOpenResp, err := listOpenFunc(ctx, request)
		if err != nil {
			return nil, err
		}

		if len(listOpenResp.Executions) > 0 {
			request.OverridePageSize(request.GetPageSize() - len(listOpenResp.Executions))
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
			token.ForOpenWorkflows = false
			request.OverrideToken(nil)
		}
	}

	listCloseResp, err := listCloseFunc(ctx, request)
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

func (s *standardStore) listOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByWorkflowIDRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutionsByWorkflowID", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutionsByWorkflowID(ctx, actualRequest)
}

func (s *standardStore) listOpenWorkflowExecutionsByType(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByTypeRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutionsByType", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutionsByType(ctx, actualRequest)
}

func (s *standardStore) listOpenWorkflowExecutions(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listOpenWorkflowExecutions", reflect.TypeOf(request)))
	}

	return s.ListOpenWorkflowExecutions(ctx, actualRequest)
}

func (s *standardStore) listClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByWorkflowIDRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutionsByWorkflowID", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutionsByWorkflowID(ctx, actualRequest)
}

func (s *standardStore) listClosedWorkflowExecutionsByType(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsByTypeRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutionsByType", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutionsByType(ctx, actualRequest)
}

func (s *standardStore) listClosedWorkflowExecutions(
	ctx context.Context,
	request listRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	actualRequest, ok := request.(*manager.ListWorkflowExecutionsRequest)
	if !ok {
		panic(fmt.Errorf("wrong request type %v for listClosedWorkflowExecutions", reflect.TypeOf(request)))
	}

	return s.ListClosedWorkflowExecutions(ctx, actualRequest)
}
