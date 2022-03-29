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

package store

// -aux_files is required here due to Closeable interface being in another file.
//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination visibility_store_mock.go -aux_files go.temporal.io/server/common/persistence=../../dataInterfaces.go

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	// VisibilityStore is the store interface for visibility
	VisibilityStore interface {
		persistence.Closeable
		GetName() string

		// Write APIs.
		RecordWorkflowExecutionStarted(ctx context.Context, request *InternalRecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(ctx context.Context, request *InternalRecordWorkflowExecutionClosedRequest) error
		UpsertWorkflowExecution(ctx context.Context, request *InternalUpsertWorkflowExecutionRequest) error
		DeleteWorkflowExecution(ctx context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error

		// Read APIs.
		ListOpenWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByType(ctx context.Context, request *manager.ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByType(ctx context.Context, request *manager.ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByWorkflowID(ctx context.Context, request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByWorkflowID(ctx context.Context, request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByStatus(ctx context.Context, request *manager.ListClosedWorkflowExecutionsByStatusRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		ScanWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		CountWorkflowExecutions(ctx context.Context, request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error)
	}

	// InternalWorkflowExecutionInfo is visibility info for internal response
	InternalWorkflowExecutionInfo struct {
		WorkflowID           string
		RunID                string
		TypeName             string
		StartTime            time.Time
		ExecutionTime        time.Time
		CloseTime            time.Time
		Status               enumspb.WorkflowExecutionStatus
		HistoryLength        int64
		StateTransitionCount int64
		Memo                 *commonpb.DataBlob
		TaskQueue            string
		SearchAttributes     *commonpb.SearchAttributes
	}

	// InternalListWorkflowExecutionsResponse is response from ListWorkflowExecutions
	InternalListWorkflowExecutionsResponse struct {
		Executions []*InternalWorkflowExecutionInfo
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// InternalVisibilityRequestBase is a base request to visibility APIs.
	InternalVisibilityRequestBase struct {
		NamespaceID          string
		WorkflowID           string
		RunID                string
		WorkflowTypeName     string
		StartTime            time.Time
		Status               enumspb.WorkflowExecutionStatus
		ExecutionTime        time.Time
		StateTransitionCount int64
		TaskID               int64
		ShardID              int32
		Memo                 *commonpb.DataBlob
		TaskQueue            string
		SearchAttributes     *commonpb.SearchAttributes
	}

	// InternalRecordWorkflowExecutionStartedRequest request to RecordWorkflowExecutionStarted
	InternalRecordWorkflowExecutionStartedRequest struct {
		*InternalVisibilityRequestBase
	}

	// InternalRecordWorkflowExecutionClosedRequest is request to RecordWorkflowExecutionClosed
	InternalRecordWorkflowExecutionClosedRequest struct {
		*InternalVisibilityRequestBase
		CloseTime     time.Time
		HistoryLength int64
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		*InternalVisibilityRequestBase
	}
)
