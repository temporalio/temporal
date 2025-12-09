package store

// -aux_files is required here due to Closeable interface being in another file.
//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination visibility_store_mock.go -aux_files go.temporal.io/server/common/persistence=../../data_interfaces.go

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
		GetIndexName() string

		// Validate search attributes based on the store constraints. It returns a new map containing
		// only search attributes with valid values. If there are invalid values, it returns error of type
		// serviceerror.InvalidArgument.
		ValidateCustomSearchAttributes(searchAttributes map[string]any) (map[string]any, error)

		// Write APIs.
		RecordWorkflowExecutionStarted(ctx context.Context, request *InternalRecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(ctx context.Context, request *InternalRecordWorkflowExecutionClosedRequest) error
		UpsertWorkflowExecution(ctx context.Context, request *InternalUpsertWorkflowExecutionRequest) error
		DeleteWorkflowExecution(ctx context.Context, request *manager.VisibilityDeleteWorkflowExecutionRequest) error

		// Read APIs.
		ListWorkflowExecutions(ctx context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*InternalListExecutionsResponse, error)
		ListChasmExecutions(ctx context.Context, request *manager.ListChasmExecutionsRequest) (*InternalListExecutionsResponse, error)
		CountWorkflowExecutions(ctx context.Context, request *manager.CountWorkflowExecutionsRequest) (*InternalCountExecutionsResponse, error)
		CountChasmExecutions(ctx context.Context, request *manager.CountChasmExecutionsRequest) (*InternalCountExecutionsResponse, error)
		GetWorkflowExecution(ctx context.Context, request *manager.GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)

		// Admin APIs

		// AddSearchAttributes makes schema changes to add the search attributes. This function must be
		// idempotent, ie., if a search attribute already exists, this function must be no-op, and must
		// not return any error.
		AddSearchAttributes(ctx context.Context, request *manager.AddSearchAttributesRequest) error
	}

	// InternalExecutionInfo is internal visibility info for workflow execution
	InternalExecutionInfo struct {
		WorkflowID           string
		RunID                string
		TypeName             string
		StartTime            time.Time
		ExecutionTime        time.Time
		CloseTime            time.Time
		ExecutionDuration    time.Duration
		Status               enumspb.WorkflowExecutionStatus
		HistoryLength        int64
		HistorySizeBytes     int64
		StateTransitionCount int64
		Memo                 *commonpb.DataBlob
		TaskQueue            string
		SearchAttributes     *commonpb.SearchAttributes
		ParentWorkflowID     string
		ParentRunID          string
		RootWorkflowID       string
		RootRunID            string
	}

	// InternalListExecutionsResponse is response from ListWorkflowExecutions and ListChasmExecutions
	InternalListExecutionsResponse struct {
		Executions []*InternalExecutionInfo
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// InternalCountExecutionsResponse is response from CountWorkflowExecutions and CountChasmExecutions
	InternalCountExecutionsResponse struct {
		Count  int64
		Groups []InternalAggregationGroup
	}

	// InternalAggregationGroup represents a GROUP BY aggregation result
	InternalAggregationGroup struct {
		GroupValues []*commonpb.Payload
		Count       int64
	}

	// InternalGetWorkflowExecutionResponse is response from GetWorkflowExecution
	InternalGetWorkflowExecutionResponse struct {
		Execution *InternalExecutionInfo
	}

	// InternalVisibilityRequestBase is a base request to visibility APIs.
	InternalVisibilityRequestBase struct {
		NamespaceID      string
		WorkflowID       string
		RunID            string
		WorkflowTypeName string
		StartTime        time.Time
		Status           enumspb.WorkflowExecutionStatus
		ExecutionTime    time.Time
		TaskID           int64
		ShardID          int32
		Memo             *commonpb.DataBlob
		TaskQueue        string
		SearchAttributes *commonpb.SearchAttributes
		ParentWorkflowID *string
		ParentRunID      *string
		RootWorkflowID   string
		RootRunID        string
	}

	// InternalRecordWorkflowExecutionStartedRequest request to RecordWorkflowExecutionStarted
	InternalRecordWorkflowExecutionStartedRequest struct {
		*InternalVisibilityRequestBase
	}

	// InternalRecordWorkflowExecutionClosedRequest is request to RecordWorkflowExecutionClosed
	InternalRecordWorkflowExecutionClosedRequest struct {
		*InternalVisibilityRequestBase
		CloseTime            time.Time
		HistoryLength        int64
		HistorySizeBytes     int64
		ExecutionDuration    time.Duration
		StateTransitionCount int64
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		*InternalVisibilityRequestBase
	}
)
