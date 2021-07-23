package visibility

// -aux_files is required here due to Closeable interface being in another file.
//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination visibility_store_mock.go -aux_files go.temporal.io/server/common/persistence=../dataInterfaces.go

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
)

type (
	// VisibilityWorkflowExecutionInfo is visibility info for internal response
	VisibilityWorkflowExecutionInfo struct {
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
		SearchAttributes     map[string]interface{}
	}

	// InternalListWorkflowExecutionsResponse is response from ListWorkflowExecutions
	InternalListWorkflowExecutionsResponse struct {
		Executions []*VisibilityWorkflowExecutionInfo
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// InternalGetClosedWorkflowExecutionResponse is response from GetWorkflowExecution
	InternalGetClosedWorkflowExecutionResponse struct {
		Execution *VisibilityWorkflowExecutionInfo
	}

	// InternalRecordWorkflowExecutionStartedRequest request to RecordWorkflowExecutionStarted
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
		Retention     *time.Duration
	}

	// InternalUpsertWorkflowExecutionRequest is request to UpsertWorkflowExecution
	InternalUpsertWorkflowExecutionRequest struct {
		*InternalVisibilityRequestBase
	}

	// VisibilityStore is the store interface for visibility
	VisibilityStore interface {
		persistence.Closeable
		GetName() string
		RecordWorkflowExecutionStarted(request *InternalRecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(request *InternalRecordWorkflowExecutionClosedRequest) error
		UpsertWorkflowExecution(request *InternalUpsertWorkflowExecutionRequest) error
		ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*InternalListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*InternalListWorkflowExecutionsResponse, error)
		GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*InternalGetClosedWorkflowExecutionResponse, error)
		DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error
		ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*InternalListWorkflowExecutionsResponse, error)
		CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error)
	}
)
