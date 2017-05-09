package persistence

import s "github.com/uber/cadence/.gen/go/shared"

// Interfaces for the Visibility Store.
// This is a secondary store that is eventually consistent with the main
// executions store, and stores workflow execution records for visibility
// purposes.

type (

	// RecordWorkflowExecutionStartedRequest is used to add a record of a newly
	// started execution
	RecordWorkflowExecutionStartedRequest struct {
		DomainUUID       string
		Execution        s.WorkflowExecution
		WorkflowTypeName string
		StartTimestamp   int64
	}

	// RecordWorkflowExecutionClosedRequest is used to add a record of a newly
	// closed execution
	RecordWorkflowExecutionClosedRequest struct {
		DomainUUID       string
		Execution        s.WorkflowExecution
		WorkflowTypeName string
		StartTimestamp   int64
		CloseTimestamp   int64
		Status           s.WorkflowExecutionCloseStatus
	}

	// ListWorkflowExecutionsRequest is used to list executions in a domain
	ListWorkflowExecutionsRequest struct {
		DomainUUID        string
		EarliestStartTime int64
		LatestStartTime   int64
		// Maximum number of workflow executions per page
		PageSize int
		// Token to continue reading next page of workflow executions.
		// Pass in empty slice for first page.
		NextPageToken []byte
	}

	// ListWorkflowExecutionsResponse is the response to ListWorkflowExecutionsRequest
	ListWorkflowExecutionsResponse struct {
		Executions []*s.WorkflowExecutionInfo
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// ListWorkflowExecutionsByTypeRequest is used to list executions of
	// a specific type in a domain
	ListWorkflowExecutionsByTypeRequest struct {
		ListWorkflowExecutionsRequest
		WorkflowTypeName string
	}

	// ListWorkflowExecutionsByWorkflowIDRequest is used to list executions that
	// have specific WorkflowID in a domain
	ListWorkflowExecutionsByWorkflowIDRequest struct {
		ListWorkflowExecutionsRequest
		WorkflowID string
	}

	// ListClosedWorkflowExecutionsByStatusRequest is used to list executions that
	// have specific close status
	ListClosedWorkflowExecutionsByStatusRequest struct {
		ListWorkflowExecutionsRequest
		Status s.WorkflowExecutionCloseStatus
	}

	// VisibilityManager is used to manage the visibility store
	VisibilityManager interface {
		RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error
		ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error)
		ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error)
	}
)
