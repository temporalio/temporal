package persistence

import (
	"time"

	s "github.com/uber/cadence/.gen/go/shared"
)

// Interfaces for the Visibility Store.
// This is a secondary store that is eventually consistent with the main
// executions store, and stores workflow execution records for visibility
// purposes.

type (

	// WorkflowExecutionRecord contains info about workflow execution
	WorkflowExecutionRecord struct {
		Execution        s.WorkflowExecution
		WorkflowTypeName string
		StartTime        time.Time
		CloseTime        time.Time
	}

	// RecordWorkflowExecutionStartedRequest is used to add a record of a newly
	// started execution
	RecordWorkflowExecutionStartedRequest struct {
		DomainUUID       string
		Execution        s.WorkflowExecution
		WorkflowTypeName string
		StartTime        time.Time
	}

	// RecordWorkflowExecutionClosedRequest is used to add a record of a newly
	// closed execution
	RecordWorkflowExecutionClosedRequest struct {
		DomainUUID       string
		Execution        s.WorkflowExecution
		WorkflowTypeName string
		StartTime        time.Time
		CloseTime        time.Time
	}

	// ListWorkflowExecutionsRequest is used to list executions in a domain
	ListWorkflowExecutionsRequest struct {
		DomainUUID string
		// Maximum number of workflow executions per page
		PageSize int
		// Token to continue reading next page of workflow executions.
		// Pass in empty slice for first page.
		NextPageToken []byte
	}

	// ListWorkflowExecutionsResponse is the response to ListWorkflowExecutionsRequest
	ListWorkflowExecutionsResponse struct {
		Executions []*WorkflowExecutionRecord
		// Token to read next page if there are more workflow executions beyond page size.
		// Use this to set NextPageToken on ListWorkflowExecutionsRequest to read the next page.
		NextPageToken []byte
	}

	// VisibilityManager is used to manage the visibility store
	VisibilityManager interface {
		RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error
		RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error
		ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
		ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
	}
)
