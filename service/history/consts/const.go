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

package consts

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
)

const (
	IdentityHistoryService = "history-service"
	IdentityResetter       = "history-resetter"
	LibraryName            = "go.temporal.io/service/history"
)

var (
	// ErrTaskDiscarded is the error indicating that the standby timer / transfer task is pending for too long and discarded.
	ErrTaskDiscarded = errors.New("passive task pending for too long")
	// ErrTaskVersionMismatch is an error indicating the task is discarded due to version mismatch.
	ErrTaskVersionMismatch = errors.New("task discarded due to version mismatch")
	// ErrTaskRetry is the error indicating that the standby timer / transfer task should be retried since condition in mutable state is not met.
	ErrTaskRetry = errors.New("passive task should retry due to condition in mutable state is not met")
	// ErrDependencyTaskNotCompleted is the error returned when a task this task depends on is not completed yet
	ErrDependencyTaskNotCompleted = errors.New("a task which this task depends on has not been completed yet")
	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("duplicate task, completing it")
	// ErrLocateCurrentWorkflowExecution is the error returned when current workflow execution can't be located
	ErrLocateCurrentWorkflowExecution = serviceerror.NewUnavailable("unable to locate current workflow execution")
	// ErrStaleReference is an indicator that a task or an API request cannot be executed because it contains a stale reference.
	// This is expected in certain situations and it is safe to drop the task or fail a request with a non-retryable error.
	// An example of a stale reference is when the task is pointing to a state machine in mutable state that has a newer
	// version than the version that task was created from, that is the state machine has already transitioned and the
	// task is no longer needed.
	// It is also a NotFoundError to indicate to API callers that the object they're targeting is not found.
	ErrStaleReference = serviceerror.NewNotFound("stale reference")
	// ErrStaleState is the error returned during state update indicating that cached mutable state could be stale after
	// a reload attempt.
	ErrStaleState = staleStateError{}
	// ErrActivityTaskNotFound is the error to indicate activity task could be duplicate and activity already completed
	ErrActivityTaskNotFound = serviceerror.NewNotFound("invalid activityID or activity already timed out or invoking workflow is completed")
	// ErrActivityTaskNotCancelRequested is the error to indicate activity to be canceled is not cancel requested
	ErrActivityTaskNotCancelRequested = serviceerror.NewInvalidArgument("unable to mark activity as canceled without activity being request canceled first")
	// ErrWorkflowCompleted is the error to indicate workflow execution already completed
	ErrWorkflowCompleted = serviceerror.NewNotFound("workflow execution already completed")
	// ErrWorkflowZombie is the error to indicate workflow execution is in zombie state and cannot be updated
	ErrWorkflowZombie = fmt.Errorf("%w: zombie workflow cannot be updated", ErrStaleReference)
	// ErrWorkflowExecutionNotFound is the error to indicate workflow execution does not exist
	ErrWorkflowExecutionNotFound = serviceerror.NewNotFound("workflow execution not found")
	// ErrWorkflowParent is the error to parent execution is given and mismatch
	ErrWorkflowParent = serviceerror.NewNotFound("workflow parent does not match")
	// ErrDeserializingToken is the error to indicate task token is invalid
	ErrDeserializingToken = serviceerror.NewInvalidArgument("error deserializing task token")
	// ErrSignalsLimitExceeded is the error indicating limit reached for maximum number of signal events
	ErrSignalsLimitExceeded = serviceerror.NewInvalidArgument("exceeded workflow execution limit for signal events")
	// ErrWorkflowClosing is the error indicating requests to workflow can not be applied as workflow is closing
	ErrWorkflowClosing = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "workflow operation can not be applied because workflow is closing",
	}
	// ErrEventsAterWorkflowFinish is the error indicating server error trying to write events after workflow finish event
	ErrEventsAterWorkflowFinish = serviceerror.NewInternal("error validating last event being workflow finish event")
	// ErrQueryEnteredInvalidState is error indicating query entered invalid state
	ErrQueryEnteredInvalidState = serviceerror.NewInvalidArgument("query entered invalid state, this should be impossible")
	// ErrConsistentQueryBufferExceeded is error indicating that too many consistent queries have been buffered and until buffered queries are finished new consistent queries cannot be buffered
	ErrConsistentQueryBufferExceeded = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "consistent query buffer is full, this may be caused by too many queries and workflow not able to process query fast enough",
	}
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = serviceerror.NewInvalidArgument("encountered empty history batch")
	// ErrHistorySizeExceedsLimit is error indicating workflow execution has exceeded system defined history size limit
	ErrHistorySizeExceedsLimit = serviceerror.NewInvalidArgument(common.FailureReasonHistorySizeExceedsLimit)
	// ErrHistoryCountExceedsLimit is error indicating workflow execution has exceeded system defined history count limit
	ErrHistoryCountExceedsLimit = serviceerror.NewInvalidArgument(common.FailureReasonHistoryCountExceedsLimit)
	// ErrMutableStateSizeExceedsLimit is error indicating workflow execution has exceeded system defined mutable state size limit
	ErrMutableStateSizeExceedsLimit = serviceerror.NewInvalidArgument(common.FailureReasonMutableStateSizeExceedsLimit)
	// ErrUnknownCluster is error indicating unknown cluster
	ErrUnknownCluster = serviceerror.NewInvalidArgument("unknown cluster")
	// ErrBufferedQueryCleared is error indicating mutable state is cleared while buffered query is pending
	ErrBufferedQueryCleared = serviceerror.NewUnavailable("buffered query cleared, please retry")
	// ErrChildExecutionNotFound is error indicating pending child execution can't be found in workflow mutable state current branch
	ErrChildExecutionNotFound = serviceerror.NewNotFound("Pending child execution not found.")
	// ErrWorkflowNotReady is error indicating workflow mutable state is missing necessary information for handling the request
	ErrWorkflowNotReady = serviceerror.NewWorkflowNotReady("Workflow state is not ready to handle the request.")
	// ErrWorkflowTaskNotScheduled is error indicating workflow task is not scheduled yet.
	ErrWorkflowTaskNotScheduled = serviceerror.NewWorkflowNotReady("Workflow task is not scheduled yet.")
	// ErrNamespaceHandover is error indicating namespace is in handover state and cannot process request.
	ErrNamespaceHandover = common.ErrNamespaceHandover
	// ErrWorkflowTaskStateInconsistent is error indicating workflow task state is inconsistent, for example there was no workflow task scheduled but buffered events are present.
	ErrWorkflowTaskStateInconsistent = serviceerror.NewUnavailable("Workflow task state is inconsistent.")
	// ErrResourceExhaustedBusyWorkflow is an error indicating workflow resource is exhausted and should not be retried by service handler and client
	ErrResourceExhaustedBusyWorkflow = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "Workflow is busy.",
	}
	// ErrResourceExhaustedAPSLimit is an error indicating user has reached their action per second limit
	ErrResourceExhaustedAPSLimit = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "Action per second limit exceeded.",
	}
	// ErrWorkflowClosedBeforeWorkflowTaskStarted is an error indicating workflow execution was closed before WorkflowTaskStarted event
	ErrWorkflowClosedBeforeWorkflowTaskStarted = serviceerror.NewWorkflowNotReady("Workflow execution closed before WorkflowTaskStarted event")

	ErrWorkflowIDNotSet                 = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	ErrInvalidRunID                     = serviceerror.NewInvalidArgument("Invalid RunId.")
	ErrInvalidNextPageToken             = serviceerror.NewInvalidArgument("Invalid NextPageToken.")
	ErrNextPageTokenRunIDMismatch       = serviceerror.NewInvalidArgument("RunId in the request does not match the NextPageToken.")
	ErrInvalidPageSize                  = serviceerror.NewInvalidArgument("Invalid PageSize.")
	ErrInvalidPaginationToken           = serviceerror.NewInvalidArgument("Invalid pagination token.")
	ErrInvalidFirstNextEventCombination = serviceerror.NewInvalidArgument("Invalid FirstEventId and NextEventId combination.")
	ErrInvalidVersionHistories          = serviceerror.NewInvalidArgument("Invalid version histories.")
	ErrInvalidEventQueryRange           = serviceerror.NewInvalidArgument("Invalid event query range.")

	ErrUnableToGetSearchAttributesMessage = "Unable to get search attributes: %v."

	// FailedWorkflowStatuses is a set of failed workflow close states, used for start workflow policy
	// for start workflow execution API
	FailedWorkflowStatuses = map[enumspb.WorkflowExecutionStatus]struct{}{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:     {},
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:   {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED: {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:  {},
	}
)

// StaleStateError is an indicator that after loading the state for a task it was detected as stale. It's possible that
// state reload solves this issue but otherwise it is unexpected and considered terminal.
type staleStateError struct {
	Message string
}

func (staleStateError) Error() string {
	return "cached mutable state could potentially be stale"
}

// IsTerminalTaskError marks this error as terminal to be handled appropriately.
func (staleStateError) IsTerminalTaskError() bool {
	return true
}
