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
	// ErrTaskRetry is the error indicating that the standby timer / transfer task should be retried since condition in mutable state is not met.
	ErrTaskRetry = errors.New("passive task should retry due to condition in mutable state is not met")
	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("duplicate task, completing it")
	// ErrConflict is exported temporarily for integration test
	ErrConflict = errors.New("conditional update failed")
	// ErrMaxAttemptsExceeded is exported temporarily for integration test
	ErrMaxAttemptsExceeded = errors.New("maximum attempts exceeded to update history")
	// ErrStaleState is the error returned during state update indicating that cached mutable state could be stale
	ErrStaleState = errors.New("cache mutable state could potentially be stale")
	// ErrActivityTaskNotFound is the error to indicate activity task could be duplicate and activity already completed
	ErrActivityTaskNotFound = serviceerror.NewNotFound("invalid activityID or activity already timed out or invoking workflow is completed")
	// ErrActivityTaskNotCancelRequested is the error to indicate activity to be canceled is not cancel requested
	ErrActivityTaskNotCancelRequested = serviceerror.NewInvalidArgument("unable to mark activity as canceled without activity being request canceled first")
	// ErrWorkflowCompleted is the error to indicate workflow execution already completed
	ErrWorkflowCompleted = serviceerror.NewNotFound("workflow execution already completed")
	// ErrWorkflowExecutionNotFound is the error to indicate workflow execution does not exist
	ErrWorkflowExecutionNotFound = serviceerror.NewNotFound("workflow execution not found")
	// ErrWorkflowParent is the error to parent execution is given and mismatch
	ErrWorkflowParent = serviceerror.NewNotFound("workflow parent does not match")
	// ErrDeserializingToken is the error to indicate task token is invalid
	ErrDeserializingToken = serviceerror.NewInvalidArgument("error deserializing task token")
	// ErrSignalsLimitExceeded is the error indicating limit reached for maximum number of signal events
	ErrSignalsLimitExceeded = serviceerror.NewInvalidArgument("exceeded workflow execution limit for signal events")
	// ErrEventsAterWorkflowFinish is the error indicating server error trying to write events after workflow finish event
	ErrEventsAterWorkflowFinish = serviceerror.NewInternal("error validating last event being workflow finish event")
	// ErrQueryEnteredInvalidState is error indicating query entered invalid state
	ErrQueryEnteredInvalidState = serviceerror.NewInvalidArgument("query entered invalid state, this should be impossible")
	// ErrConsistentQueryBufferExceeded is error indicating that too many consistent queries have been buffered and until buffered queries are finished new consistent queries cannot be buffered
	ErrConsistentQueryBufferExceeded = serviceerror.NewUnavailable("consistent query buffer is full, cannot accept new consistent queries")
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = serviceerror.NewInvalidArgument("encountered empty history batch")
	// ErrSizeExceedsLimit is error indicating workflow execution has exceeded system defined limit
	ErrSizeExceedsLimit = serviceerror.NewInvalidArgument(common.FailureReasonSizeExceedsLimit)
	// ErrUnknownCluster is error indicating unknown cluster
	ErrUnknownCluster = serviceerror.NewInvalidArgument("unknown cluster")
	// ErrBufferedQueryCleared is error indicating mutable state is cleared while buffered query is pending
	ErrBufferedQueryCleared = serviceerror.NewUnavailable("buffered query cleared, please retry")
	// ErrWorkflowBusy is error indicating workflow is currently busy and workflow context can't be locked within specified timeout
	ErrWorkflowBusy = serviceerror.NewUnavailable("timeout locking workflow execution")
	// ErrChildExecutionNotFound is error indicating pending child execution can't be found in workflow mutable state current branch
	ErrChildExecutionNotFound = serviceerror.NewNotFound("Pending child execution not found.")
	// ErrWorkflowNotReady is error indicating workflow mutable state is missing necessary information for handling the request
	ErrWorkflowNotReady = serviceerror.NewWorkflowNotReady("Workflow state is not ready to handle the request.")
	// ErrWorkflowTaskNotScheduled is error indicating workflow task is not scheduled yet.
	ErrWorkflowTaskNotScheduled = serviceerror.NewWorkflowNotReady("Workflow task is not scheduled yet.")

	// FailedWorkflowStatuses is a set of failed workflow close states, used for start workflow policy
	// for start workflow execution API
	FailedWorkflowStatuses = map[enumspb.WorkflowExecutionStatus]struct{}{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:     {},
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:   {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED: {},
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:  {},
	}
)
