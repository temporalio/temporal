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

package workflow

import (
	"math"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/primitives/timestamp"
)

// TODO treat 0 as 0, not infinite

func getBackoffInterval(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *time.Duration,
	maxInterval *time.Duration,
	expirationTime *time.Time,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) (time.Duration, enumspb.RetryState) {

	// TODO remove below checks, most are already set with correct values
	if currentAttempt < 1 {
		currentAttempt = 1
	}

	if initInterval == nil {
		initInterval = timestamp.DurationPtr(time.Duration(0))
	}

	if maxInterval != nil && *maxInterval == 0 {
		maxInterval = nil
	}

	if expirationTime != nil && expirationTime.IsZero() {
		expirationTime = nil
	}
	// TODO remove above checks, most are already set with correct values

	if !isRetryable(failure, nonRetryableTypes) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}

	// currentAttempt starts from 1.
	// maxAttempts is the total attempts, including initial (non-retry) attempt.
	// At this point we are about to make next attempt and all calculations in this func are for this next attempt.
	// For example, if maxAttempts is set to 2 and we are making 1st retry, currentAttempt will be 1
	// (we made 1 non-retry attempt already) and condition (currentAttempt+1 > maxAttempts) will be false.
	// With 2nd retry, currentAttempt will be 2 (1 non-retry + 1 retry attempt already made) and
	// condition (currentAttempt >= maxAttempts) will be true (means stop retrying, we tried 2 times already).
	if maxAttempts > 0 && currentAttempt >= maxAttempts {
		return backoff.NoBackoff, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
	}

	interval := time.Duration(int64(float64(initInterval.Nanoseconds()) * math.Pow(backoffCoefficient, float64(currentAttempt-1))))
	if maxInterval != nil && (interval <= 0 || interval > *maxInterval) {
		interval = *maxInterval
	} else if maxInterval == nil && interval <= 0 {
		return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
	} else {
		// maxInterval != nil && (0 < interval && interval <= *maxInterval)
		// or
		// maxInterval == nil && interval > 0
	}

	if expirationTime != nil && now.Add(interval).After(*expirationTime) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
	}
	return interval, enumspb.RETRY_STATE_IN_PROGRESS
}

func isRetryable(failure *failurepb.Failure, nonRetryableTypes []string) bool {
	if failure == nil {
		return true
	}

	if failure.GetTerminatedFailureInfo() != nil || failure.GetCanceledFailureInfo() != nil {
		return false
	}

	if failure.GetTimeoutFailureInfo() != nil {
		return failure.GetTimeoutFailureInfo().GetTimeoutType() == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
			failure.GetTimeoutFailureInfo().GetTimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT
	}

	if failure.GetServerFailureInfo() != nil {
		return !failure.GetServerFailureInfo().GetNonRetryable()
	}

	if failure.GetApplicationFailureInfo() != nil {
		if failure.GetApplicationFailureInfo().GetNonRetryable() {
			return false
		}

		failureType := failure.GetApplicationFailureInfo().GetType()
		for _, nrt := range nonRetryableTypes {
			if nrt == failureType {
				return false
			}
		}
	}
	return true
}

// Helpers for creating new retry/cron workflows:

// QUESTION: is this a good name? any suggestions?
func SetupNewWorkflowForRetryOrCron(
	oldMs MutableState,
	newMs MutableState,
	newRunId string,
	startAttr *historypb.WorkflowExecutionStartedEventAttributes,
	lastCompletionResult *commonpb.Payloads,
	failure *failurepb.Failure,
	backoffInterval time.Duration,
	initiator enumspb.ContinueAsNewInitiator,
) error {

	// Extract ParentExecutionInfo from current run so it can be passed down to the next
	var parentInfo *workflowspb.ParentExecutionInfo
	executionInfo := oldMs.GetExecutionInfo()
	if oldMs.HasParentExecution() {
		parentInfo = &workflowspb.ParentExecutionInfo{
			NamespaceId: executionInfo.ParentNamespaceId,
			Namespace:   startAttr.GetParentWorkflowNamespace(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.ParentWorkflowId,
				RunId:      executionInfo.ParentRunId,
			},
			InitiatedId: executionInfo.InitiatedId,
		}
	}

	newExecution := commonpb.WorkflowExecution{
		WorkflowId: executionInfo.WorkflowId,
		RunId:      newRunId,
	}

	firstRunID, err := oldMs.GetFirstRunID()
	if err != nil {
		return err
	}

	previousExecutionInfo := oldMs.GetExecutionInfo()
	taskQueue := previousExecutionInfo.TaskQueue
	if startAttr.TaskQueue != nil {
		taskQueue = startAttr.TaskQueue.GetName()
	}
	tq := &taskqueuepb.TaskQueue{
		Name: taskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	workflowType := previousExecutionInfo.WorkflowTypeName
	if startAttr.WorkflowType != nil {
		workflowType = startAttr.WorkflowType.GetName()
	}
	wType := &commonpb.WorkflowType{}
	wType.Name = workflowType

	var taskTimeout *time.Duration
	if timestamp.DurationValue(startAttr.GetWorkflowTaskTimeout()) == 0 {
		taskTimeout = previousExecutionInfo.DefaultWorkflowTaskTimeout
	} else {
		taskTimeout = startAttr.GetWorkflowTaskTimeout()
	}

	// Workflow runTimeout is already set to the correct value in
	// validateContinueAsNewWorkflowExecutionAttributes
	runTimeout := startAttr.GetWorkflowRunTimeout()

	createRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                newMs.GetNamespaceEntry().GetInfo().Name,
		WorkflowId:               newExecution.WorkflowId,
		TaskQueue:                tq,
		WorkflowType:             wType,
		WorkflowExecutionTimeout: oldMs.GetExecutionInfo().WorkflowExecutionTimeout,
		WorkflowRunTimeout:       runTimeout,
		WorkflowTaskTimeout:      taskTimeout,
		Input:                    startAttr.Input,
		Header:                   startAttr.Header,
		RetryPolicy:              startAttr.RetryPolicy,
		CronSchedule:             startAttr.CronSchedule,
		Memo:                     startAttr.Memo,
		SearchAttributes:         startAttr.SearchAttributes,
	}

	attempt := int32(1)
	if initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY {
		attempt = oldMs.GetExecutionInfo().Attempt + 1
	}

	req := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:              newMs.GetNamespaceEntry().GetInfo().Id,
		StartRequest:             createRequest,
		ParentExecutionInfo:      parentInfo,
		LastCompletionResult:     lastCompletionResult,
		ContinuedFailure:         failure,
		ContinueAsNewInitiator:   initiator,
		FirstWorkflowTaskBackoff: timestamp.DurationPtr(backoffInterval),
		Attempt:                  attempt,
	}
	workflowTimeoutTime := timestamp.TimeValue(oldMs.GetExecutionInfo().WorkflowExecutionExpirationTime)
	if !workflowTimeoutTime.IsZero() {
		req.WorkflowExecutionExpirationTime = &workflowTimeoutTime
	}

	event, err := newMs.AddWorkflowExecutionStartedEventWithOptions(
		newExecution,
		req,
		parentInfo.GetNamespaceId(),
		previousExecutionInfo.AutoResetPoints,
		oldMs.GetExecutionState().GetRunId(),
		firstRunID,
	)
	if err != nil {
		return serviceerror.NewInternal("Failed to add workflow execution started event.")
	}
	if err = newMs.AddFirstWorkflowTaskScheduled(event); err != nil {
		return err
	}

	return nil
}
