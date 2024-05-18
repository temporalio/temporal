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
	"context"
	"math"
	"time"

	"github.com/pborman/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/worker_versioning"
)

type BackoffCalculatorAlgorithmFunc func(duration *durationpb.Duration, coefficient float64, currentAttempt int32) time.Duration

func ExponentialBackoffAlgorithm(initInterval *durationpb.Duration, backoffCoefficient float64, currentAttempt int32) time.Duration {
	return time.Duration(int64(float64(initInterval.AsDuration().Nanoseconds()) * math.Pow(backoffCoefficient, float64(currentAttempt-1))))
}

// TODO treat 0 as 0, not infinite

func getBackoffInterval(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) (time.Duration, enumspb.RetryState) {

	if !isRetryable(failure, nonRetryableTypes) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}

	return nextBackoffInterval(now, currentAttempt, maxAttempts, initInterval, maxInterval, expirationTime, backoffCoefficient, ExponentialBackoffAlgorithm)
}

func nextBackoffInterval(
	now time.Time,
	currentAttempt int32,
	maxAttempts int32,
	initInterval *durationpb.Duration,
	maxInterval *durationpb.Duration,
	expirationTime *timestamppb.Timestamp,
	backoffCoefficient float64,
	intervalCalculator BackoffCalculatorAlgorithmFunc,
) (time.Duration, enumspb.RetryState) {
	// TODO remove below checks, most are already set with correct values
	if currentAttempt < 1 {
		currentAttempt = 1
	}

	if expirationTime != nil && expirationTime.AsTime().IsZero() {
		expirationTime = nil
	}
	// TODO remove above checks, most are already set with correct values
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

	interval := intervalCalculator(initInterval, backoffCoefficient, currentAttempt)
	if maxInterval.AsDuration() == 0 && interval <= 0 {
		return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
	}

	if maxInterval.AsDuration() != 0 && (interval <= 0 || interval > maxInterval.AsDuration()) {
		interval = maxInterval.AsDuration()
	}

	if expirationTime != nil && now.Add(interval).After(expirationTime.AsTime()) {
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
		timeoutType := failure.GetTimeoutFailureInfo().GetTimeoutType()
		if timeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
			timeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT {
			return !slices.Contains(
				nonRetryableTypes,
				retrypolicy.TimeoutFailureTypePrefix+timeoutType.String(),
			)
		}

		return false
	}

	if failure.GetServerFailureInfo() != nil {
		return !failure.GetServerFailureInfo().GetNonRetryable()
	}

	if failure.GetApplicationFailureInfo() != nil {
		if failure.GetApplicationFailureInfo().GetNonRetryable() {
			return false
		}

		return !slices.Contains(
			nonRetryableTypes,
			failure.GetApplicationFailureInfo().GetType(),
		)
	}
	return true
}

// Helpers for creating new retry/cron workflows:

func SetupNewWorkflowForRetryOrCron(
	ctx context.Context,
	previousMutableState MutableState,
	newMutableState MutableState,
	newRunID string,
	startAttr *historypb.WorkflowExecutionStartedEventAttributes,
	lastCompletionResult *commonpb.Payloads,
	failure *failurepb.Failure,
	backoffInterval time.Duration,
	initiator enumspb.ContinueAsNewInitiator,
) error {

	// Extract ParentExecutionInfo and RootExecutionInfo from current run so it can be passed down to the next
	var parentInfo *workflowspb.ParentExecutionInfo
	var rootInfo *workflowspb.RootExecutionInfo
	previousExecutionInfo := previousMutableState.GetExecutionInfo()
	if previousMutableState.HasParentExecution() {
		parentInfo = &workflowspb.ParentExecutionInfo{
			NamespaceId: previousExecutionInfo.ParentNamespaceId,
			Namespace:   startAttr.GetParentWorkflowNamespace(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: previousExecutionInfo.ParentWorkflowId,
				RunId:      previousExecutionInfo.ParentRunId,
			},
			InitiatedId:      previousExecutionInfo.ParentInitiatedId,
			InitiatedVersion: previousExecutionInfo.ParentInitiatedVersion,
			Clock:            previousExecutionInfo.ParentClock,
		}
		rootInfo = &workflowspb.RootExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: previousExecutionInfo.RootWorkflowId,
				RunId:      previousExecutionInfo.RootRunId,
			},
		}
	}

	newExecution := commonpb.WorkflowExecution{
		WorkflowId: previousExecutionInfo.WorkflowId,
		RunId:      newRunID,
	}

	firstRunID, err := previousMutableState.GetFirstRunID(ctx)
	if err != nil {
		return err
	}

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
	wType := &commonpb.WorkflowType{
		Name: workflowType,
	}

	var taskTimeout *durationpb.Duration
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
		Namespace:                newMutableState.GetNamespaceEntry().Name().String(),
		WorkflowId:               newExecution.WorkflowId,
		TaskQueue:                tq,
		WorkflowType:             wType,
		WorkflowExecutionTimeout: previousExecutionInfo.WorkflowExecutionTimeout,
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
		attempt = previousExecutionInfo.Attempt + 1
	}

	var sourceVersionStamp *commonpb.WorkerVersionStamp
	if previousExecutionInfo.AssignedBuildId == "" {
		// TODO: only keeping this part for old versioning. The desired logic seem to be the same for both cron and
		// retry: keep originally-inherited build ID. [cleanup-old-wv]
		// For retry: propagate build-id version info to new workflow.
		// For cron: do not propagate (always start on latest version).
		if initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY {
			sourceVersionStamp = worker_versioning.StampIfUsingVersioning(previousMutableState.GetMostRecentWorkerVersionStamp())
		}
	}

	req := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId:            newMutableState.GetNamespaceEntry().ID().String(),
		StartRequest:           createRequest,
		ParentExecutionInfo:    parentInfo,
		LastCompletionResult:   lastCompletionResult,
		ContinuedFailure:       failure,
		ContinueAsNewInitiator: initiator,
		// enforce minimal interval between runs to prevent tight loop continue as new spin.
		FirstWorkflowTaskBackoff: previousMutableState.ContinueAsNewMinBackoff(durationpb.New(backoffInterval)),
		Attempt:                  attempt,
		SourceVersionStamp:       sourceVersionStamp,
		RootExecutionInfo:        rootInfo,
		InheritedBuildId:         startAttr.InheritedBuildId,
	}
	workflowTimeoutTime := timestamp.TimeValue(previousExecutionInfo.WorkflowExecutionExpirationTime)
	if !workflowTimeoutTime.IsZero() {
		req.WorkflowExecutionExpirationTime = timestamppb.New(workflowTimeoutTime)
	}

	event, err := newMutableState.AddWorkflowExecutionStartedEventWithOptions(
		&newExecution,
		req,
		previousExecutionInfo.AutoResetPoints,
		previousMutableState.GetExecutionState().GetRunId(),
		firstRunID,
	)
	if err != nil {
		return serviceerror.NewInternal("Failed to add workflow execution started event.")
	}
	var parentClock *clock.VectorClock
	if parentInfo != nil {
		parentClock = parentInfo.Clock
	}
	if _, err = newMutableState.AddFirstWorkflowTaskScheduled(parentClock, event, false); err != nil {
		return err
	}

	return nil
}
