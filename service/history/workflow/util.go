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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/consts"
)

func failWorkflowTask(
	mutableState MutableState,
	workflowTask *WorkflowTaskInfo,
	workflowTaskFailureCause enumspb.WorkflowTaskFailedCause,
) error {

	if _, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		workflowTaskFailureCause,
		nil,
		consts.IdentityHistoryService,
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	mutableState.FlushBufferedEvents()
	return nil
}

func ScheduleWorkflowTask(
	mutableState MutableState,
) error {

	if mutableState.HasPendingWorkflowTask() {
		return nil
	}

	_, err := mutableState.AddWorkflowTaskScheduledEvent(false)
	if err != nil {
		return serviceerror.NewInternal("Failed to add workflow task scheduled event.")
	}
	return nil
}

func RetryWorkflow(
	mutableState MutableState,
	eventBatchFirstEventID int64,
	parentNamespace namespace.Name,
	continueAsNewAttributes *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) (MutableState, error) {

	if workflowTask, ok := mutableState.GetInFlightWorkflowTask(); ok {
		if err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		); err != nil {
			return nil, err
		}
	}

	_, newMutableState, err := mutableState.AddContinueAsNewEvent(
		eventBatchFirstEventID,
		common.EmptyEventID,
		parentNamespace,
		continueAsNewAttributes,
	)
	if err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func TimeoutWorkflow(
	mutableState MutableState,
	eventBatchFirstEventID int64,
	retryState enumspb.RetryState,
	continuedRunID string,
) error {

	if workflowTask, ok := mutableState.GetInFlightWorkflowTask(); ok {
		if err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
		retryState,
		continuedRunID,
	)
	return err
}

func TerminateWorkflow(
	mutableState MutableState,
	eventBatchFirstEventID int64,
	terminateReason string,
	terminateDetails *commonpb.Payloads,
	terminateIdentity string,
	deleteAfterTerminate bool,
) error {

	if workflowTask, ok := mutableState.GetInFlightWorkflowTask(); ok {
		if err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
		deleteAfterTerminate,
	)
	return err
}

// FindAutoResetPoint returns the auto reset point
func FindAutoResetPoint(
	timeSource clock.TimeSource,
	verifyChecksum func(string) error,
	autoResetPoints *workflowpb.ResetPoints,
) (string, *workflowpb.ResetPointInfo) {
	if autoResetPoints == nil {
		return "", nil
	}
	now := timeSource.Now()
	for _, p := range autoResetPoints.Points {
		if err := verifyChecksum(p.GetBinaryChecksum()); err != nil && p.GetResettable() {
			expireTime := timestamp.TimeValue(p.GetExpireTime())
			if !expireTime.IsZero() && now.After(expireTime) {
				// reset point has expired and we may already deleted the history
				continue
			}
			return err.Error(), p
		}
	}
	return "", nil
}
