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
	commonpb "go.temporal.io/api/common/v1"
	deploymentspb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/consts"
)

func failWorkflowTask(
	mutableState MutableState,
	workflowTask *WorkflowTaskInfo,
	workflowTaskFailureCause enumspb.WorkflowTaskFailedCause,
) (*historypb.HistoryEvent, error) {

	// IMPORTANT: wtFailedEvent can be nil under some circumstances. Specifically, if WT is transient.
	wtFailedEvent, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		workflowTaskFailureCause,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		0,
	)
	if err != nil {
		return nil, err
	}

	mutableState.FlushBufferedEvents()
	return wtFailedEvent, nil
}

func ScheduleWorkflowTask(
	mutableState MutableState,
) error {

	if mutableState.HasPendingWorkflowTask() {
		return nil
	}

	_, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		return serviceerror.NewInternal("Failed to add workflow task scheduled event.")
	}
	return nil
}

func TimeoutWorkflow(
	mutableState MutableState,
	retryState enumspb.RetryState,
	continuedRunID string,
) error {

	// Check TerminateWorkflow comment bellow.
	eventBatchFirstEventID := mutableState.GetNextEventID()
	if workflowTask := mutableState.GetStartedWorkflowTask(); workflowTask != nil {
		wtFailedEvent, err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		)
		if err != nil {
			return err
		}
		if wtFailedEvent != nil {
			eventBatchFirstEventID = wtFailedEvent.GetEventId()
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
		retryState,
		continuedRunID,
	)
	return err
}

// TerminateWorkflow will write a WorkflowExecutionTerminated event with a fresh
// batch ID. Do not use for situations where the WorkflowExecutionTerminated
// event must fall within an existing event batch (for example, if you've already
// failed a workflow task via `failWorkflowTask` and have an event batch ID).
func TerminateWorkflow(
	mutableState MutableState,
	terminateReason string,
	terminateDetails *commonpb.Payloads,
	terminateIdentity string,
	deleteAfterTerminate bool,
	links []*commonpb.Link,
) error {

	// Terminate workflow is written as a separate batch and might result in more than one event
	// if there is started WT which needs to be failed before.
	// Failing speculative WT creates 3 events: WTScheduled, WTStarted, and WTFailed.
	// First 2 goes to separate batch and eventBatchFirstEventID has to point to WTFailed event.
	// Failing transient WT doesn't create any events at all and wtFailedEvent is nil.
	// WTFailed event wasn't created (because there were no WT or WT was transient),
	// then eventBatchFirstEventID points to TerminateWorkflow event (which is next event).
	eventBatchFirstEventID := mutableState.GetNextEventID()

	if workflowTask := mutableState.GetStartedWorkflowTask(); workflowTask != nil {
		wtFailedEvent, err := failWorkflowTask(
			mutableState,
			workflowTask,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND,
		)
		if err != nil {
			return err
		}
		if wtFailedEvent != nil {
			eventBatchFirstEventID = wtFailedEvent.GetEventId()
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
		deleteAfterTerminate,
		links,
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

func WithEffects(effects effect.Controller, ms MutableState) MutableStateWithEffects {
	return MutableStateWithEffects{
		MutableState: ms,
		Controller:   effects,
	}
}

type MutableStateWithEffects struct {
	MutableState
	effect.Controller
}

func (mse MutableStateWithEffects) CanAddEvent() bool {
	// Event can be added to the history if workflow is still running.
	return mse.MutableState.IsWorkflowExecutionRunning()
}

// GetEffectiveDeployment returns the effective deployment in the following order:
//  1. DeploymentTransition.Deployment: this is returned when the wf is transitioning to a
//     new deployment
//  2. VersioningOverride.Deployment: this is returned when user has set a PINNED override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  3. Deployment: this is returned when there is no transition and no override (the most
//     common case). Deployment is set based on the worker-sent deployment in the latest WFT
//     completion. Exception: if Deployment is set but the workflow's effective behavior is
//     UNSPECIFIED, it means the workflow is unversioned, so effective deployment will be nil.
//
// Note: Deployment objects are immutable, never change their fields.
func GetEffectiveDeployment(versioningInfo *workflowpb.WorkflowExecutionVersioningInfo) *deploymentspb.Deployment {
	if versioningInfo == nil {
		return nil
	} else if transition := versioningInfo.GetDeploymentTransition(); transition != nil {
		return transition.GetDeployment()
	} else if override := versioningInfo.GetVersioningOverride(); override != nil &&
		override.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED {
		return override.GetDeployment()
	} else if GetEffectiveVersioningBehavior(versioningInfo) != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		return versioningInfo.GetDeployment()
	}
	return nil
}

// GetEffectiveVersioningBehavior returns the effective versioning behavior in the following
// order:
//  1. VersioningOverride.Behavior: this is returned when user has set a behavior override
//     at wf start time, or later via UpdateWorkflowExecutionOptions.
//  2. Behavior: this is returned when there is no override (most common case). Behavior is
//     set based on the worker-sent deployment in the latest WFT completion.
func GetEffectiveVersioningBehavior(versioningInfo *workflowpb.WorkflowExecutionVersioningInfo) enumspb.VersioningBehavior {
	if versioningInfo == nil {
		return enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	} else if override := versioningInfo.GetVersioningOverride(); override != nil {
		return override.GetBehavior()
	}
	return versioningInfo.GetBehavior()
}
