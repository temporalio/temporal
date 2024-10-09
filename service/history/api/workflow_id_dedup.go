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

package api

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

// ErrUseCurrentExecution is a sentinel error to indicate to the caller to
// use the current workflow execution instead of creating a new one
var ErrUseCurrentExecution = errors.New("ErrUseCurrentExecution")

// ResolveDuplicateWorkflowID determines how to resolve a workflow ID duplication upon workflow start according
// to the WorkflowIdReusePolicy (for *completed* workflow) or WorkflowIdConflictPolicy (for *running* workflow).
//
// NOTE: this function assumes the workflow id reuse policy Terminate-if-Running has been migrated
// to the workflow id conflict policy Terminate-Existing before it is invoked.
//
// An action (ie "mitigate and allow"), an error (ie "deny") or neither (ie "allow") is returned.
func ResolveDuplicateWorkflowID(
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	currentState enumsspb.WorkflowExecutionState,
	currentStatus enumspb.WorkflowExecutionStatus,
	currentStartRequestID string,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
	wfIDConflictPolicy enumspb.WorkflowIdConflictPolicy,
	currentWorkflowStartTime time.Time,
) (UpdateWorkflowActionFunc, error) {

	switch currentState {
	// *running* workflow: apply WorkflowIdConflictPolicy
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		switch wfIDConflictPolicy {
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL:
			msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
			return nil, generateWorkflowAlreadyStartedError(msg, currentStartRequestID, workflowKey)
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING:
			return nil, ErrUseCurrentExecution
		case enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING:
			return resolveDuplicateWorkflowStart(shardContext, currentWorkflowStartTime, workflowKey, namespaceEntry, newRunID)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to process start workflow id conflict policy: %v.", wfIDConflictPolicy))
		}

	// *completed* workflow: apply WorkflowIdReusePolicy
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		switch wfIDReusePolicy {
		case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE:
			// no action or error
		case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
			if _, ok := consts.FailedWorkflowStatuses[currentStatus]; !ok {
				msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: allow duplicate workflow Id if last run failed."
				return nil, generateWorkflowAlreadyStartedError(msg, currentStartRequestID, workflowKey)
			}
		case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
			msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: reject duplicate workflow Id."
			return nil, generateWorkflowAlreadyStartedError(msg, currentStartRequestID, workflowKey)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to process start workflow id reuse policy: %v.", wfIDReusePolicy))
		}

	default:
		// persistence.WorkflowStateZombie or unknown type
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to process workflow, workflow has invalid state: %v.", currentState))
	}

	// ie "allow"
	return nil, nil
}

// A minimal interval between workflow starts is used to prevent multiple starts with the same ID too rapidly.
// If the new workflow is started before the interval elapsed, the workflow start is aborted.
func resolveDuplicateWorkflowStart(
	shardContext shard.Context,
	currentWorkflowStartTime time.Time,
	workflowKey definition.WorkflowKey,
	namespaceEntry *namespace.Namespace,
	newRunID string,
) (UpdateWorkflowActionFunc, error) {

	if namespaceEntry == nil {
		return nil, &serviceerror.Internal{
			Message: fmt.Sprintf("Unknown namespace entry for %v.", workflowKey),
		}
	}

	nsName := namespaceEntry.Name().String()
	minimalReuseInterval := shardContext.GetConfig().WorkflowIdReuseMinimalInterval(nsName)

	now := shardContext.GetTimeSource().Now().UTC()
	timeSinceStart := now.Sub(currentWorkflowStartTime.UTC())

	if minimalReuseInterval == 0 || minimalReuseInterval < timeSinceStart {
		return terminateWorkflowAction(newRunID)
	}

	// Since there is a grace period, and the current workflow's start time is within that period,
	// abort the entire request.
	msg := fmt.Sprintf(
		"Too many restarts for workflow %s. Time since last start: %d ms",
		workflowKey.WorkflowID,
		timeSinceStart.Milliseconds(),
	)
	return nil, &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: msg,
	}
}

func terminateWorkflowAction(
	newRunID string,
) (UpdateWorkflowActionFunc, error) {
	return func(workflowLease WorkflowLease) (*UpdateWorkflowAction, error) {
		mutableState := workflowLease.GetMutableState()

		if !mutableState.IsWorkflowExecutionRunning() {
			return nil, consts.ErrWorkflowCompleted
		}

		return UpdateWorkflowTerminate, workflow.TerminateWorkflow(
			mutableState,
			"TerminateIfRunning WorkflowIdReusePolicy",
			payloads.EncodeString(fmt.Sprintf("terminated by new runID: %s", newRunID)),
			consts.IdentityHistoryService,
			false,
			nil, // No links necessary.
		)
	}, nil
}

func generateWorkflowAlreadyStartedError(
	errMsg string,
	createRequestID string,
	workflowKey definition.WorkflowKey,
) error {
	return serviceerror.NewWorkflowExecutionAlreadyStarted(
		fmt.Sprintf(errMsg, workflowKey.WorkflowID, workflowKey.RunID),
		createRequestID,
		workflowKey.RunID,
	)
}

func MigrateWorkflowIdReusePolicyForRunningWorkflow(
	wfIDReusePolicy *enumspb.WorkflowIdReusePolicy,
	wfIDConflictPolicy *enumspb.WorkflowIdConflictPolicy,
) {
	// workflow id reuse policy's Terminate-if-Running has been replaced by
	// workflow id conflict policy's Terminate-Existing
	if *wfIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
		*wfIDConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING

		// for *closed* workflows, its behavior is defined as ALLOW_DUPLICATE
		*wfIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}
