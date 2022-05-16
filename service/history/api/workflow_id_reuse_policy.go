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
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
)

// ApplyWorkflowIDReusePolicy returns updateWorkflowActionFunc
// for updating the previous execution and an error if the situation is
// not allowed by the workflowIDReusePolicy.
// Both result may be nil, if the case is to allow and no update is needed
// for the previous execution.
func ApplyWorkflowIDReusePolicy(
	prevStartRequestID,
	prevRunID string,
	prevState enumsspb.WorkflowExecutionState,
	prevStatus enumspb.WorkflowExecutionStatus,
	workflowID string,
	runID string,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
) (UpdateWorkflowActionFunc, error) {

	// here we know there is some information about the prev workflow, i.e. either running right now
	// or has history check if the this workflow is finished
	switch prevState {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		if wfIDReusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
			return func(workflowContext WorkflowContext) (*UpdateWorkflowAction, error) {
				mutableState := workflowContext.GetMutableState()
				if !mutableState.IsWorkflowExecutionRunning() {
					return nil, consts.ErrWorkflowCompleted
				}

				return UpdateWorkflowWithoutWorkflowTask, workflow.TerminateWorkflow(
					mutableState,
					mutableState.GetNextEventID(),
					"TerminateIfRunning WorkflowIdReusePolicy Policy",
					payloads.EncodeString(
						fmt.Sprintf("terminated by new runID: %s", runID),
					),
					consts.IdentityHistoryService,
					false,
				)
			}, nil
		}

		msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
		return nil, generateWorkflowAlreadyStartedError(msg, prevStartRequestID, workflowID, prevRunID)
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		// previous workflow completed, proceed
	default:
		// persistence.WorkflowStateZombie or unknown type
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to process workflow, workflow has invalid state: %v.", prevState))
	}

	switch wfIDReusePolicy {
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		if _, ok := consts.FailedWorkflowStatuses[prevStatus]; !ok {
			msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: allow duplicate workflow Id if last run failed."
			return nil, generateWorkflowAlreadyStartedError(msg, prevStartRequestID, workflowID, prevRunID)
		}
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING:
		// as long as workflow not running, so this case has no check
	case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
		msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: reject duplicate workflow Id."
		return nil, generateWorkflowAlreadyStartedError(msg, prevStartRequestID, workflowID, prevRunID)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Failed to process start workflow reuse policy: %v.", wfIDReusePolicy))
	}

	return nil, nil
}

func generateWorkflowAlreadyStartedError(
	errMsg string,
	createRequestID string,
	workflowID string,
	runID string,
) error {
	return serviceerror.NewWorkflowExecutionAlreadyStarted(
		fmt.Sprintf(errMsg, workflowID, runID),
		createRequestID,
		runID,
	)
}
