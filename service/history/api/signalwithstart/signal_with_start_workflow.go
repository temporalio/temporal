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

package signalwithstart

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func SignalWithStartWorkflow(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	currentWorkflowContext api.WorkflowContext,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (string, error) {

	if currentWorkflowContext != nil &&
		currentWorkflowContext.GetMutableState().IsWorkflowExecutionRunning() &&
		signalWithStartRequest.WorkflowIdReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {

		// current workflow exists & running
		if err := signalWorkflow(
			ctx,
			shard,
			currentWorkflowContext,
			signalWithStartRequest,
		); err != nil {
			return "", err
		}
		return currentWorkflowContext.GetRunID(), nil
	}

	return startAndSignalWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowContext,
		startRequest,
		signalWithStartRequest,
	)
}

func startAndSignalWorkflow(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	currentWorkflowContext api.WorkflowContext,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (string, error) {
	workflowID := signalWithStartRequest.GetWorkflowId()
	runID := uuid.New().String()
	newWorkflowContext, err := api.NewWorkflowWithSignal(
		shard,
		namespaceEntry,
		workflowID,
		runID,
		startRequest,
		signalWithStartRequest,
	)
	if err != nil {
		return "", err
	}
	if err := api.ValidateSignal(
		ctx,
		shard,
		newWorkflowContext.GetMutableState(),
		signalWithStartRequest.GetSignalInput().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		return "", err
	}

	casPredicate, currentWorkflowMutationFn, err := startAndSignalWorkflowActionFn(
		currentWorkflowContext,
		signalWithStartRequest.WorkflowIdReusePolicy,
		runID,
	)
	if err != nil {
		return "", err
	}

	if currentWorkflowMutationFn != nil {
		if err := startAndSignalWithCurrentWorkflow(
			ctx,
			shard,
			currentWorkflowContext,
			currentWorkflowMutationFn,
			newWorkflowContext,
		); err != nil {
			return "", err
		}
		return runID, nil
	}

	return startAndSignalWithoutCurrentWorkflow(
		ctx,
		shard,
		casPredicate,
		newWorkflowContext,
		signalWithStartRequest.RequestId,
	)
}

func startAndSignalWorkflowActionFn(
	currentWorkflowContext api.WorkflowContext,
	workflowIDReusePolicy enumspb.WorkflowIdReusePolicy,
	newRunID string,
) (*api.CreateWorkflowCASPredicate, api.UpdateWorkflowActionFunc, error) {
	if currentWorkflowContext == nil {
		return nil, nil, nil
	}

	currentExecutionState := currentWorkflowContext.GetMutableState().GetExecutionState()
	currentExecutionUpdateAction, err := api.ApplyWorkflowIDReusePolicy(
		currentExecutionState.CreateRequestId,
		currentExecutionState.RunId,
		currentExecutionState.State,
		currentExecutionState.Status,
		currentWorkflowContext.GetWorkflowID(),
		newRunID,
		workflowIDReusePolicy,
	)
	if err != nil {
		return nil, nil, err
	}
	if currentExecutionUpdateAction != nil {
		return nil, currentExecutionUpdateAction, nil
	}

	currentLastWriteVersion, err := currentWorkflowContext.GetMutableState().GetLastWriteVersion()
	if err != nil {
		return nil, nil, err
	}
	casPredicate := &api.CreateWorkflowCASPredicate{
		RunID:            currentExecutionState.RunId,
		LastWriteVersion: currentLastWriteVersion,
	}
	return casPredicate, nil, nil
}

func startAndSignalWithCurrentWorkflow(
	ctx context.Context,
	shard shard.Context,
	currentWorkflowContext api.WorkflowContext,
	currentWorkflowUpdateAction api.UpdateWorkflowActionFunc,
	newWorkflowContext api.WorkflowContext,
) error {
	err := api.UpdateWorkflowWithNew(
		shard,
		ctx,
		currentWorkflowContext,
		currentWorkflowUpdateAction,
		func() (workflow.Context, workflow.MutableState, error) {
			return newWorkflowContext.GetContext(), newWorkflowContext.GetMutableState(), nil
		},
	)
	if err != nil {
		return err
	}
	return nil

}

func startAndSignalWithoutCurrentWorkflow(
	ctx context.Context,
	shard shard.Context,
	casPredicate *api.CreateWorkflowCASPredicate,
	newWorkflowContext api.WorkflowContext,
	requestID string,
) (string, error) {
	now := shard.GetTimeSource().Now()
	newWorkflow, newWorkflowEventsSeq, err := newWorkflowContext.GetMutableState().CloseTransactionAsSnapshot(
		now,
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return "", err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return "", serviceerror.NewInternal("unable to create 1st event batch")
	}

	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	if casPredicate != nil {
		createMode = persistence.CreateWorkflowModeUpdateCurrent
		prevRunID = casPredicate.RunID
		prevLastWriteVersion = casPredicate.LastWriteVersion
		if err := api.NewWorkflowVersionCheck(shard, casPredicate.LastWriteVersion, newWorkflowContext.GetMutableState()); err != nil {
			return "", err
		}
	}
	err = newWorkflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		newWorkflowContext.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	)
	switch failedErr := err.(type) {
	case nil:
		return newWorkflowContext.GetRunID(), nil
	case *persistence.CurrentWorkflowConditionFailedError:
		if failedErr.RequestID == requestID {
			return failedErr.RunID, nil
		}
		return "", err
	default:
		return "", err
	}
}

func signalWorkflow(
	ctx context.Context,
	shard shard.Context,
	workflowContext api.WorkflowContext,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) error {
	mutableState := workflowContext.GetMutableState()
	if err := api.ValidateSignal(
		ctx,
		shard,
		workflowContext.GetMutableState(),
		request.GetSignalInput().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		return err
	}

	if request.GetRequestId() != "" && mutableState.IsSignalRequested(request.GetRequestId()) {
		// duplicate signal
		return nil
	}
	if request.GetRequestId() != "" {
		mutableState.AddSignalRequested(request.GetRequestId())
	}
	if _, err := mutableState.AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
	); err != nil {
		return err
	}

	// Create a transfer task to schedule a workflow task
	if !mutableState.HasPendingWorkflowTask() {
		_, err := mutableState.AddWorkflowTaskScheduledEvent(false)
		if err != nil {
			return err
		}
	}

	// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
	// the history and try the operation again.
	if err := workflowContext.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shard.GetTimeSource().Now(),
	); err != nil {
		return err
	}
	return nil
}
