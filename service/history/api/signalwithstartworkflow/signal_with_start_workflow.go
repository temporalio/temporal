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

package signalwithstartworkflow

import (
	"context"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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
	currentWorkflowLease api.WorkflowLease,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (string, bool, error) {
	// workflow is running and restart was not requested
	if currentWorkflowLease != nil &&
		currentWorkflowLease.GetMutableState().IsWorkflowExecutionRunning() &&
		signalWithStartRequest.WorkflowIdConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING {

		// current workflow exists & running
		if err := signalWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			signalWithStartRequest,
		); err != nil {
			return "", false, err
		}
		return currentWorkflowLease.GetContext().GetWorkflowKey().RunID, false, nil
	}
	// else, either workflow is not running or restart requested
	return startAndSignalWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowLease,
		startRequest,
		signalWithStartRequest,
	)
}

func startAndSignalWorkflow(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	currentWorkflowLease api.WorkflowLease,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (string, bool, error) {
	workflowID := signalWithStartRequest.GetWorkflowId()
	runID := uuid.New().String()
	// TODO(bergundy): Support eager workflow task
	newMutableState, err := api.NewWorkflowWithSignal(
		shard,
		namespaceEntry,
		workflowID,
		runID,
		startRequest,
		signalWithStartRequest,
	)
	if err != nil {
		return "", false, err
	}

	newWorkflowLease, err := api.NewWorkflowLeaseAndContext(nil, shard, newMutableState)
	if err != nil {
		return "", false, err
	}

	if err = api.ValidateSignal(
		ctx,
		shard,
		newMutableState,
		signalWithStartRequest.GetSignalInput().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		return "", false, err
	}

	workflowMutationFn, err := createWorkflowMutationFunction(
		shard,
		currentWorkflowLease,
		namespaceEntry,
		runID,
		signalWithStartRequest.GetWorkflowIdReusePolicy(),
		signalWithStartRequest.GetWorkflowIdConflictPolicy(),
	)
	if err != nil {
		return "", false, err
	}
	if workflowMutationFn != nil {
		if err = startAndSignalWithCurrentWorkflow(
			ctx,
			shard,
			currentWorkflowLease,
			workflowMutationFn,
			newWorkflowLease,
		); err != nil {
			return "", false, err
		}
		return runID, true, nil
	}
	vrid, err := createVersionedRunID(currentWorkflowLease)
	if err != nil {
		return "", false, err
	}
	return startAndSignalWithoutCurrentWorkflow(
		ctx,
		shard,
		vrid,
		newWorkflowLease,
		signalWithStartRequest.RequestId,
	)
}

func createWorkflowMutationFunction(
	shardContext shard.Context,
	currentWorkflowLease api.WorkflowLease,
	namespaceEntry *namespace.Namespace,
	newRunID string,
	workflowIDReusePolicy enumspb.WorkflowIdReusePolicy,
	workflowIDConflictPolicy enumspb.WorkflowIdConflictPolicy,
) (api.UpdateWorkflowActionFunc, error) {
	if currentWorkflowLease == nil {
		return nil, nil
	}
	currentMutableState := currentWorkflowLease.GetMutableState()
	currentExecutionState := currentMutableState.GetExecutionState()
	currentWorkflowStartTime := currentMutableState.GetExecutionState().StartTime.AsTime()

	// It is unclear if currentExecutionState.RunId is the same as
	// currentWorkflowLease.GetContext().GetWorkflowKey().RunID
	workflowKey := definition.WorkflowKey{
		NamespaceID: currentWorkflowLease.GetContext().GetWorkflowKey().NamespaceID,
		WorkflowID:  currentWorkflowLease.GetContext().GetWorkflowKey().WorkflowID,
		RunID:       currentExecutionState.RunId,
	}

	workflowMutationFunc, err := api.ResolveDuplicateWorkflowID(
		shardContext,
		workflowKey,
		namespaceEntry,
		newRunID,
		currentExecutionState.State,
		currentExecutionState.Status,
		currentExecutionState.CreateRequestId,
		workflowIDReusePolicy,
		workflowIDConflictPolicy,
		currentWorkflowStartTime,
	)
	return workflowMutationFunc, err
}

func createVersionedRunID(currentWorkflowLease api.WorkflowLease) (*api.VersionedRunID, error) {
	if currentWorkflowLease == nil {
		return nil, nil
	}
	currentExecutionState := currentWorkflowLease.GetMutableState().GetExecutionState()
	currentCloseVersion, err := currentWorkflowLease.GetMutableState().GetCloseVersion()
	if err != nil {
		return nil, err
	}
	id := api.VersionedRunID{
		RunID: currentExecutionState.RunId,
		// we stop updating last write version in the current record after workflow is closed
		// so workflow close version is the last write version for the current record
		LastWriteVersion: currentCloseVersion,
	}
	return &id, nil
}

func startAndSignalWithCurrentWorkflow(
	ctx context.Context,
	shard shard.Context,
	currentWorkflowLease api.WorkflowLease,
	currentWorkflowUpdateAction api.UpdateWorkflowActionFunc,
	newWorkflowLease api.WorkflowLease,
) error {
	err := api.UpdateWorkflowWithNew(
		shard,
		ctx,
		currentWorkflowLease,
		currentWorkflowUpdateAction,
		func() (workflow.Context, workflow.MutableState, error) {
			return newWorkflowLease.GetContext(), newWorkflowLease.GetMutableState(), nil
		},
	)
	if err != nil {
		return err
	}
	return nil

}

func startAndSignalWithoutCurrentWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	vrid *api.VersionedRunID,
	newWorkflowLease api.WorkflowLease,
	requestID string,
) (string, bool, error) {
	newWorkflow, newWorkflowEventsSeq, err := newWorkflowLease.GetMutableState().CloseTransactionAsSnapshot(
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return "", false, err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return "", false, serviceerror.NewInternal("unable to create 1st event batch")
	}

	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	if vrid != nil {
		createMode = persistence.CreateWorkflowModeUpdateCurrent
		prevRunID = vrid.RunID
		prevLastWriteVersion = vrid.LastWriteVersion
		err = api.NewWorkflowVersionCheck(
			shardContext,
			vrid.LastWriteVersion,
			newWorkflowLease.GetMutableState(),
		)
		if err != nil {
			return "", false, err
		}
	}
	err = newWorkflowLease.GetContext().CreateWorkflowExecution(
		ctx,
		shardContext,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		newWorkflowLease.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	)
	switch failedErr := err.(type) {
	case nil:
		return newWorkflowLease.GetContext().GetWorkflowKey().RunID, true, nil
	case *persistence.CurrentWorkflowConditionFailedError:
		if failedErr.RequestID == requestID {
			return failedErr.RunID, false, nil
		}
		return "", false, err
	default:
		return "", false, err
	}
}

func signalWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	workflowLease api.WorkflowLease,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) error {
	mutableState := workflowLease.GetMutableState()
	if err := api.ValidateSignal(
		ctx,
		shardContext,
		workflowLease.GetMutableState(),
		request.GetSignalInput().Size(),
		"SignalWithStartWorkflowExecution",
	); err != nil {
		// in-memory mutable state is still clean, release the lock with nil error to prevent
		// clearing and reloading mutable state
		workflowLease.GetReleaseFn()(nil)
		return err
	}

	if request.GetRequestId() != "" && mutableState.IsSignalRequested(request.GetRequestId()) {
		// duplicate signal
		// in-memory mutable state is still clean, release the lock with nil error to prevent
		// clearing and reloading mutable state
		workflowLease.GetReleaseFn()(nil)
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
		request.GetLinks(),
	); err != nil {
		return err
	}

	// Create a transfer task to schedule a workflow task
	if !mutableState.HasPendingWorkflowTask() {

		executionInfo := mutableState.GetExecutionInfo()
		executionState := mutableState.GetExecutionState()
		if !mutableState.HadOrHasWorkflowTask() && !executionInfo.ExecutionTime.AsTime().Equal(executionState.StartTime.AsTime()) {
			metrics.SignalWithStartSkipDelayCounter.With(shardContext.GetMetricsHandler()).Record(1, metrics.NamespaceTag(request.GetNamespace()))

			workflowKey := workflowLease.GetContext().GetWorkflowKey()
			shardContext.GetThrottledLogger().Info(
				"Skipped workflow start delay for signalWithStart request",
				tag.WorkflowNamespace(request.GetNamespace()),
				tag.WorkflowID(workflowKey.WorkflowID),
				tag.WorkflowRunID(workflowKey.RunID),
			)
		}

		_, err := mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
		if err != nil {
			return err
		}
	}

	// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
	// the history and try the operation again.
	return workflowLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	)
}
