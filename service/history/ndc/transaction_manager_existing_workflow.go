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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transaction_manager_existing_workflow_mock.go

package ndc

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	transactionMgrForExistingWorkflow interface {
		dispatchForExistingWorkflow(
			ctx context.Context,
			isWorkflowRebuilt bool,
			targetWorkflow Workflow,
			newWorkflow Workflow,
		) error
	}

	nDCTransactionMgrForExistingWorkflowImpl struct {
		shardContext                shard.Context
		transactionMgr              TransactionManager
		bypassVersionSemanticsCheck bool
	}
)

var _ transactionMgrForExistingWorkflow = (*nDCTransactionMgrForExistingWorkflowImpl)(nil)

func newNDCTransactionMgrForExistingWorkflow(
	shardContext shard.Context,
	transactionMgr TransactionManager,
	bypassVersionSemanticsCheck bool,
) *nDCTransactionMgrForExistingWorkflowImpl {

	return &nDCTransactionMgrForExistingWorkflowImpl{
		shardContext:                shardContext,
		transactionMgr:              transactionMgr,
		bypassVersionSemanticsCheck: bypassVersionSemanticsCheck,
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchForExistingWorkflow(
	ctx context.Context,
	isWorkflowRebuilt bool,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	// NOTE: this function does NOT mutate current workflow, target workflow or new workflow,
	//  workflow mutation is done in methods within executeTransaction function

	// this is a performance optimization so most update does not need to
	// check whether target workflow is current workflow by calling DB API
	if !isWorkflowRebuilt && targetWorkflow.GetMutableState().IsCurrentWorkflowGuaranteed() {
		// NOTE: if target workflow is rebuilt, then IsCurrentWorkflowGuaranteed is not trustworthy

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	targetExecutionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
	targetExecutionState := targetWorkflow.GetMutableState().GetExecutionState()
	namespaceID := namespace.ID(targetExecutionInfo.NamespaceId)
	workflowID := targetExecutionInfo.WorkflowId
	targetRunID := targetExecutionState.RunId

	// the target workflow is rebuilt
	// we need to check the current workflow execution
	currentRunID, err := r.transactionMgr.GetCurrentWorkflowRunID(
		ctx,
		namespaceID,
		workflowID,
	)
	if err != nil {
		return err
	}
	if currentRunID == "" {
		// this means a bug in our code or DB is inconsistent...
		return serviceerror.NewInternal("transactionMgr: unable to locate current workflow during update")
	}

	if currentRunID == targetRunID {
		if !isWorkflowRebuilt {
			return serviceerror.NewInternal("transactionMgr: encountered workflow not rebuilt & current workflow not guaranteed")
		}

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := r.transactionMgr.LoadWorkflow(
		ctx,
		namespaceID,
		workflowID,
		currentRunID,
	)
	if err != nil {
		return err
	}

	targetWorkflowIsNewer, err := targetWorkflow.HappensAfter(currentWorkflow)
	if err != nil {
		return err
	}

	if !targetWorkflowIsNewer {
		// target workflow is older than current workflow, need to suppress the target workflow
		return r.dispatchWorkflowUpdateAsZombie(
			ctx,
			isWorkflowRebuilt,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)
	}

	// isWorkflowRebuilt is irrelevant here, because the DB API to be used
	// will set target workflow using snapshot
	return r.executeTransaction(
		ctx,
		nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsCurrent(
	ctx context.Context,
	isWorkflowRebuilt bool,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyUpdateAsCurrent,
			nil,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		nDCTransactionPolicyConflictResolveAsCurrent,
		nil,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsZombie(
	ctx context.Context,
	isWorkflowRebuilt bool,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyUpdateAsZombie,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		nDCTransactionPolicyConflictResolveAsZombie,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsCurrent(
	ctx context.Context,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	if newWorkflow == nil {
		return targetWorkflow.GetContext().UpdateWorkflowExecutionAsPassive(ctx, r.shardContext)
	}

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNewAsPassive(
		ctx,
		r.shardContext,
		newWorkflow.GetContext(),
		newWorkflow.GetMutableState(),
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsZombie(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	targetPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if !r.bypassVersionSemanticsCheck && targetPolicy != workflow.TransactionPolicyPassive {
		return serviceerror.NewInternal("transactionMgrForExistingWorkflow updateAsZombie encountered target workflow policy not being passive")
	}

	var newContext workflow.Context
	var newMutableState workflow.MutableState
	var newTransactionPolicy *workflow.TransactionPolicy
	if newWorkflow != nil {
		newWorkflowPolicy, err := newWorkflow.SuppressBy(
			currentWorkflow,
		)
		if err != nil {
			return err
		}
		if !r.bypassVersionSemanticsCheck && newWorkflowPolicy != workflow.TransactionPolicyPassive {
			return serviceerror.NewInternal("transactionMgrForExistingWorkflow updateAsZombie encountered new workflow policy not being passive")
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newExecutionState := newWorkflow.GetMutableState().GetExecutionState()
		newWorkflowExists, err := r.transactionMgr.CheckWorkflowExists(
			ctx,
			namespace.ID(newExecutionInfo.NamespaceId),
			newExecutionInfo.WorkflowId,
			newExecutionState.RunId,
		)
		if err != nil {
			return err
		}
		if newWorkflowExists {
			// new workflow already exists, do not create again
			newContext = nil
			newMutableState = nil
			newTransactionPolicy = nil
		} else {
			// new workflow does not exist, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
			newTransactionPolicy = workflow.TransactionPolicyPassive.Ptr()
		}
	}

	// release lock on current workflow, since current cluster maybe the active cluster
	//  and events maybe reapplied to current workflow
	currentWorkflow.GetReleaseFn()(nil)
	currentWorkflow = nil

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		workflow.TransactionPolicyPassive,
		newTransactionPolicy,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) suppressCurrentAndUpdateAsCurrent(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	var err error
	resetWorkflowPolicy := workflow.TransactionPolicyPassive
	currentWorkflowPolicy := workflow.TransactionPolicyPassive
	if currentWorkflow.GetMutableState().IsWorkflowExecutionRunning() {
		currentWorkflowPolicy, err = currentWorkflow.SuppressBy(
			targetWorkflow,
		)
		if err != nil {
			return err
		}
	}
	if err := targetWorkflow.Revive(); err != nil {
		return err
	}

	var newWorkflowPolicy *workflow.TransactionPolicy
	var newContext workflow.Context
	var newMutableState workflow.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		if err := newWorkflow.Revive(); err != nil {
			return err
		}
		newWorkflowPolicy = workflow.TransactionPolicyPassive.Ptr()
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		currentWorkflow.GetContext(),
		currentWorkflow.GetMutableState(),
		resetWorkflowPolicy,
		newWorkflowPolicy,
		currentWorkflowPolicy.Ptr(),
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsCurrent(
	ctx context.Context,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	targetWorkflowPolicy := workflow.TransactionPolicyPassive

	var newWorkflowPolicy *workflow.TransactionPolicy
	var newContext workflow.Context
	var newMutableState workflow.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		newWorkflowPolicy = workflow.TransactionPolicyPassive.Ptr()
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		targetWorkflowPolicy,
		newWorkflowPolicy,
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsZombie(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	var err error

	targetWorkflowPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if !r.bypassVersionSemanticsCheck && targetWorkflowPolicy != workflow.TransactionPolicyPassive {
		return serviceerror.NewInternal("transactionMgrForExistingWorkflow conflictResolveAsZombie encountered target workflow policy not being passive")
	}

	var newWorkflowPolicy workflow.TransactionPolicy
	var newContext workflow.Context
	var newMutableState workflow.MutableState
	if newWorkflow != nil {
		newWorkflowPolicy, err = newWorkflow.SuppressBy(
			currentWorkflow,
		)
		if err != nil {
			return err
		}
		if !r.bypassVersionSemanticsCheck && newWorkflowPolicy != workflow.TransactionPolicyPassive {
			return serviceerror.NewInternal("transactionMgrForExistingWorkflow conflictResolveAsZombie encountered new workflow policy not being passive")
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newExecutionState := newWorkflow.GetMutableState().GetExecutionState()
		newWorkflowExists, err := r.transactionMgr.CheckWorkflowExists(
			ctx,
			namespace.ID(newExecutionInfo.NamespaceId),
			newExecutionInfo.WorkflowId,
			newExecutionState.RunId,
		)
		if err != nil {
			return err
		}
		if newWorkflowExists {
			// new workflow already exists, do not create again
			newContext = nil
			newMutableState = nil
		} else {
			// new workflow does not exist, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
		}
	}

	// release lock on current workflow, since current cluster maybe the active cluster
	//  and events maybe reapplied to current workflow
	currentWorkflow.GetReleaseFn()(nil)
	currentWorkflow = nil

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		targetWorkflowPolicy,
		newWorkflowPolicy.Ptr(),
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) executeTransaction(
	ctx context.Context,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) (retError error) {

	defer func() {
		if rec := recover(); rec != nil {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, newWorkflow, errPanic)
			panic(rec)
		} else {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, newWorkflow, retError)
		}
	}()

	switch transactionPolicy {
	case nDCTransactionPolicyUpdateAsCurrent:
		return r.updateAsCurrent(
			ctx,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyUpdateAsZombie:
		return r.updateAsZombie(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent:
		return r.suppressCurrentAndUpdateAsCurrent(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsCurrent:
		return r.conflictResolveAsCurrent(
			ctx,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsZombie:
		return r.conflictResolveAsZombie(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	default:
		return serviceerror.NewInternal(fmt.Sprintf("transactionMgr: encountered unknown transaction type: %v", transactionPolicy))
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) cleanupTransaction(
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.GetReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.GetReleaseFn()(err)
	}
	if newWorkflow != nil {
		newWorkflow.GetReleaseFn()(err)
	}
}
